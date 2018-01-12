local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local consts = require('vshard.consts')
local lerror = require('vshard.error')
local lcfg = require('vshard.cfg')
local lreplicaset = require('vshard.replicaset')

-- Internal state
local self = {
    errinj = {
        ERRINJ_FAILOVER_CHANGE_CFG = false,
    }
}

--------------------------------------------------------------------------------
-- Routing
--------------------------------------------------------------------------------

--
-- All known replicasets used for bucket re-balancing
--
self.replicasets = nil

-- Bucket map cache
-- NOTE: it is should be good to store bucket map in memtx space
self.route_map = {}

-- Search bucket in whole cluster
local function bucket_discovery(bucket_id)
    local replicaset = self.route_map[bucket_id]
    if replicaset ~= nil then
        return replicaset
    end

    log.info("Discovering bucket %d", bucket_id)
    local unreachable_uuid = nil
    local is_transfer_in_progress = false
    for _, replicaset in pairs(self.replicasets) do
        local stat, err = replicaset:callrw('vshard.storage.bucket_stat',
                                             {bucket_id})
        if stat then
            if stat.status == consts.BUCKET.ACTIVE or
               stat.status == consts.BUCKET.SENDING then
                log.info("Discovered bucket %d on %s", bucket_id,
                         replicaset.uuid)
                self.route_map[bucket_id] = replicaset
                return replicaset
            elseif stat.status == consts.BUCKET.RECEIVING then
                is_transfer_in_progress = true
            end
        elseif err.code ~= lerror.code.WRONG_BUCKET then
            unreachable_uuid = replicaset.uuid
        end
    end
    local errcode = nil
    if unreachable_uuid then
        errcode = lerror.code.UNREACHABLE_REPLICASET
    elseif is_transfer_in_progress then
        errcode = lerror.code.TRANSFER_IS_IN_PROGRESS
    else
        -- All replicasets were scanned, but a bucket was not
        -- found anywhere, so most likely it does not exist. It
        -- can be wrong, if rebalancing is in progress, and a
        -- bucket was found to be RECEIVING on one replicaset, and
        -- was not found on other replicasets (it was sent during
        -- discovery).
        errcode = lerror.code.NO_ROUTE_TO_BUCKET
    end

    return nil, lerror.vshard(errcode, {bucket_id = bucket_id,
                                        unreachable_uuid = unreachable_uuid})
end

-- Resolve bucket id to replicaset uuid
local function bucket_resolve(bucket_id)
    local replicaset, err
    local replicaset = self.route_map[bucket_id]
    if replicaset ~= nil then
        return replicaset
    end
    -- Replicaset removed from cluster, perform discovery
    replicaset, err = bucket_discovery(bucket_id)
    if replicaset == nil then
        return nil, err
    end
    return replicaset
end

--------------------------------------------------------------------------------
-- API
--------------------------------------------------------------------------------

-- Perform shard operation
-- Function will restart operation after wrong bucket response until timeout
-- is reached
--
local function router_call(bucket_id, mode, func, args)
    local replicaset, err
    local tstart = lfiber.time()
    if bucket_id > consts.BUCKET_COUNT or bucket_id < 0 then
        error('Bucket is unreachable: bucket id is out of range')
    end
    local call
    if mode == 'read' then
        call = 'callro'
    else
        call = 'callrw'
    end
    repeat
        replicaset, err = bucket_resolve(bucket_id)
        if replicaset then
            local storage_call_status, call_status, call_error =
                replicaset[call](replicaset, 'vshard.storage.call',
                                 {bucket_id, mode, func, args})
            if storage_call_status then
                if call_status == nil and call_error ~= nil then
                    return call_status, call_error
                else
                    return call_status
                end
            end
            err = call_status
            if err.code == lerror.code.WRONG_BUCKET or
               err.code == lerror.code.TRANSFER_IS_IN_PROGRESS then
                self.route_map[bucket_id] = nil
            elseif err.code == lerror.code.NON_MASTER then
                log.warn("Replica %s is not master for replicaset %s anymore,"..
                         "please update configuration!",
                          replicaset.master.uuid, replicaset.uuid)
            end
            return nil, err
        end
    until not (lfiber.time() <= tstart + consts.CALL_TIMEOUT)
    if err then
        return nil, err
    else
        local _, boxerror = pcall(box.error, box.error.TIMEOUT)
        return nil, lerror.box(boxerror)
    end
end

--
-- Get replicaset object by bucket identifier.
-- @param bucket_id Bucket identifier.
-- @retval Netbox connection.
--
local function router_route(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: router.route(bucket_id)')
    end
    return bucket_resolve(bucket_id)
end

--
-- Return map of all replicasets.
-- @retval See self.replicasets map.
--
local function router_route_all()
    return self.replicasets
end

--------------------------------------------------------------------------------
-- Failover
--------------------------------------------------------------------------------
--
-- Fiber to maintain replica connections.
--
self.failover_fiber = nil

--
-- Replicaset must fall its replica connection to lower priority,
-- if the current one is down too long.
--
local function failover_need_down_priority(replicaset, curr_ts)
    local r = replicaset.replica
    return r and r.down_ts and
           curr_ts - r.down_ts >= consts.FAILOVER_DOWN_TIMEOUT
           and r.next_by_priority
end

--
-- Replicaset must try to connect to a server with the highest
-- priority once per specified timeout. It allows to return to
-- the best server, if it was unavailable and has returned back.
-- And if the connection attempt was not successfull, then the
-- candidate must try a replica next by priority.
--
local function failover_need_update_candidate(replicaset, curr_ts)
    local up_ts = replicaset.replica_up_ts
    -- First attempt to connect to replica.
    if not up_ts then
        return true
    end
    -- Try to reconnect to the best replica once per UP_TIMEOUT.
    if curr_ts - up_ts >= consts.FAILOVER_UP_TIMEOUT then
        return true
    end
    -- Candidate can not connect to a replica. Try next by
    -- priority, if it is not current replica. Candidate always
    -- must have weight <= current replica weight.
    local candidate = replicaset.candidate
    return candidate and
           curr_ts - candidate.down_ts >= consts.FAILOVER_DOWN_TIMEOUT and
           candidate.next_by_priority and
           candidate.next_by_priority ~= replicaset.replica
end

--
-- Check that a candidate is connected to its replica. In such a
-- case it becames new replica, because its weight <= current one.
--
local function failover_is_candidate_connected(replicaset)
    local candidate = replicaset.candidate
    return candidate and candidate:is_connected()
end

--
-- Collect UUIDs of replicasets, priority of whose replica
-- connections must be updated.
--
local function failover_collect_to_update()
    local ts = lfiber.time()
    local uuid_to_update = {}
    for uuid, rs in pairs(self.replicasets) do
        if failover_need_down_priority(rs, ts) or
           failover_is_candidate_connected(rs) or
           failover_need_update_candidate(rs, ts) then
            table.insert(uuid_to_update, uuid)
        end
    end
    return uuid_to_update
end

--
-- Failover background function. Replica connection is the
-- connection to the nearest available server. Replica connection
-- is hold for each replicaset. This function periodically scans
-- replicasets and their replica connections. And some of them
-- appear to be disconnected or connected not to optimal replica.
--
-- If a connection is disconnected too long (more than
-- FAILOVER_DOWN_TIMEOUT), this function tries to connect to the
-- server with the lower priority. Priorities are specified in
-- weight matrix in config.
--
-- If a current replica connection has no the highest priority,
-- then this function periodically (once per FAILOVER_UP_TIMEOUT)
-- tries to reconnect to the best replica. When the connection is
-- established, it replaces the original replica.
--
local function failover_f()
    log.info('Start failover fiber')
    self.failover_fiber = lfiber.self()
    local min_timeout = math.min(consts.FAILOVER_UP_TIMEOUT,
                                 consts.FAILOVER_DOWN_TIMEOUT)
    -- This flag is used to avoid logging like:
    -- 'All is ok ... All is ok ... All is ok ...'
    -- each min_timeout seconds.
    local prev_was_ok = false
    while true do
::continue::
        local uuid_to_update = failover_collect_to_update()
        if #uuid_to_update == 0 then
            if not prev_was_ok then
                log.info('All replicas are ok')
                prev_was_ok = true
            end
            lfiber.sleep(min_timeout)
            goto continue
        end
        prev_was_ok = false
        local curr_ts = lfiber.time()
        for _, uuid in pairs(uuid_to_update) do
            local rs = self.replicasets[uuid]
            if self.errinj.ERRINJ_FAILOVER_CHANGE_CFG then
                rs = nil
                self.errinj.ERRINJ_FAILOVER_CHANGE_CFG = false
            end
            if rs == nil then
                log.info('Configuration has changed, restart failovering')
                lfiber.yield()
                goto continue
            end
            local old_replica = rs.replica
            if failover_is_candidate_connected(rs) then
                rs:set_candidate_as_replica()
            end
            if failover_need_update_candidate(rs, curr_ts) then
                rs:update_candidate()
            end
            if failover_need_down_priority(rs, curr_ts) then
                rs:down_replica_priority()
            end
            if old_replica ~= rs.replica then
                log.info('New replica "%s:%d" for replicaset "%s"',
                         rs.replica.conn.host, rs.replica.conn.port, rs.uuid)
            end
        end
        log.info('Failovering step is finished. Schedule next after %f '..
                 'seconds', min_timeout)
        lfiber.sleep(min_timeout)
    end
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function router_cfg(cfg)
    cfg = table.deepcopy(cfg)
    lcfg.check(cfg)
    if self.replicasets == nil then
        log.info('Starting router configuration')
    else
        log.info('Starting router reconfiguration')
    end
    self.replicasets = lreplicaset.buildall(cfg, self.replicasets or {})
    -- TODO: update existing route map in-place
    self.route_map = {}
    cfg.sharding = nil
    cfg.weights = nil
    cfg.zone = nil

    log.info("Calling box.cfg()...")
    for k, v in pairs(cfg) do
        log.info({[k] = v})
    end
    box.cfg(cfg)
    log.info("Box has been configured")
    -- Force net.box connection on cfg()
    for _, replicaset in pairs(self.replicasets) do
        replicaset:connect()
        replicaset:update_candidate()
    end
    if self.failover_fiber == nil then
        lfiber.create(failover_f)
    end
end

--------------------------------------------------------------------------------
-- Bootstrap
--------------------------------------------------------------------------------

local function cluster_bootstrap()
    local replicasets = {}
    for uuid, replicaset in pairs(self.replicasets) do
        table.insert(replicasets, replicaset)
        local count, err = replicaset:callrw('vshard.storage.buckets_count',
                                             {})
        if count == nil then
            return nil, err
        end
        if count > 0 then
            return nil, lerror.vshard(lerror.code.NON_EMPTY, {},
                                      'Cluster is already bootstrapped')
        end
    end
    local replicaset_count = #replicasets
    for bucket_id= 1, consts.BUCKET_COUNT do
        local replicaset = replicasets[1 + (bucket_id - 1) % replicaset_count]
        assert(replicaset ~= nil)
        log.info("Distributing bucket %d to %s", bucket_id, replicaset)
        local status, info =
            replicaset:callrw('vshard.storage.bucket_force_create',
                              {bucket_id})
        if not status then
            return nil, info
        end
    end
    return true
end

--------------------------------------------------------------------------------
-- Monitoring
--------------------------------------------------------------------------------

--
-- Collect info about a replicaset's replica with a specified
-- name. Found alerts are appended to @an alerts table, if a
-- replica does not exist or is unavailable. In a case of error
-- @a errcolor is returned, and GREEN else.
--
local function replicaset_instance_info(replicaset, name, alerts, errcolor,
                                        errcode_unreachable, params1,
                                        errcode_missing, params2)
    local info = {}
    local replica = replicaset[name]
    if replica then
        info.uri = replica.uri
        info.uuid = replica.uuid
        if replica:is_connected() then
            info.status = 'available'
        else
            info.status = 'unreachable'
            if errcode_unreachable then
                table.insert(alerts, lerror.alert(errcode_unreachable,
                                                  unpack(params1)))
                return info, errcolor
            end
        end
    else
        info.status = 'missing'
        if errcode_missing then
            table.insert(alerts, lerror.alert(errcode_missing, unpack(params2)))
            return info, errcolor
        end
    end
    return info, consts.STATUS.GREEN
end

local function router_info()
    local state = {
        replicasets = {},
        alerts = {},
        status = consts.STATUS.GREEN,
    }
    for rs_uuid, replicaset in pairs(self.replicasets) do
        -- Replicaset info parameters:
        -- * master instance info;
        -- * replica instance info;
        -- * replicaset uuid.
        --
        -- Instance info parameters:
        -- * uri;
        -- * uuid;
        -- * status - available, unreachable, missing.
        local rs_info = {uuid = replicaset.uuid}
        state.replicasets[replicaset.uuid] = rs_info

        -- Build master info.
        local info, color =
            replicaset_instance_info(replicaset, 'master', state.alerts,
                                     consts.STATUS.ORANGE,
                                     -- Master exists, but not
                                     -- available.
                                     lerror.code.UNREACHABLE_MASTER,
                                     {replicaset.uuid, 'disconnected'},
                                     -- Master does not exists.
                                     lerror.code.MISSING_MASTER,
                                     {replicaset.uuid})
        state.status = math.max(state.status, color)
        rs_info.master = info

        -- Build replica info.
        local uuid = replicaset.replica and replicaset.replica.uuid
        info = replicaset_instance_info(replicaset, 'replica', state.alerts)
        rs_info.replica = info
        if replicaset.replica and
           replicaset.replica ~= replicaset.priority_list[1] then
            -- If the replica is not optimal, then some replicas
            -- possibly are down.
            local a = lerror.alert(lerror.code.SUBOPTIMAL_REPLICA,
                                   replicaset.uuid)
            table.insert(state.alerts, a)
            state.status = math.max(state.status, consts.STATUS.YELLOW)
        end

        if rs_info.replica.status ~= 'available' and
           rs_info.master.status ~= 'available' then
            local a = lerror.alert(lerror.code.UNREACHABLE_REPLICASET,
                                   replicaset.uuid)
            table.insert(state.alerts, a)
            state.status = consts.STATUS.RED
        end
    end
    return state
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------

return {
    cfg = router_cfg;
    info = router_info;
    call = router_call;
    route = router_route;
    route_all = router_route_all;
    bootstrap = cluster_bootstrap;
    bucket_discovery = bucket_discovery;
    internal = self;
}
