local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local consts = require('vshard.consts')
local codes = require('vshard.codes')
local lcfg = require('vshard.cfg')
local lreplicaset = require('vshard.replicaset')
local util = require('vshard.util')

-- Internal state
local self = {}

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
        local stat, err = replicaset:call('vshard.storage.bucket_stat',
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
        elseif err.code ~= codes.WRONG_BUCKET then
            unreachable_uuid = replicaset.uuid
        end
    end
    local errcode = nil
    if unreachable_uuid then
        errcode = codes.REPLICASET_IS_UNREACHABLE
    elseif is_transfer_in_progress then
        errcode = codes.TRANSFER_IS_IN_PROGRESS
    else
        -- All replicasets were scanned, but a bucket was not
        -- found anywhere, so most likely it does not exist. It
        -- can be wrong, if rebalancing is in progress, and a
        -- bucket was found to be RECEIVING on one replicaset, and
        -- was not found on other replicasets (it was sent during
        -- discovery).
        errcode = codes.NO_ROUTE_TO_BUCKET
    end

    return nil, {
        code = errcode,
        bucket_id = bucket_id,
        unreachable_uuid = unreachable_uuid,
    }
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
    local is_transfer_in_progress = false
    if bucket_id > consts.BUCKET_COUNT or bucket_id < 0 then
        error('Bucket is unreachable: bucket id is out of range')
    end
    repeat
        replicaset, err = bucket_resolve(bucket_id)
        if replicaset then
            local storage_call_status, call_status, call_error =
                replicaset:call('vshard.storage.call',
                                {bucket_id, mode, func, args})
            if storage_call_status then
                if call_status == nil and call_error ~= nil then
                    return call_status, call_error
                else
                    return call_status
                end
            end
            err = call_status
            if err.code == codes.WRONG_BUCKET or
               err.code == codes.TRANSFER_IS_IN_PROGRESS then
                self.route_map[bucket_id] = nil
            elseif err.code == codes.NON_MASTER then
                log.warn("Replica %s is not master for replicaset %s anymore,"..
                         "please update configuration!",
                          replicaset.master.uuid, replicaset.uuid)
                error("Can't found master for "..tostring(replicaset.uuid))
            elseif err.code == codes.BOX_ERROR then
                -- Re-throw original error
                error(err.error)
            else
                assert(false)
            end
        end
    until not (lfiber.time() <= tstart + consts.CALL_TIMEOUT)
    if err then
        if err.code == codes.TRANSFER_IS_IN_PROGRESS then
            error('Bucket transfer is in progress')
        elseif err.code == codes.NO_ROUTE_TO_BUCKET then
            error('Bucket is unreachable: no route to bucket')
        else
            assert(err.code == codes.REPLICASET_IS_UNREACHABLE)
            error(string.format('Bucket is unreachable: replicaset\'s "%s" '..
                                'master is unreachable',
                                err.unreachable_uuid))
        end
    else
        return box.error(box.error.TIMEOUT)
    end
end

--
-- Get netbox connection by bucket identifier.
-- @param bucket_id Bucket identifier.
-- @retval Netbox connection.
--
local function router_route(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: router.route(bucket_id)')
    end
    local replicaset, err = bucket_resolve(bucket_id)
    if replicaset == nil then
        return nil, err
    end
    local conn, err = replicaset:connect()
    if conn == nil then
        return nil, err
    end
    return conn
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function router_cfg(cfg)
    cfg = table.deepcopy(cfg)
    lcfg.check(cfg.sharding)
    if self.replicasets == nil then
        log.info('Starting router configuration')
    else
        log.info('Starting router reconfiguration')
    end
    self.replicasets = lreplicaset.buildall(cfg.sharding,
                                             self.replicasets or {})
    -- TODO: update existing route map in-place
    self.route_map = {}
    cfg.sharding = nil

    log.info("Calling box.cfg()...")
    for k, v in pairs(cfg) do
        log.info({[k] = v})
    end
    box.cfg(cfg)
    log.info("Box has been configured")
    -- Force net.box connection on cfg()
    for _, replicaset in pairs(self.replicasets) do
        replicaset:connect()
    end
end

--------------------------------------------------------------------------------
-- Bootstrap
--------------------------------------------------------------------------------

local function cluster_bootstrap()
    local replicasets = {}
    for uuid, replicaset in pairs(self.replicasets) do
        table.insert(replicasets, replicaset)
    end
    local replicaset_count = #replicasets
    for bucket_id= 1, consts.BUCKET_COUNT do
        local replicaset = replicasets[1 + (bucket_id - 1) % replicaset_count]
        assert(replicaset ~= nil)
        log.info("Distributing bucket %d to %s", bucket_id, replicaset)
        local status, info =
            replicaset:call('vshard.storage.bucket_force_create', {bucket_id})
        if not status then
            -- TODO: handle errors properly
            error('Failed to bootstrap cluster: '..tostring(info))
        end
    end
end

--------------------------------------------------------------------------------
-- Monitoring
--------------------------------------------------------------------------------

local function router_info()
    local ireplicaset = {}
    for _, replicaset in pairs(self.replicasets) do
        table.insert(ireplicaset, {
            master = {
                uri = replicaset.master.uri;
                uuid = replicaset.master.conn and replicaset.master.conn.peer_uuid;
                state = replicaset.master.conn and replicaset.master.conn.state;
                error = replicaset.master.conn and replicaset.master.conn.error;
            };
        });
    end

    return {
        replicasets = ireplicaset;
    }
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------

return {
    cfg = router_cfg;
    info = router_info;
    call = router_call;
    route = router_route;
    bootstrap = cluster_bootstrap;
    bucket_discovery = bucket_discovery;
    internal = self;
}
