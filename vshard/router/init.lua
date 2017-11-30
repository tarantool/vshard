local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local netbox = require('net.box')
local consts = require('vshard.consts')
local util = require('vshard.util')

-- Internal state
local self = {}

--------------------------------------------------------------------------------
-- Routing
--------------------------------------------------------------------------------

--
-- All known replicasets used for bucket re-balancing
--
-- {
--     [pos] = { -- replicaset #1
--         master_uri = <master_uri>
--         master_conn = <master net.box>
--     },
--     ...
-- }
self.replicasets = nil

-- Bucket map cache
-- NOTE: it is should be good to store bucket map in memtx space
self.route_map = {}

-- Search bucket in whole cluster
local function bucket_discovery(bucket_id)
    local replicaset = self.route_map[bucket_id]
    if replicaset ~= nil then
        return consts.PROTO.OK, replicaset
    end

    log.info("Discovering bucket %d", bucket_id)
    for _, replicaset in pairs(self.replicasets) do
        local conn = replicaset.master_conn
        local status, result = conn:call('vshard.storage.bucket_stat', {bucket_id})
        if status == consts.PROTO.OK then
            self.route_map[bucket_id] = replicaset
            return consts.PROTO.OK, replicaset
        end
    end

    return consts.PROTO.WRONG_BUCKET
end

-- Resolve bucket id to replicaset uuid
local function bucket_resolve(bucket_id)
    local replicaset
    local replicaset = self.route_map[bucket_id]
    if replicaset ~= nil then
        return consts.PROTO.OK, replicaset
    end
    -- Replicaset removed from cluster, perform discovery
    local status
    status, reason = bucket_discovery(bucket_id)
    if status ~= consts.PROTO.OK then
        return status, reason
    end
    replicaset = reason
    return consts.PROTO.OK, replicaset
end

-- Perform shard operation
-- Function will restart operation after wrong bucket response until timeout
-- is reached
local function router_call(bucket_id, mode, func, args)
    local replicaset, status, reason
    local tstart = lfiber.time()
    repeat
        status, reason = bucket_resolve(bucket_id)
        if status == consts.PROTO.OK then
            replicaset = reason
            local conn = replicaset.master_conn
            if not conn:is_connected() then
                -- Skip this event loop iteration and allow netbox
                -- to try to reconnect.
                lfiber.yield()
            end
            local status, info = conn:call('vshard.storage.call',
                                           {bucket_id, mode, func, args})
            if status == consts.PROTO.OK then
                return info
            elseif status == consts.PROTO.WRONG_BUCKET then
                route_map[bucket_id] = nil
            else
                error("Unknown result code: "..tostring(info))
            end
        end
    until not (lfiber.time() <= tstart + consts.CALL_TIMEOUT)
    return box.error(box.error.TIMEOUT)
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function router_cfg(cfg)
    util.sanity_check_config(cfg.sharding)
    cfg = table.deepcopy(cfg)
    if self.replicasets == nil then
        log.info('Starting router configuration')
    else
        log.info('Starting router reconfiguration')
    end

    local replicasets = {}
    local move_to_new_config = {}
    for _, replicaset in ipairs(cfg.sharding) do
        local master = nil
        for _, replica in ipairs(replicaset) do
            if replica.master then
                master = replica
            end
        end
        -- Ignore replicaset with no master.
        if master then
            -- Try to reuse existing connection.
            local old_idx = nil
            for idx, old_replicaset in pairs(self.replicasets or {}) do
                if old_replicaset.master_uri == master.uri then
                    old_idx = idx
                    break
                end
            end
            if old_idx ~= nil then
                log.info('Move master %s to a new config', master.uri)
                move_to_new_config[old_idx] = true
                table.insert(replicasets, self.replicasets[old_idx])
            else
                log.info('Connect to a new replicaset master %s', master.uri)
                local master_conn = netbox.connect(master.uri,
                    {reconnect_after = consts.RECONNECT_TIMEOUT})
                table.insert(replicasets, {
                    master_uri = master.uri,
                    master_conn = master_conn
                })
            end
        end
    end
    for idx, replicaset in pairs(self.replicasets or {}) do
        if not move_to_new_config[idx] then
            log.info('Kill connection to an old master %s',
                     replicaset.master_uri)
            replicaset.master_conn:close()
        end
    end
    self.replicasets = replicasets

    log.info("Calling box.cfg()...")
    cfg.sharding = nil
    for k, v in pairs(cfg) do
        log.info({[k] = v})
    end
    box.cfg(cfg)
    log.info("Box has been configured")
end

--------------------------------------------------------------------------------
-- Bootstrap
--------------------------------------------------------------------------------

local function cluster_bootstrap()
    local replicasets = self.replicasets
    local replicaset_count = #replicasets;
    for bucket_id=1,consts.BUCKET_COUNT do
        local replicaset = replicasets[1 + (bucket_id - 1) % replicaset_count]
        assert(replicaset ~= nil)
        log.info("Distributing bucket %d to master %s", bucket_id,
                 replicaset.master_uri)
        local conn = replicaset.master_conn
        local status, info = conn:call('vshard.storage.bucket_force_create',
                                       {bucket_id})
        if status ~= consts.PROTO.OK then
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
                uri = replicaset.master_uri;
                uuid = replicaset.master_conn and replicaset.master_conn.peer_uuid;
                state = replicaset.master_conn and replicaset.master_conn.state;
                error = replicaset.master_conn and replicaset.master_conn.error;
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
    bootstrap = cluster_bootstrap;
    bucket_discovery = bucket_discovery;
    internal = self;
}
