local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local consts = require('vshard.consts')
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
        return consts.PROTO.OK, replicaset
    end

    log.info("Discovering bucket %d", bucket_id)
    for _, replicaset in pairs(self.replicasets) do
        local status, result = replicaset:call('vshard.storage.bucket_stat',
                                               {bucket_id})
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
    local status, reason
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
            local conn = replicaset.master.conn
            if not conn:is_connected() then
                -- Skip this event loop iteration and allow netbox
                -- to try to reconnect.
                lfiber.yield()
            end
            local status, info =
                replicaset:call('vshard.storage.call',
                                {bucket_id, mode, func, args})
            if status == consts.PROTO.OK then
                return info
            elseif status == consts.PROTO.WRONG_BUCKET then
                route_map[bucket_id] = nil
            elseif status == consts.PROTO.NON_MASTER then
                error("Can't found master for "..tostring(replicaset.uuid))
            elseif status == consts.PROTO.BOX_ERROR then
                -- Re-throw original error
                error(info)
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
    cfg = table.deepcopy(cfg)
    util.sanity_check_config(cfg.sharding)
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
    bootstrap = cluster_bootstrap;
    bucket_discovery = bucket_discovery;
    internal = self;
}
