local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local netbox = require('net.box')
local consts = require('vshard.consts')

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

    return consts.PROTO.WRONG_BUCKET, result
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

--
-- Fucking net.box doesn't reconnect automatically as it supposed to do.
-- Use this background fiber to re-create buggy net.box instances.
-- https://github.com/tarantool/tarantool/issues/2959
--
local function netbox_workaround_f()
    lfiber.name("netbox_workaround")
    lfiber.sleep(0)
    while true do
        for _, replicaset in pairs(self.replicasets) do
            local conn = replicaset.master_conn
            if not conn:ping() then
                -- Re-create net.box
                replicaset.master_conn = netbox.connect(replicaset.master_uri)
                conn:close()
            end
        end
        lfiber.sleep(0.1)
    end
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function router_cfg(cfg)
    assert(self.replicasets == nil, "reconfiguration is not implemented yet")
    log.info("Starting cluster for replica '%s'", name)

    local replicasets = {}
    local replicaset_count = 0
    for _, creplicaset in ipairs(cfg.sharding) do
        local replicaset = {}
        local cmaster = nil
        for _, creplica in ipairs(creplicaset) do
            if creplica.master then
                cmaster = creplica
            end
        end
        -- Ignore replicaset without active master
        if cmaster then
            replicaset_count = replicaset_count + 1
            replicasets[replicaset_count] = {
                id = replicaset_count;
                master_uri = cmaster.uri;
                master_conn = netbox.connect(cmaster.uri)
            }
        end
    end

    self.replicasets = replicasets
    lfiber.create(netbox_workaround_f)

    log.info("Calling box.cfg()...")
    cfg.sharding = nil
    for k, v in pairs(cfg) do
        log.info({[k] = v})
    end
    box.cfg(cfg)
    log.info("box has been configured")
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
    for id, replicaset in pairs(self.replicasets) do
        ireplicaset[id] = {
            master = {
                uri = replicaset.master_uri;
                uuid = replicaset.master_conn and replicaset.master_conn.peer_uuid;
                state = replicaset.master_conn and replicaset.master_conn.state;
                error = replicaset.master_conn and replicaset.master_conn.error;
            };
        };
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
