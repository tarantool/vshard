local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local netbox = require('net.box')

-- Internal state
local self = {}

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------

local function storage_schema_v1(username, password)
    log.info("I'm master")
    log.info("Creating schema")
    box.schema.user.create(username, {password = password})
    box.schema.user.grant(username, 'replication')
end

--------------------------------------------------------------------------------
-- Re-balancing
--------------------------------------------------------------------------------

--
-- All known replicasets used for bucket re-balancing
--
-- {
--     [uuid] = { -- replicaset #1
--         master_uri = <master_uri>
--         master_conn = <master net.box>
--         uuid = <uuid>,
--     },
--     ...
-- }
self.replicasets = nil

-- Array of replicasets without known UUID
-- { <master_uri>, <master_uri>, <master_uri>,. .. }
self.replicasets_to_discovery = nil

--
-- A background fiber to discovery UUID of all configured replicasets
--
local function replicaset_discovery_f()
    lfiber.name("replicaset_discovery")
    log.info("Started replicaset discovery")
    while true do
        local replicasets_to_discovery2 = {}
        for _, master_uri in pairs(self.replicasets_to_discovery) do
            local conn = netbox.new(master_uri)
            local status, uuid = pcall(function()
                return conn.space._schema:get('cluster')[2]
            end)
            if status then
                log.info("Discovered replicaset %s on %s", uuid, master_uri)
                if self.replicasets[uuid] ~= nil then
                    log.warn("Duplicate replicaset %s on %s and %s",
                             self.replicasets[uuid].master_uri, master_uri)
                else
                    self.replicasets[uuid] = {
                        master_uri = master_uri;
                        master_conn = conn;
                        uuid = uuid
                    }
                end
            else
                conn:close()
                conn = nil
                table.insert(replicasets_to_discovery2, master_uri)
            end
        end
        self.replicasets_to_discovery = replicasets_to_discovery2
        if #replicasets_to_discovery2 == 0 then
            break
        end
    end
    log.info("Discovered all configured replicasets")
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function storage_cfg(cfg, name)
    assert(self.replicasets == nil, "reconfiguration is not implemented yet")
    log.info("Starting cluster for replica '%s'", name)

    -- TODO: check configuration
    assert(name ~= nil, "name is not empty")
    assert(cfg.listen == nil, "cfg.listen should be empty")
    assert(cfg.replication == nil, "cfg.replication should be empty")

    self.replicasets = {}

    local local_creplicaset = nil
    local local_creplica = nil

    local replicasets_to_discovery = {}
    for _, creplicaset in ipairs(cfg.sharding) do
        local replicaset = {}
        local cmaster = nil
        for _, creplica in ipairs(creplicaset) do
            if creplica.name == name then
                assert(local_replica == nil, "duplicate name")
                local_creplicaset = creplicaset
                local_creplica = creplica
            end
            if creplica.master then
                cmaster = creplica
            end
        end
        -- Ignore replicaset without active master
        if cmaster then
            table.insert(replicasets_to_discovery, cmaster.uri)
        end
    end
    if local_creplicaset == nil then
        error(string.format("Cannot find replica '%s' in configuration", name))
    end
    log.info("Successfully found myself in the configuration")
    cfg.listen = local_creplica.uri
    cfg.replication = {}
    for _, creplica in ipairs(local_creplicaset) do
        assert(creplica.uri ~= nil, "missing .uri key for replica")
        table.insert(cfg.replication, creplica.uri)
    end
    assert(#cfg.replication > 0, "empty replicaset")
    log.info("Calling box.cfg()...")
    cfg.sharding = nil
    for k, v in pairs(cfg) do
        log.info({[k] = v})
    end
    box.cfg(cfg)
    log.info("box has been configured")

    local uri = luri.parse(local_creplica.uri)
    assert(uri.login ~= nil, "login is not empty")
    assert(uri.password ~= nil, "password is not empty")
    box.once("vshard:storage:1", storage_schema_v1, uri.login, uri.password)

    self.replicasets_to_discovery = replicasets_to_discovery

    -- Start background process to discovery replicasets
    lfiber.create(replicaset_discovery_f)
end

return {
    cfg = storage_cfg;
    internal = self;
}
