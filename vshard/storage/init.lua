local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local netbox = require('net.box')
local consts = require('vshard.consts')

-- Internal state
local self = {}

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------

local function storage_schema_v1(username, password)
    log.info("I'm master")

    log.info("Initializing schema")
    box.schema.user.create(username, {password = password})
    box.schema.user.grant(username, 'replication')

    local bucket = box.schema.space.create('_bucket')
    bucket:format({
        {'id', 'unsigned'},
        {'status', 'string'},
        {'destination', 'string', is_nullable = true}
    })
    bucket:create_index('pk', {parts = {'id'}})
    bucket:create_index('status', {parts = {'status'}, unique = false})

    local storage_api = {
        'vshard.storage.call',
        'vshard.storage.bucket_force_create',
        'vshard.storage.bucket_force_drop',
        'vshard.storage.bucket_collect',
        'vshard.storage.bucket_send',
        'vshard.storage.bucket_recv',
    }

    for _, name in ipairs(storage_api) do
        box.schema.func.create(name, {setuid = true})
        box.schema.user.grant(username, 'execute', 'function', name)
    end

    box.snapshot()
end

--------------------------------------------------------------------------------
-- Buckets
--------------------------------------------------------------------------------

local function bucket_check_state(bucket_id, mode)
    assert(type(bucket_id) == 'number')
    assert(mode == consts.MODE.READ or mode == consts.MODE.WRITE)
    local bucket = box.space._bucket:get({bucket_id})
    if bucket == nil then
        return consts.PROTO.WRONG_BUCKET
    end

    if bucket.status == consts.BUCKET.ACTIVE or
       (bucket.status == consts.BUCKET.SENDING and mode == consts.MODE.READ) then
        return consts.PROTO.OK
    end

    if mode == consts.MODE.WRITE and box.info.ro then
        -- Add redirect here
        return consts.PROTO.NON_MASTER
    end

    assert(bucket.status == consts.BUCKET_SENDING or
           bucket.status == consts.BUCKET.SENT or
           bucket.status == consts.BUCKET_STATUS_RECEIVING)

    return consts.PROTO.WRONG_BUCKET, {bucket.id, bucket.destination}
end

--
-- Create bucket manually for initial bootstrap, tests or
-- emergency cases
--
local function bucket_force_create(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_force_create(bucket_id)')
    end

    box.space._bucket:insert({bucket_id, consts.BUCKET.ACTIVE})
    return consts.PROTO.OK
end

--
-- Drop bucket manually for tests or emergency cases
--
local function bucket_force_drop(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_force_drop(bucket_id)')
    end

    box.space._bucket:delete({bucket_id})
    return consts.PROTO.OK
end


--
-- Receive bucket with its data
--
local function bucket_recv(bucket_id, from, data)
    if type(bucket_id) ~= 'number' or type(data) ~= 'table' then
        error('Usage: bucket_recv(bucket_id, data)')
    end

    local bucket = box.space._bucket:get({bucket_id})
    if bucket ~= nil then
        return consts.PROTO.BUCKET_ALREADY_EXISTS, bucket_id
    end

    bucket = box.space._bucket:insert({bucket_id, consts.BUCKET.RECEIVING, from})

    box.begin()

    -- Fill spaces with data
    for _, row in ipairs(data) do
        local space_id, space_data = row[1], row[2]
        local space = box.space[space_id]
        if space == nil then
            box.error(box.error.NO_SUCH_SPACE, space_id)
            assert(false)
        end
        for _, tuple in ipairs(space_data) do
            space:insert(tuple)
        end
    end

    -- Activate bucket
    bucket = box.space._bucket:replace({bucket_id, consts.BUCKET.ACTIVE})

    box.commit()
    return consts.PROTO.OK
end

local function bucket_collect_internal(bucket_id)
    local data = {}
    for k, space in pairs(box.space) do
        if type(k) == 'number' and space.index.bucket_id ~= nil then
            local space_data = space.index.bucket_id:select({bucket_id})
            table.insert(data, {space.id, space_data})
        end
    end

    return consts.PROTO.OK, data
end

--
-- Collect bucket content
--
local function bucket_collect(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_collect(bucket_id)')
    end

    local bucket = box.space._bucket:get({bucket_id})
    if bucket == nil or bucket.status ~= consts.BUCKET.ACTIVE then
        return consts.PROTO.WRONG_BUCKET, bucket_id
    end

    return bucket_collect_internal(bucket_id)
end

--
-- Send a bucket to other replicaset
--
local function bucket_send(bucket_id, destination)
    if type(bucket_id) ~= 'number' or type(destination) ~= 'string' then
        error('Usage: bucket_send(bucket_id, destination)')
    end

    local bucket = box.space._bucket:get({bucket_id})
    if bucket == nil or bucket.status ~= consts.BUCKET.ACTIVE then
        return consts.PROTO.WRONG_BUCKET, bucket_id
    end
    local replicaset = self.replicasets[destination]
    if replicaset == nil then
        return consts.PROTO.NO_SUCH_REPLICASET, destination
    end

    if destination == box.info.cluster.uuid then
        return consts.PROTO.MOVE_TO_SELF, bucket_id, destination
    end

    local status, data = bucket_collect_internal(bucket_id)
    if status ~= consts.PROTO.OK then
        return status, data
    end

    box.space._bucket:replace({bucket_id, consts.BUCKET.SENDING, destination})

    local conn = replicaset.master_conn
    local status, info = pcall(conn.call, conn, 'vshard.storage.bucket_recv',
                               {bucket_id, box.info.cluster.uuid, data})
    if status ~= true then
        -- Rollback bucket state.
        box.space._bucket:replace({bucket_id, consts.BUCKET.ACTIVE})
        return status, info
    end

    box.space._bucket:replace({bucket_id, consts.BUCKET.SENT, destination})

    return true
end


--------------------------------------------------------------------------------
-- API
--------------------------------------------------------------------------------

-- Call wrapper
-- There is two modes for call operation: read and write, explicitly used for
-- call protocol: there is no way to detect what corresponding function does.
-- NOTE: may be a custom function call api without any checks is needed,
-- for example for some monitoring functions.
local function storage_call(bucket_id, mode, name, args)
    if mode == 'write' then
        mode = consts.MODE.WRITE
    elseif mode == 'read' then
        mode = consts.MODE.READ
    else
        error('Unknown mode: '..tostring(mode))
    end

    local status, info = bucket_check_state(bucket_id, mode)
    if status ~= consts.PROTO.OK then
        return status, info
    end
    -- TODO: implement box.call()
    return consts.PROTO.OK, netbox.self:call(name, args)
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
    -- TODO: doesn't work
    -- cfg.read_only = not local_creplica.master
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

--------------------------------------------------------------------------------
-- Monitoring
--------------------------------------------------------------------------------

local function storage_info()
    local ibuckets = setmetatable({}, { __serialize = 'mapping' })

    for _, bucket in box.space._bucket:pairs() do
        ibuckets[bucket.id] = {
            id = bucket.id;
            status = bucket.status;
            destination = bucket.destination;
        }
    end

    local ireplicaset = {}
    for uuid, replicaset in pairs(self.replicasets) do
        ireplicaset[uuid] = {
            uuid = uuid;
            master = {
                uri = replicaset.master_uri;
                uuid = replicaset.master_conn and replicaset.master_conn.peer_uuid;
                state = replicaset.master_conn and replicaset.master_conn.state;
                error = replicaset.master_conn and replicaset.master_conn.error;
            };
        };
    end

    return {
        buckets = ibuckets;
        replicasets = ireplicaset;
    }
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------

return {
    bucket_force_create = bucket_force_create;
    bucket_force_drop = bucket_force_drop;
    bucket_collect = bucket_collect;
    bucket_recv = bucket_recv;
    bucket_send = bucket_send;
    call = storage_call;
    cfg = storage_cfg;
    info = storage_info;
    internal = self;
}
