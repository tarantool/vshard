local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local netbox = require('net.box')
local consts = require('vshard.consts')
local util = require('vshard.util')

-- Internal state
local self = {
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
    replicasets = nil,
    -- Array of replicasets without known UUID
    -- { <master_uri>, <master_uri>, <master_uri>,. .. }
    replicasets_to_discovery = nil,
    -- Fiber to discovery uuid of replicasets.
    discovery_fiber = nil
}

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------

local function storage_schema_v1(username, password)
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
        'vshard.storage.bucket_stat',
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
    assert(mode == 'read' or mode == 'write')
    if not self.local_creplica.master then
        -- Add redirect here
        return consts.PROTO.NON_MASTER
    end

    local bucket = box.space._bucket:get({bucket_id})
    if bucket == nil then
        return consts.PROTO.WRONG_BUCKET
    end

    if bucket.status == consts.BUCKET.ACTIVE or
       (bucket.status == consts.BUCKET.SENDING and mode == 'read') then
        return consts.PROTO.OK
    end

    assert(bucket.status == consts.BUCKET_SENDING or
           bucket.status == consts.BUCKET.SENT or
           bucket.status == consts.BUCKET_STATUS_RECEIVING)

    return consts.PROTO.WRONG_BUCKET, {bucket.id, bucket.destination}
end

--
-- Return information about bucket
--
local function bucket_stat(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_force_create(bucket_id)')
    end

    local bucket = box.space._bucket:get({bucket_id})
    if bucket == nil or bucket.status ~= consts.BUCKET.ACTIVE then
        return consts.PROTO.WRONG_BUCKET, bucket_id
    end

    return consts.PROTO.OK, {
        id = bucket.id;
        status = bucket.status;
        destination = bucket.destination;
    }
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
    if mode ~= 'write' and mode ~= 'read' then
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
-- A background fiber to discovery UUID of all configured replicasets
--
local function replicaset_discovery_f()
    lfiber.name("replicaset_discovery")
    log.info("Started replicaset discovery")
    while true do
        local replicasets_to_discovery2 = {}
        for _, master_uri in pairs(self.replicasets_to_discovery) do
            local conn = netbox.new(master_uri, {reconnect_after =
                                                 consts.RECONNECT_TIMEOUT})
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

--
-- Try to find a new replicaset in existing ones by master uris.
-- @param new_replicaset One replicaset from sharding config.
-- @retval Not nil UUID of found replicaset.
-- @retval Nil Not found.
--
local function find_in_existing_replicasets(existing_replicasets,
                                            new_replicaset)
    for uuid, replicaset in pairs(existing_replicasets) do
        local master_uri = nil
        for _, new_replica in ipairs(new_replicaset) do
            if new_replica.master then
                master_uri = new_replica.uri
                break
            end
        end
        if master_uri == replicaset.master_uri then
            return uuid
        end
    end
end

--
-- Extract local replicaset, replica and replicasets to
-- discovery from a shard config.
-- @param cfg Shard config.
-- @param local_name Name of the current storage.
--
-- @retval Local replicaset, replica, replicasets to discovery and
--         already discovered replicasets.
--
local function parse_config(existing_replicasets, cfg, local_name)
    util.sanity_check_config(cfg)
    -- Replicaset to which belong the current storage.
    local local_replicaset = nil
    -- This storage.
    local local_replica = nil
    local new_self_replicasets = {}

    local replicasets_to_discovery = {}
    for _, new_replicaset in ipairs(cfg) do
        local uuid = find_in_existing_replicasets(existing_replicasets,
                                                  new_replicaset)
        if uuid ~= nil then
            log.info('Move old replicaset %s to a new config with no changes',
                     uuid)
            new_self_replicasets[uuid] = existing_replicasets[uuid]
        end
        -- Try to find self.
        local master = nil
        for _, new_replica in ipairs(new_replicaset) do
            if new_replica.name == local_name then
                -- Checked by sanity check.
                assert(local_replica == nil)
                local_replicaset = new_replicaset
                local_replica = new_replica
            end
            if new_replica.master then
                master = new_replica
            end
        end
        -- Ignore replicaset with no active master and do not
        -- rediscovery already discovered one.
        if master and not uuid then
            log.info('Schedule master %s to discovery later', master.uri)
            table.insert(replicasets_to_discovery, master.uri)
        end
    end
    if local_replicaset == nil then
        error(string.format("Cannot find replica %s in configuration",
                            local_name))
    end
    return local_replicaset, local_replica, replicasets_to_discovery,
           new_self_replicasets
end

--
-- Create a config for box using a shard config and a local
-- replicaset settings.
--
local function turn_shard_into_box_cfg(shard_cfg, local_replica,
                                       local_replicaset)
    local box_cfg = table.deepcopy(shard_cfg)
    box_cfg.sharding = nil
    box_cfg.listen = local_replica.uri
    box_cfg.replication = {}
    for _, replica in ipairs(local_replicaset) do
        assert(replica.uri ~= nil)
        table.insert(box_cfg.replication, replica.uri)
    end
    return box_cfg
end

--
-- Close connections to masters, which are no masters anymore.
-- Use a new list of replicasets.
--
local function reset_replicasets(new_replicasets)
    if self.replicasets == nil then
        self.replicasets = new_replicasets
        return
    end
    for uuid, replicaset in pairs(self.replicasets) do
        if new_replicasets[uuid] == nil then
            log.info('Close connection to an old master %s',
                     replicaset.master_uri)
            replicaset.master_conn:close()
        end
    end
    self.replicasets = new_replicasets
end

local function storage_cfg(cfg, name)
    if self.replicasets ~= nil then
        log.info("Starting reconfiguration of replica %s", name)
    else
        log.info("Staring configuration of replica %s", name)
    end

    if name == nil then
        error('Name must be specified')
    end
    if cfg.listen ~= nil or cfg.replication ~= nil then
        error('"listen" and "replication" can not be set manually')
    end
    local local_replicaset, local_replica, replicasets_to_discovery,
          new_self_replicasets =
            parse_config(self.replicasets or {}, cfg.sharding, name)
    self.local_creplicaset = local_replicaset
    self.local_creplica = local_replica
    if local_replica.master then
        log.info('I am master')
    end
    local uri = luri.parse(local_replica.uri)
    if uri.login == nil or uri.password == nil then
        error('URI must contain login and password')
    end
    log.info("Successfully found self in the configuration")
    cfg = turn_shard_into_box_cfg(cfg, local_replica, local_replicaset)
    log.info("Calling box.cfg()...")
    -- Stop discovering unti successfull cfg{} end.
    local saved_replicasets_to_discovery = self.replicasets_to_discovery
    self.replicasets_to_discovery = {}
    local status = pcall(box.cfg, cfg)
    if not status then
        -- Continue working with old config.
        self.replicasets_to_discovery = saved_replicasets_to_discovery
        box.error()
    end
    log.info("Box has been configured")
    box.once("vshard:storage:1", storage_schema_v1, uri.login, uri.password)
    self.replicasets_to_discovery = replicasets_to_discovery
    reset_replicasets(new_self_replicasets)
    if #self.replicasets_to_discovery > 0 and (self.discovery_fiber == nil or
       self.discovery_fiber:status() == 'dead') then
        -- Start background process to discovery replicasets
        self.discovery_fiber = lfiber.create(replicaset_discovery_f)
    end
end

--------------------------------------------------------------------------------
-- Monitoring
--------------------------------------------------------------------------------

--
-- Wait @a timeout seconds until all replicaset uuids are
-- discovered.
--
local function wait_discovery(timeout)
    if self.replicasets_to_discovery == nil then
        return true
    end
    if timeout == nil then
        timeout = 1
    end
    local total_time = 0
    while #self.replicasets_to_discovery > 0 and total_time < timeout do
        lfiber.sleep(0.1)
        total_time = total_time + 0.1
    end
    return #self.replicasets_to_discovery == 0
end

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

self.parse_config = parse_config

return {
    bucket_force_create = bucket_force_create;
    bucket_force_drop = bucket_force_drop;
    bucket_collect = bucket_collect;
    bucket_recv = bucket_recv;
    bucket_send = bucket_send;
    bucket_stat = bucket_stat;
    call = storage_call;
    cfg = storage_cfg;
    info = storage_info;
    wait_discovery = wait_discovery,
    internal = self;
}
