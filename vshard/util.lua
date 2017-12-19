local log = require('log')
local luri = require('uri')
local consts = require('vshard.consts')
local netbox = require('net.box')

--
-- Check replicaset config on correctness.
--
local function sanity_check_replicaset(replicaset)
    if type(replicaset) ~= 'table' then
        error('Replicaset must be a table')
    end
    if type(replicaset.replicas) ~= 'table' then
        error('Replicaset.replicas must be array of replicas')
    end
    local master_is_found = false
    for k, replica in pairs(replicaset.replicas) do
        if type(replica.uri) ~= 'string' then
            error('replica uri must be string')
        end
        local uri = luri.parse(replica.uri)
        if uri.login == nil or uri.password == nil then
            error('URI must contain login and password')
        end
         if type(replica.name) ~= 'string' then
            error('replica name must be string')
        end
        if replica.master ~= nil then
            if type(replica.master) ~= 'boolean' then
                error('"master" must be boolean')
            end
            if replica.master then
                if master_is_found then
                    error('Only one master is allowed per replicaset')
                end
                master_is_found = true
            end
        end
    end
end

--
-- Check sharding config on correctness. Check types, name and uri
-- uniqueness, master count (in each replicaset must by <= 1).
--
local function sanity_check_config(shard_cfg)
    if type(shard_cfg) ~= 'table' then
        error('Sharding config must be array of replicasets')
    end
    local uuids = {}
    local uris = {}
    for replicaset_uuid, replicaset in pairs(shard_cfg) do
        if uuids[replicaset_uuid] then
            error(string.format('Duplicate uuid %s', replicaset_uuid))
        end
        uuids[replicaset_uuid] = true
        sanity_check_replicaset(replicaset)
        for replica_uuid, replica in pairs(replicaset.replicas) do
            if uris[replica.uri] then
                error(string.format('Duplicate uri %s', replica.uri))
            end
            uris[replica.uri] = true
            if uuids[replica_uuid] then
                error(string.format('Duplicate uuid %s', replica_uuid))
            end
            uuids[replica_uuid] = true
        end
    end
end

--
-- on_connect() trigger for net.box
--
local function netbox_on_connect(conn)
    log.info("connected to %s:%s", conn.host, conn.port)
end

--
-- on_disconnect() trigger for net.box
--
local function netbox_on_disconnect(conn)
    log.info("disconnected from %s:%s", conn.host, conn.port)
end

--
-- Create net.box connection to master
--
local function replicaset_connect(replicaset)
    local master = replicaset.master
    if master == nil then
        return consts.PROTO.MISSING_MASTER
    end
    local conn = master.conn
    if conn == nil then
        -- Use wait_connected = false to prevent races on parallel requests
        conn = netbox.connect(master.uri, {
            reconnect_after = consts.RECONNECT_TIMEOUT,
            wait_connected = false
        })
        conn:on_connect(netbox_on_connect)
        conn:on_disconnect(netbox_on_disconnect)
        master.conn = conn
        conn:ping()
    end
    return consts.PROTO.OK, conn
end

--
-- Destroy net.box connection to master
--
local function replicaset_disconnect(replicaset)
    local master = replicaset.master
    if master == nil then
       return consts.PROTO.OK
    end
    local conn = replicaset.master.conn
    replicaset.master.conn = nil
    conn:close()
    return consts.PROTO.OK
end

--
-- Call a function on remote storage
--
local function replicaset_call(replicaset, func, args)
    assert(type(func) == 'string', 'function name')
    assert(type(args) == 'table', 'function arguments')
    local status, conn = replicaset_connect(replicaset)
    if status ~= consts.PROTO.OK then
        return status, conn
    end
    local pstatus, status, result = pcall(conn.call, conn, func, args)
    if not pstatus then
        log.error("Exception during calling '%s' on '%s': %s", func,
                  replicaset.master.uuid, status)
        return consts.PROTO.BOX_ERROR, status
    end
    if status == consts.PROTO.OK then
        return status, result
    end
    if status == consts.PROTO.NON_MASTER then
        log.warn("Replica %s is not master for replicaset %s anymore,"..
                 "please update configuration!",
                  replicaset.master.uuid, replicaset.uuid)
    end
    return status, result
end

--
-- Nice formatter for replicaset
--
local function replicaset_tostring(replicaset)
    local uri = ''
    if replicaset.master then
        uri = replicaset.master.uri
    end
    return string.format('Replicaset(uuid=%s, master=%s)',
        replicaset.uuid, uri)
end

--
-- Meta-methods
--
local replicaset_mt = {
    __index = {
        connect = replicaset_connect;
        disconnect = replicaset_disconnect;
        call = replicaset_call;
    };
    __tostring = replicaset_tostring;
}

--
-- Build replicasets map in a format: {
--     [replicaset_uuid] = {
--         replicas = array of maps of type {
--             uri = string,
--             name = string,
--             uuid = string,
--             conn = netbox connection
--         },
--         master = <master replica from the array above>
--         uuid = <replicaset_uuid>,
--     },
--     ...
-- }
--
local function build_replicasets(shard_cfg, existing_replicasets)
    local new_replicasets = {}
    for replicaset_uuid, replicaset in pairs(shard_cfg.sharding) do
        local new_replicaset = setmetatable({
            replicas = {},
            uuid = replicaset_uuid
        }, replicaset_mt)
        for replica_uuid, replica in pairs(replicaset.replicas) do
            local new_replica = {uri = replica.uri, name = replica.name,
                                 uuid = replica_uuid}
            local existing_rs = existing_replicasets[replicaset_uuid]
            if existing_rs ~= nil and existing_rs.replicas[replica_uuid] then
                new_replica.conn = existing_rs.replicas[replica_uuid].conn
            end
            new_replicaset.replicas[replica_uuid] = new_replica
            if replica.master then
                new_replicaset.master = new_replica
            end
        end
        new_replicasets[replicaset_uuid] = new_replicaset
    end
    return new_replicasets
end

--
-- Extract parts of a tuple.
-- @param tuple Tuple to extract a key from.
-- @param parts Array of index parts. Each part must contain
--        'fieldno' attribute.
--
-- @retval Extracted key.
--
local function tuple_extract_key(tuple, parts)
    local key = {}
    for _, part in ipairs(parts) do
        table.insert(key, tuple[part.fieldno])
    end
    return key
end

return {
    sanity_check_config = sanity_check_config,
    build_replicasets = build_replicasets,
    tuple_extract_key = tuple_extract_key
}
