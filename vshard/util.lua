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
    if type(replicaset.servers) ~= 'table' then
        error('Replicaset.servers must be array of servers')
    end
    local master_is_found = false
    for k, server in pairs(replicaset.servers) do
        if type(server.uri) ~= 'string' then
            error('Server uri must be string')
        end
        local uri = luri.parse(server.uri)
        if uri.login == nil or uri.password == nil then
            error('URI must contain login and password')
        end
         if type(server.name) ~= 'string' then
            error('Server name must be string')
        end
        if server.master ~= nil then
            if type(server.master) ~= 'boolean' then
                error('"master" must be boolean')
            end
            if server.master then
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
        for replica_uuid, replica in pairs(replicaset.servers) do
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
-- Build replicasets map in a format: {
--     [replicaset_uuid] = {
--         servers = array of maps of type {
--             uri = string,
--             name = string,
--             uuid = string,
--             conn = netbox connection
--         },
--         master = <master server from the array above>
--         uuid = <replicaset_uuid>,
--     },
--     ...
-- }
--
local function build_replicasets(shard_cfg, existing_replicasets, do_connection)
    local new_replicasets = {}
    for replicaset_uuid, replicaset in pairs(shard_cfg.sharding) do
        local new_replicaset = {servers = {}, uuid = replicaset_uuid}
        for replica_uuid, replica in pairs(replicaset.servers) do
            local new_replica = {uri = replica.uri, name = replica.name,
                                 uuid = replica_uuid}
            local existing_rs = existing_replicasets[replicaset_uuid]
            if existing_rs ~= nil and existing_rs.servers[replica_uuid] then
                new_replica.conn = existing_rs.servers[replica_uuid].conn
            end
            new_replicaset.servers[replica_uuid] = new_replica
            if replica.master then
                new_replicaset.master = new_replica
            end
        end
        local rs_master = new_replicaset.master
        if do_connection and rs_master ~= nil and rs_master.conn == nil then
            rs_master.conn = netbox.new(rs_master.uri,
                                        {reconnect_after = consts.RECONNECT_TIMEOUT})
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
    for _, part in pairs(parts) do
        table.insert(key, tuple[part.fieldno])
    end
    return key
end

return {
    sanity_check_config = sanity_check_config,
    build_replicasets = build_replicasets,
    tuple_extract_key = tuple_extract_key
}
