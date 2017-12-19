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
    tuple_extract_key = tuple_extract_key
}
