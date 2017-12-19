-- vshard.cfg

local log = require('log')
local luri = require('uri')

--
-- Check replicaset config on correctness.
--
local function cfg_check_replicaset(replicaset)
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
local function cfg_check(shard_cfg)
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
        cfg_check_replicaset(replicaset)
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


return {
    check = cfg_check
}
