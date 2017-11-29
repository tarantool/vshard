--
-- Check replicaset config on correctness.
--
local function sanity_check_replicaset(replicaset)
    if type(replicaset) ~= 'table' then
        error('Replicaset must be array of servers')
    end
    local i = 1
    local master_is_found = false
    for k, server in pairs(replicaset) do
        if k ~= i then
            error('Replicaset must be array of servers')
        end
        if type(server.uri) ~= 'string' then
            error('Server uri must be string')
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
        i = i + 1
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
    local uris = {}
    local names = {}
    local i = 1
    for k, replicaset in pairs(shard_cfg) do
        if k ~= i then
            error('Sharding config must be array of replicasets')
        end
        sanity_check_replicaset(replicaset)
        for _, replica in ipairs(replicaset) do
            if uris[replica.uri] then
                error(string.format('Duplicate uri %s', replica.uri))
            end
            uris[replica.uri] = true
            if names[replica.name] then
                error(string.format('Duplicate name %s', replica.name))
            end
            names[replica.name] = true
        end
        i = i + 1
    end
end

return {
    sanity_check_config = sanity_check_config
}
