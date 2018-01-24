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
    if replicaset.weight ~= nil and (type(replicaset.weight) ~= 'number' or
                                     replicaset.weight < 0) then
        error('Replicaset weight must be either nil or non-negative number')
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
        if replica.zone ~= nil and type(replica.zone) ~= 'number' and
           type(replica.zone) ~= 'string' then
            error('replica zone must be either string or number')
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
-- Check weights map on correctness.
--
local function cfg_check_weights(weights)
    if type(weights) ~= 'table' then
        error('weights must be map of maps')
    end
    for zone1, v in pairs(weights) do
        if type(zone1) ~= 'number' and type(zone1) ~= 'string' then
            -- Zone1 can be not number or string, if an user made
            -- this: weights = {[{1}] = ...}. In such a case
            -- {1} is the unaccassible key of a lua table, which
            -- is available only via pairs.
            error('zone identifier must be either string or number')
        end
        if type(v) ~= 'table' then
            error('zone must be map of relative weights of other zones')
        end
        for zone2, weight in pairs(v) do
            if type(zone2) ~= 'number' and type(zone2) ~= 'string' then
                error('zone identifier must be either string or number')
            end
            if type(weight) ~= 'number' or weight < 0 then
                error('weight must be either nil or non-negative number')
            end
            if zone2 == zone1 and weight ~= 0 then
                error('weight of zone self must be either nil or 0')
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
        error('Сonfig must be map of options')
    end
    if type(shard_cfg.sharding) ~= 'table' then
        error('Sharding config must be array of replicasets')
    end
    if shard_cfg.weights ~= nil then
        cfg_check_weights(shard_cfg.weights)
    end
    if shard_cfg.zone ~= nil and type(shard_cfg.zone) ~= 'number' and
       type(shard_cfg.zone) ~= 'string' then
        error('Config zone must be either number or string')
    end
    if shard_cfg.bucket_count ~= nil and
       (type(shard_cfg.bucket_count) ~= 'number' or
        shard_cfg.bucket_count <= 0 or
        math.floor(shard_cfg.bucket_count) ~= shard_cfg.bucket_count) then
        error('Bucket count must be positive integer')
    end
    if shard_cfg.rebalancer_disbalance_threshold ~= nil then
        local t = shard_cfg.rebalancer_disbalance_threshold
        if type(t) ~= 'number' or t < 0 then
            error('Rebalancer disbalance threshold must be non-negative number')
        end
    end
    if shard_cfg.rebalancer_max_receiving ~= nil then
        local t = shard_cfg.rebalancer_max_receiving
        if type(t) ~= 'number' or t <= 0 or math.floor(t) ~= t then
            error('Rebalancer max receiving bucket count must be '..
                  'positive integer')
        end
    end
    local uuids = {}
    local uris = {}
    for replicaset_uuid, replicaset in pairs(shard_cfg.sharding) do
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

--
-- Nullify non-box options.
--
local function prepare_for_box_cfg(cfg)
    cfg.sharding = nil
    cfg.weights = nil
    cfg.zone = nil
    cfg.bucket_count = nil
    cfg.rebalancer_disbalance_threshold = nil
    cfg.rebalancer_max_receiving = nil
end

return {
    check = cfg_check,
    prepare_for_box_cfg = prepare_for_box_cfg,
}
