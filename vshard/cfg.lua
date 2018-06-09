-- vshard.cfg

local log = require('log')
local luri = require('uri')
local consts = require('vshard.consts')

local function check_uri(uri)
    if not luri.parse(uri) then
        error('Invalid URI: ' .. uri)
    end
end

local function check_master(master, ctx)
    if master then
        if ctx.master then
            error('Only one master is allowed per replicaset')
        else
            ctx.master = master
        end
    end
end

local type_validate = {
    ['string'] = function(v) return type(v) == 'string' end,
    ['non-empty string'] = function(v)
        return type(v) == 'string' and #v > 0
    end,
    ['boolean'] = function(v) return type(v) == 'boolean' end,
    ['number'] = function(v) return type(v) == 'number' end,
    ['non-negative number'] = function(v)
        return type(v) == 'number' and v >= 0
    end,
    ['positive number'] = function(v)
        return type(v) == 'number' and v > 0
    end,
    ['non-negative integer'] = function(v)
        return type(v) == 'number' and v >= 0 and math.floor(v) == v
    end,
    ['positive integer'] = function(v)
        return type(v) == 'number' and v > 0 and math.floor(v) == v
    end,
    ['table'] = function(v) return type(v) == 'table' end,
}

local function validate_config(config, template, check_arg)
    for _, key_template in pairs(template) do
        local key = key_template[1]
        local template_value = key_template[2]
        local value = config[key]
        if not value then
            if not template_value.is_optional then
                error(string.format('%s must be specified',
                                    template_value.name))
            else
                config[key] = template_value.default
            end
        else
            if type(template_value.type) == 'string' then
                if not type_validate[template_value.type](value) then
                    error(string.format('%s must be %s', template_value.name,
                                        template_value.type))
                end
            else
                local is_valid_type_found = false
                for _, t in pairs(template_value.type) do
                    if type_validate[t](value) then
                        is_valid_type_found = true
                        break
                    end
                end
                if not is_valid_type_found then
                    local types = table.concat(template_value.type, ', ')
                    error(string.format('%s must be one of the following '..
                                        'types: %s', template_value.name,
                                        types))
                end
            end
            if template_value.check then
                template_value.check(value, check_arg)
            end
        end
    end
end

local replica_template = {
    {'uri', {type = 'non-empty string', name = 'URI', check = check_uri}},
    {'name', {type = 'string', name = "Name", is_optional = true}},
    {'zone', {type = {'string', 'number'}, name = "Zone", is_optional = true}},
    {'master', {
        type = 'boolean', name = "Master", is_optional = true, default = false,
        check = check_master
    }},
}

local function check_replicas(replicas)
    local ctx = {master = false}
    for _, replica in pairs(replicas) do
        validate_config(replica, replica_template, ctx)
    end
end

local replicaset_template = {
    {'replicas', {type = 'table', name = 'Replicas', check = check_replicas}},
    {'weight', {
        type = 'non-negative number', name = 'Weight', is_optional = true,
        default = 1,
    }},
    {'lock', {type = 'boolean', name = 'Lock', is_optional = true}},
}

--
-- Check weights map on correctness.
--
local function cfg_check_weights(weights)
    for zone1, v in pairs(weights) do
        if type(zone1) ~= 'number' and type(zone1) ~= 'string' then
            -- Zone1 can be not number or string, if an user made
            -- this: weights = {[{1}] = ...}. In such a case
            -- {1} is the unaccassible key of a lua table, which
            -- is available only via pairs.
            error('Zone identifier must be either string or number')
        end
        if type(v) ~= 'table' then
            error('Zone must be map of relative weights of other zones')
        end
        for zone2, weight in pairs(v) do
            if type(zone2) ~= 'number' and type(zone2) ~= 'string' then
                error('Zone identifier must be either string or number')
            end
            if type(weight) ~= 'number' or weight < 0 then
                error('Zone weight must be either nil or non-negative number')
            end
            if zone2 == zone1 and weight ~= 0 then
                error('Weight of own zone must be either nil or 0')
            end
        end
    end
end

local function check_sharding(sharding)
    local uuids = {}
    local uris = {}
    local names = {}
    for replicaset_uuid, replicaset in pairs(sharding) do
        if uuids[replicaset_uuid] then
            error(string.format('Duplicate uuid %s', replicaset_uuid))
        end
        uuids[replicaset_uuid] = true
        if type(replicaset) ~= 'table' then
            error('Replicaset must be a table')
        end
        validate_config(replicaset, replicaset_template)
        for replica_uuid, replica in pairs(replicaset.replicas) do
            if uris[replica.uri] then
                error(string.format('Duplicate uri %s', replica.uri))
            end
            uris[replica.uri] = true
            if uuids[replica_uuid] then
                error(string.format('Duplicate uuid %s', replica_uuid))
            end
            uuids[replica_uuid] = true
            -- Log warning in case replica.name duplicate is
            -- found. Message appears once for each unique
            -- duplicate.
            local name = replica.name
            if name then
                if names[name] == nil then
                    names[name] = 1
                elseif names[name] == 1 then
                    log.warn('Duplicate replica.name is found: %s', name)
                    -- Next duplicates should not be reported.
                    names[name] = 2
                end
            end
        end
    end
end

local cfg_template = {
    {'sharding', {type = 'table', name = 'Sharding', check = check_sharding}},
    {'weights', {
        type = 'table', name = 'Weight matrix', is_optional = true,
        check = cfg_check_weights
    }},
    {'shard_index', {
        type = {'non-empty string', 'non-negative integer'},
        name = 'Shard index', is_optional = true, default = 'bucket_id',
    }},
    {'zone', {
        type = {'string', 'number'}, name = 'Zone identifier',
        is_optional = true
    }},
    {'bucket_count', {
        type = 'positive integer', name = 'Bucket count', is_optional = true,
        default = consts.DEFAULT_BUCKET_COUNT
    }},
    {'rebalancer_disbalance_threshold', {
        type = 'non-negative number', name = 'Rebalancer disbalance threshold',
        is_optional = true,
        default = consts.DEFAULT_REBALANCER_DISBALANCE_THRESHOLD
    }},
    {'rebalancer_max_receiving', {
        type = 'positive integer',
        name = 'Rebalancer max receiving bucket count', is_optional = true,
        default = consts.DEFAULT_REBALANCER_MAX_RECEIVING
    }},
    {'collect_bucket_garbage_interval', {
        type = 'positive number', name = 'Garbage bucket collect interval',
        is_optional = true,
        default = consts.DEFAULT_COLLECT_BUCKET_GARBAGE_INTERVAL
    }},
    {'collect_lua_garbage', {
        type = 'boolean', name = 'Garbage Lua collect necessity',
        is_optional = true, default = false
    }},
    {'sync_timeout', {
        type = 'non-negative number', name = 'Sync timeout', is_optional = true,
        default = consts.DEFAULT_SYNC_TIMEOUT
    }},
    {'connection_outdate_delay', {
        type = 'non-negative number', name = 'Object outdate timeout',
        is_optional = true
    }},
}

--
-- Names of options which cannot be changed during reconfigure.
--
local non_dynamic_options = {
    'bucket_count', 'shard_index'
}

--
-- Check sharding config on correctness. Check types, name and uri
-- uniqueness, master count (in each replicaset must be <= 1).
--
local function cfg_check(shard_cfg, old_cfg)
    if type(shard_cfg) ~= 'table' then
        error('Сonfig must be map of options')
    end
    shard_cfg = table.deepcopy(shard_cfg)
    validate_config(shard_cfg, cfg_template)
    if not old_cfg then
        return shard_cfg
    end
    -- Check non-dynamic after default values are added.
    for _, f_name in pairs(non_dynamic_options) do
        -- New option may be added in new vshard version.
        if shard_cfg[f_name] ~= old_cfg[f_name] then
           error(string.format('Non-dynamic option %s ' ..
                               'cannot be reconfigured', f_name))
        end
    end
    return shard_cfg
end

--
-- Nullify non-box options.
--
local function remove_non_box_options(cfg)
    cfg.sharding = nil
    cfg.weights = nil
    cfg.zone = nil
    cfg.bucket_count = nil
    cfg.rebalancer_disbalance_threshold = nil
    cfg.rebalancer_max_receiving = nil
    cfg.shard_index = nil
    cfg.collect_bucket_garbage_interval = nil
    cfg.collect_lua_garbage = nil
    cfg.sync_timeout = nil
    cfg.connection_outdate_delay = nil
end

return {
    check = cfg_check,
    remove_non_box_options = remove_non_box_options,
}
