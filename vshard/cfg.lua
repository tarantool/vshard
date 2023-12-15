-- vshard.cfg

local log = require('log')
local luri = require('uri')
local lutil = require('vshard.util')
local consts = require('vshard.consts')

local function check_uri_connect(uri)
    if not lutil.feature.multilisten then
        if type(uri) == 'number' then
            uri = tostring(uri)
        elseif type(uri) ~= 'string' then
            error('Only number and string URI are supported at this version')
        end
    end
    if not luri.parse(uri) then
        error('Invalid URI')
    end
end

local function check_uri_listen(uri)
    if not lutil.feature.multilisten then
        error('Listen URIs are not supported at this version')
    end
    if not luri.parse_many(uri) then
       error('URI must be a non-empty string, or a number, or a table, or an '..
             'array of these types')
    end
end

local function check_replica_master(master, ctx)
    if master then
        if ctx.master then
            error('Only one master is allowed per replicaset')
        else
            ctx.master = master
        end
    end
end

local function check_uuid(uuid, ctx)
    if uuid and not ctx.is_named then
        error('uuid option can be specified only when ' ..
              'identification_mode = "name_as_key"')
    end
end

local function check_replica_name(name, ctx)
    if name and ctx.is_named then
        error('replica.name can be specified only when ' ..
              'identification_mode = "uuid_as_key"')
    end
end

local function is_number(v)
    return type(v) == 'number' and v == v
end

local function is_non_negative_number(v)
    return is_number(v) and v >= 0
end

local function is_positive_number(v)
    return is_number(v) and v > 0
end

local function is_non_negative_integer(v)
    return is_non_negative_number(v) and math.floor(v) == v
end

local function is_positive_integer(v)
    return is_positive_number(v) and math.floor(v) == v
end

local function type_tostring_trivial(self)
    return self.name
end

local type_descriptors = {
    -- check(self, template, value)
    -- tostring(self, template)

    ['string'] = {
        check = function(self, _, v) return type(v) == 'string' end,
        tostring = type_tostring_trivial,
    },
    ['non-empty string'] = {
        check = function(self, _, v) return type(v) == 'string' and #v > 0 end,
        tostring = type_tostring_trivial,
    },
    ['boolean'] = {
        check = function(self, _, v) return type(v) == 'boolean' end,
        tostring = type_tostring_trivial,
    },
    ['number'] = {
        check = function(self, _, v) return is_number(v) end,
        tostring = type_tostring_trivial,
    },
    ['non-negative number'] = {
        check = function(self, _, v) return is_non_negative_number(v) end,
        tostring = type_tostring_trivial,
    },
    ['positive number'] = {
        check = function(self, _, v) return is_positive_number(v) end,
        tostring = type_tostring_trivial,
    },
    ['non-negative integer'] = {
        check = function(self, _, v) return is_non_negative_integer(v) end,
        tostring = type_tostring_trivial,
    },
    ['positive integer'] = {
        check = function(self, _, v) return is_positive_integer(v) end,
        tostring = type_tostring_trivial,
    },
    ['table'] = {
        check = function(self, _, v) return type(v) == 'table' end,
        tostring = type_tostring_trivial,
    },
    ['enum'] = {
        check = function(self, tv, v)
            for _, vi in pairs(tv.enum) do
                if vi == v then
                    return true
                end
            end
            return false
        end,
        tostring = function(self, tv)
            local values = {}
            for _, v in pairs(tv.enum) do
                assert(type(v) == 'string')
                table.insert(values, ("'%s'"):format(v))
            end
            if tv.is_optional then
                table.insert(values, 'nil')
            end
            return ('enum {%s}'):format(table.concat(values, ', '))
        end,
    },
}
for name, td in pairs(type_descriptors) do
    td.name = name
end

local function validate_config(config, template, check_arg)
    for key, tv in pairs(template) do
        local value = config[key]
        local name = tv.name
        local expected_type = tv.type
        if tv.is_deprecated then
            if value ~= nil then
                local reason = tv.reason
                if reason then
                    reason = '. '..reason
                else
                    reason = ''
                end
                log.warn('Option "%s" is deprecated'..reason, name)
            end
        elseif value == nil then
            if not tv.is_optional then
                error(string.format('%s must be specified', name))
            else
                config[key] = tv.default
            end
        else
            if type(expected_type) == 'string' then
                local td = type_descriptors[expected_type]
                if not td:check(tv, value) then
                    error(string.format('%s must be %s', name, td:tostring(tv)))
                end
                local max = tv.max
                if max and value > max then
                    error(string.format('%s must not be greater than %s', name,
                                        max))
                end
            else
                local is_valid_type_found = false
                for _, t in pairs(expected_type) do
                    if type_descriptors[t]:check(tv, value) then
                        is_valid_type_found = true
                        break
                    end
                end
                if not is_valid_type_found then
                    local types = {}
                    for _, t in pairs(expected_type) do
                        table.insert(types, type_descriptors[t]:tostring(tv))
                    end
                    types = table.concat(types, ', ')
                    error(string.format('%s must be one of the following '..
                                        'types: %s', name, types))
                end
            end
            if tv.check then
                tv.check(value, check_arg)
            end
        end
    end
end

local replica_template = {
    uri = {
        type = {'non-empty string', 'number', 'table'},
        name = 'URI',
        check = check_uri_connect
    },
    listen = {
        type = {'non-empty string', 'number', 'table'},
        name = 'listen',
        check = check_uri_listen,
        is_optional = true,
        is_overriding_box = true,
    },
    name = {
        type = 'string',
        name = "Name",
        check = check_replica_name,
        is_optional = true,
    },
    zone = {type = {'string', 'number'}, name = "Zone", is_optional = true},
    master = {
        type = 'boolean', name = "Master", is_optional = true,
        check = check_replica_master
    },
    rebalancer = {type = 'boolean', name = 'Rebalancer flag', is_optional = true},
    uuid = {
        type = {'non-empty string'},
        check = check_uuid,
        name = 'UUID',
        is_optional = true,
    },
}

local function check_replicas(replicas, ctx)
    ctx.master = false
    for _, replica in pairs(replicas) do
        validate_config(replica, replica_template, ctx)
    end
end

local replicaset_template = {
    replicas = {type = 'table', name = 'Replicas', check = check_replicas},
    weight = {
        type = 'non-negative number', name = 'Weight', is_optional = true,
        default = 1,
    },
    lock = {type = 'boolean', name = 'Lock', is_optional = true},
    master = {
        type = 'enum', name = 'Master search mode', is_optional = true,
        enum = {'auto'},
    },
    rebalancer = {type = 'boolean', name = 'Rebalancer flag', is_optional = true},
    uuid = {
        type = {'non-empty string'},
        check = check_uuid,
        name = 'UUID',
        is_optional = true,
    },
}

--
-- Check weights map on correctness.
--
local function cfg_check_weights(weights)
    for zone1, v in pairs(weights) do
        if type(zone1) ~= 'number' and type(zone1) ~= 'string' then
            -- Zone1 can be not number or string, if an user made
            -- this: weights = {[{1}] = ...}. In such a case
            -- {1} is the unaccessible key of a lua table, which
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

local function check_sharding(sharding, ctx)
    local ids = {}
    local uris = {}
    local names = {}
    local is_all_weights_zero = true
    local rebalancer_id
    local rs_ids, replica_ids
    if ctx.is_named then
        -- It's allowed to have the same names for replicaset and replica.
        ids.replicasets = {}
        ids.replicas = {}
        rs_ids = ids.replicasets
        replica_ids = ids.replicas
    else
        rs_ids = ids
        replica_ids = ids
    end
    for replicaset_id, replicaset in pairs(sharding) do
        if rs_ids[replicaset_id] then
            error(string.format('Duplicate id %s', replicaset_id))
        end
        rs_ids[replicaset_id] = true
        if type(replicaset) ~= 'table' then
            error('Replicaset must be a table')
        end
        local w = replicaset.weight
        if w == math.huge or w == -math.huge then
            error('Replicaset weight can not be Inf')
        end
        if replicaset.rebalancer then
            if rebalancer_id then
                error(('Found 2 rebalancer flags at %s and %s'):format(
                      rebalancer_id, replicaset_id))
            end
            rebalancer_id = replicaset_id
        end
        validate_config(replicaset, replicaset_template, ctx)
        local no_rebalancer = replicaset.rebalancer == false
        local is_master_auto = replicaset.master == 'auto'
        for replica_id, replica in pairs(replicaset.replicas) do
            if uris[replica.uri] then
                error(string.format('Duplicate uri %s', replica.uri))
            end
            uris[replica.uri] = true
            if replica_ids[replica_id] then
                error(string.format('Duplicate id %s', replica_id))
            end
            replica_ids[replica_id] = true
            if is_master_auto and replica.master ~= nil then
                error(string.format('Can not specify master nodes when '..
                                    'master search is enabled, but found '..
                                    'master flag in replica id %s',
                                    replica_id))
            end
            if replica.rebalancer then
                if rebalancer_id then
                    error(('Found 2 rebalancer flags at %s and %s'):format(
                          rebalancer_id, replica_id))
                end
                if no_rebalancer then
                    error(('Replicaset %s can\'t run the rebalancer, and yet '..
                           'it was explicitly assigned to its instance '..
                           '%s'):format(replicaset_id, replica_id))
                end
                rebalancer_id = replica_id
            end
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
        is_all_weights_zero = is_all_weights_zero and replicaset.weight == 0
    end
    if next(sharding) and is_all_weights_zero then
        error('At least one replicaset weight should be > 0')
    end
end

local function check_identification_mode(mode)
    if mode == 'name_as_key' and not lutil.feature.persistent_names then
        error('Name identification is supported only for Tarantool >= 3.0.0')
    end
end

local cfg_template = {
    sharding = {type = 'table', name = 'Sharding', check = check_sharding},
    weights = {
        type = 'table', name = 'Weight matrix', is_optional = true,
        check = cfg_check_weights
    },
    shard_index = {
        type = {'non-empty string', 'non-negative integer'},
        name = 'Shard index', is_optional = true, default = 'bucket_id',
    },
    zone = {
        type = {'string', 'number'}, name = 'Zone identifier',
        is_optional = true
    },
    bucket_count = {
        type = 'positive integer', name = 'Bucket count', is_optional = true,
        default = consts.DEFAULT_BUCKET_COUNT
    },
    rebalancer_disbalance_threshold = {
        type = 'non-negative number', name = 'Rebalancer disbalance threshold',
        is_optional = true,
        default = consts.DEFAULT_REBALANCER_DISBALANCE_THRESHOLD
    },
    rebalancer_max_receiving = {
        type = 'positive integer',
        name = 'Rebalancer max receiving bucket count', is_optional = true,
        default = consts.DEFAULT_REBALANCER_MAX_RECEIVING
    },
    rebalancer_max_sending = {
        type = 'positive integer',
        name = 'Rebalancer max sending bucket count',
        is_optional = true,
        default = consts.DEFAULT_REBALANCER_MAX_SENDING,
        max = consts.REBALANCER_MAX_SENDING_MAX
    },
    rebalancer_mode = {
        type = 'enum',
        name = 'Rebalancer mode',
        is_optional = true,
        default = 'auto',
        enum = {'auto', 'manual', 'off'},
    },
    collect_bucket_garbage_interval = {
        name = 'Garbage bucket collect interval', is_deprecated = true,
        reason = 'Has no effect anymore'
    },
    collect_lua_garbage = {
        name = 'Garbage Lua collect necessity', is_deprecated = true,
        reason = 'Has no effect anymore and never had much sense'
    },
    sync_timeout = {
        type = 'non-negative number', name = 'Sync timeout', is_optional = true,
        default = consts.DEFAULT_SYNC_TIMEOUT
    },
    connection_outdate_delay = {
        type = 'non-negative number', name = 'Object outdate timeout',
        is_optional = true
    },
    failover_ping_timeout = {
        type = 'positive number', name = 'Failover ping timeout',
        is_optional = true, default = consts.DEFAULT_FAILOVER_PING_TIMEOUT
    },
    discovery_mode = {
        type = 'enum', name = 'Discovery mode',
        is_optional = true, default = 'on', enum = {'on', 'off', 'once'},
    },
    sched_ref_quota = {
        name = 'Scheduler storage ref quota', type = 'non-negative number',
        is_optional = true, default = consts.DEFAULT_SCHED_REF_QUOTA
    },
    sched_move_quota = {
        name = 'Scheduler bucket move quota', type = 'non-negative number',
        is_optional = true, default = consts.DEFAULT_SCHED_MOVE_QUOTA
    },
    box_cfg_mode = {
        name = 'Box.cfg mode', type = 'enum', is_optional = true,
        default = 'auto', enum = {'auto', 'manual'},
    },
    schema_management_mode = {
        name = 'Schema management mode', type = 'enum',
        is_optional = true, default = 'auto', enum = {'auto', 'manual_access'},
    },
    identification_mode = {
        name = 'Config identification mode', type = 'enum',
        is_optional = true, default = 'uuid_as_key',
        enum = {'uuid_as_key', 'name_as_key'},
        check = check_identification_mode,
    },
}

--
-- Extract vshard own options from a merged config also having box options.
--
local function cfg_extract_vshard(root_cfg)
    local vshard_cfg = {}
    for k, v in pairs(root_cfg) do
        if cfg_template[k] then
            vshard_cfg[k] = v
        end
    end
    return vshard_cfg
end

--
-- Extract box options from a merged config also having vshard options. The
-- replica options values are in priority.
--
local function cfg_extract_box(root_cfg, replica_cfg)
    local box_cfg = {}
    for k, v in pairs(root_cfg) do
        if not cfg_template[k] then
            box_cfg[k] = v
        end
    end
    for k, v in pairs(replica_cfg) do
        local template = replica_template[k]
        if not template or template.is_overriding_box then
            box_cfg[k] = v
        end
    end
    return box_cfg
end

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
    local ctx = {is_named = shard_cfg.identification_mode == 'name_as_key'}
    validate_config(shard_cfg, cfg_template, ctx)
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
-- Extract uuid and name from the replicaset/replica config. If is_named = true,
-- then identification_mode is 'name_as_key', new type of config is used:
-- names becomes identifiers instead of UUIDs, replica/replicaset.name
-- is not specified, UUIDs are specified as replica/replicaset.uuid.
--
local function cfg_extract_identifiers(cfg_key, cfg_value, is_named)
    local uuid, name
    if is_named then
        uuid = cfg_value.uuid
        name = cfg_key
        assert(cfg_value.name == nil)
    else
        uuid = cfg_key
        name = cfg_value.name
        assert(cfg_value.uuid == nil)
    end
    return uuid, name
end

return {
    check = cfg_check,
    extract_vshard = cfg_extract_vshard,
    extract_box = cfg_extract_box,
    extract_identifiers = cfg_extract_identifiers,
}
