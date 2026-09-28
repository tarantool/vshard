local fiber = require('fiber')
local msgpack = require('msgpack')
local router = require('vshard.router')
local storage = require('vshard.storage')

local M = {
    config = nil,
}

local DEFAULT_TIMEOUT = 1
local DEFAULT_MAX_LOCAL_K = 4096
local PROTOCOL_VERSION = 1

local function fail(message)
    error('vshard.vector: ' .. message, 0)
end

local function is_positive_integer(value)
    return type(value) == 'number' and value > 0 and value % 1 == 0
end

local function is_non_negative_integer(value)
    return type(value) == 'number' and value >= 0 and value % 1 == 0
end

local function is_field_reference(value)
    return type(value) == 'string' or is_positive_integer(value)
end

local function is_finite_number(value)
    return type(value) == 'number' and value == value and
           value ~= math.huge and value ~= -math.huge
end

local function collection_by_name(name)
    if M.config == nil then
        fail('call cfg() before using the module')
    end
    local collection = M.config.collections[name]
    if collection == nil then
        fail(('unknown collection %q'):format(name))
    end
    return collection
end

local function validate_collection(name, collection)
    if type(name) ~= 'string' or name == '' then
        fail('collection name must be a non-empty string')
    end
    if type(collection) ~= 'table' then
        fail(('collection %q must be a table'):format(name))
    end
    if not is_positive_integer(collection.collection_id) then
        fail(('collection %q has invalid collection_id'):format(name))
    end
    if not is_positive_integer(collection.generation) then
        fail(('collection %q has invalid generation'):format(name))
    end
    for _, field in ipairs({'space', 'index', 'primary_index', 'bucket_field',
                            'version_field', 'dimension', 'distance'}) do
        if collection[field] == nil then
            fail(('collection %q misses %s'):format(name, field))
        end
    end
    if type(collection.space) ~= 'string' or
       type(collection.index) ~= 'string' or
       type(collection.primary_index) ~= 'string' then
        fail(('collection %q has invalid space or index name'):format(name))
    end
    if not is_field_reference(collection.bucket_field) then
        fail(('collection %q has invalid bucket_field'):format(name))
    end
    if not is_field_reference(collection.version_field) then
        fail(('collection %q has invalid version_field'):format(name))
    end
    if not is_positive_integer(collection.dimension) then
        fail(('collection %q has invalid dimension'):format(name))
    end
    if collection.distance ~= 'l2' and collection.distance ~= 'cosine' and
       collection.distance ~= 'ip' then
        fail(('collection %q has invalid distance'):format(name))
    end
    if collection.max_local_k ~= nil and
       not is_positive_integer(collection.max_local_k) then
        fail(('collection %q has invalid max_local_k'):format(name))
    end
end

local function validate_config(config)
    if type(config) ~= 'table' or type(config.collections) ~= 'table' then
        fail('cfg() expects a table with collections')
    end
    local collection_ids = {}
    for name, collection in pairs(config.collections) do
        validate_collection(name, collection)
        if collection_ids[collection.collection_id] then
            fail(('collection_id %d is configured more than once'):format(
                collection.collection_id))
        end
        collection_ids[collection.collection_id] = true
    end
end

local function validate_query(query, collection)
    if type(query) ~= 'table' or #query ~= collection.dimension then
        fail(('query must contain %d coordinates'):format(collection.dimension))
    end
    for i = 1, collection.dimension do
        if not is_finite_number(query[i]) then
            fail(('query coordinate %d is not a finite number'):format(i))
        end
    end
end

local function validate_options(opts, collection)
    opts = opts or {}
    if type(opts) ~= 'table' then
        fail('search options must be a table')
    end
    if not is_positive_integer(opts.k) then
        fail('options.k must be a positive integer')
    end
    local local_k = opts.local_k or opts.k
    local max_local_k = collection.max_local_k or DEFAULT_MAX_LOCAL_K
    if not is_positive_integer(local_k) or local_k < opts.k or
       local_k > max_local_k then
        fail(('options.local_k must be in [%d, %d]'):format(
            opts.k, max_local_k))
    end
    local timeout = opts.timeout or DEFAULT_TIMEOUT
    if not is_finite_number(timeout) or timeout <= 0 then
        fail('options.timeout must be a positive finite number')
    end
    if opts.ef_search ~= nil and
       not is_non_negative_integer(opts.ef_search) then
        fail('options.ef_search must be a non-negative integer')
    end
    if opts.consistency ~= nil and opts.consistency ~= 'strict' then
        fail('only strict consistency is supported')
    end
    if opts.return_fields ~= nil and type(opts.return_fields) ~= 'table' then
        fail('options.return_fields must be a table')
    end
    if opts.return_fields ~= nil then
        for _, field in ipairs(opts.return_fields) do
            if not is_field_reference(field) then
                fail('options.return_fields contains an invalid field')
            end
        end
    end
    return {
        k = opts.k,
        local_k = local_k,
        timeout = timeout,
        ef_search = opts.ef_search,
        return_fields = opts.return_fields,
    }
end

local function tuple_key(tuple, primary_index)
    local key = {}
    for i, part in ipairs(primary_index.parts) do
        local fieldno = part.fieldno or part.field
        key[i] = tuple[fieldno]
    end
    if #key == 1 then
        return key[1]
    end
    return key
end

local function fieldno(space, field)
    if type(field) == 'number' then
        return field
    end
    for i, format in ipairs(space:format()) do
        if format.name == field then
            return i
        end
    end
    fail(('space %q has no field %q'):format(space.name, field))
end

local function tuple_projection(tuple, fields)
    if fields == nil then
        return nil
    end
    local projection = {}
    for i, field in ipairs(fields) do
        projection[i] = tuple[field]
    end
    return projection
end

local function local_collection(name, generation)
    local collection = collection_by_name(name)
    if collection.generation ~= generation then
        fail(('collection %q has generation %d, expected %d'):format(
            name, collection.generation, generation))
    end
    return collection
end

local function local_index(collection)
    local space = box.space[collection.space]
    if space == nil then
        fail(('space %q does not exist'):format(collection.space))
    end
    local index = space.index[collection.index]
    if index == nil then
        fail(('index %q does not exist'):format(collection.index))
    end
    local primary_index = space.index[collection.primary_index]
    if primary_index == nil then
        fail(('primary index %q does not exist'):format(
            collection.primary_index))
    end
    local index_opts = index.opts
    if index_opts ~= nil and (index_opts.dimension ~= collection.dimension or
        index_opts.distance ~= collection.distance) then
        fail('local VECTOR index definition differs from collection config')
    end
    return space, index, primary_index
end

local function local_replicaset_id()
    local internal = storage.internal
    local replicaset = internal and internal.this_replicaset
    if replicaset == nil or replicaset.id == nil then
        fail('vshard.storage must be configured before local search')
    end
    return replicaset.id
end

local function storage_search(name, generation, query, opts)
    local collection = local_collection(name, generation)
    validate_query(query, collection)
    opts = validate_options(opts, collection)
    local space, index, primary_index = local_index(collection)
    local bucket_fieldno = fieldno(space, collection.bucket_field)
    local version_fieldno = fieldno(space, collection.version_field)
    local projection_fields
    if opts.return_fields ~= nil then
        projection_fields = {}
        for i, field in ipairs(opts.return_fields) do
            projection_fields[i] = fieldno(space, field)
        end
    end
    local search_opts = {k = opts.local_k}
    if opts.ef_search ~= nil then
        search_opts.ef_search = opts.ef_search
    end
    local result = index:search(query, search_opts)
    local candidates = {}
    for i, row in ipairs(result) do
        local tuple = row.tuple
        if not is_finite_number(row.distance) then
            fail('local VECTOR index returned a non-finite distance')
        end
        candidates[i] = {
            key = tuple_key(tuple, primary_index),
            bucket_id = tuple[bucket_fieldno],
            vector_version = tuple[version_fieldno],
            distance = row.distance,
            tuple = tuple_projection(tuple, projection_fields),
        }
    end
    return {
        protocol_version = PROTOCOL_VERSION,
        collection_id = collection.collection_id,
        generation = collection.generation,
        replicaset_id = local_replicaset_id(),
        metric = collection.distance,
        dimension = collection.dimension,
        candidates = candidates,
    }
end

local function candidate_identity(candidate)
    return msgpack.encode({key = candidate.key,
                           vector_version = candidate.vector_version})
end

local function candidate_order(candidate, replicaset_id)
    return msgpack.encode({key = candidate.key, replicaset_id = replicaset_id})
end

local function candidate_less(left, right)
    if left.distance ~= right.distance then
        return left.distance < right.distance
    end
    if left.order ~= right.order then
        return left.order < right.order
    end
    return left.replicaset_id < right.replicaset_id
end

local function merge(map, collection, opts)
    local candidates = {}
    local seen = {}
    local shard_count = 0
    for replicaset_id, value in pairs(map) do
        shard_count = shard_count + 1
        local response = value[1]
        if type(response) ~= 'table' then
            return nil, ('replicaset %s returned an invalid response'):format(
                replicaset_id)
        end
        if response.protocol_version ~= PROTOCOL_VERSION or
           response.collection_id ~= collection.collection_id or
           response.generation ~= collection.generation or
           response.metric ~= collection.distance or
           response.dimension ~= collection.dimension or
           response.replicaset_id ~= replicaset_id or
           type(response.candidates) ~= 'table' then
            return nil, ('replicaset %s returned incompatible metadata'):format(
                replicaset_id)
        end
        if #response.candidates > opts.local_k then
            return nil, ('replicaset %s returned too many candidates'):format(
                replicaset_id)
        end
        for _, candidate in ipairs(response.candidates) do
            if type(candidate) ~= 'table' or
               not is_finite_number(candidate.distance) then
                return nil,
                       ('replicaset %s returned an invalid candidate'):format(
                           replicaset_id)
            end
            local identity = candidate_identity(candidate)
            if seen[identity] then
                return nil,
                       ('duplicate candidate returned by replicaset %s'):format(
                           replicaset_id)
            end
            seen[identity] = true
            candidate.replicaset_id = replicaset_id
            candidate.order = candidate_order(candidate, replicaset_id)
            table.insert(candidates, candidate)
        end
    end
    table.sort(candidates, candidate_less)
    local result = {}
    for i = 1, math.min(opts.k, #candidates) do
        local candidate = candidates[i]
        candidate.order = nil
        result[i] = candidate
    end
    return result, {
        complete = true,
        collection = collection.name,
        generation = collection.generation,
        shards_total = shard_count,
        shards_ok = shard_count,
        candidates = #candidates,
        deduplicated = 0,
        local_k = opts.local_k,
    }
end

local function search(name, query, opts)
    local collection = collection_by_name(name)
    validate_query(query, collection)
    opts = validate_options(opts, collection)
    local started = fiber.clock()
    local map, err, replicaset_id = router.map_callrw(
        'vshard.vector.storage_search',
        {name, collection.generation, query, opts},
        {timeout = opts.timeout})
    if map == nil then
        return nil, {
            message = tostring(err),
            stage = 'scatter',
            replicaset = replicaset_id,
        }
    end
    local result, meta = merge(map, collection, opts)
    if result == nil then
        return nil, {
            message = meta,
            stage = 'merge',
        }
    end
    meta.elapsed = fiber.clock() - started
    return result, meta
end

function M.cfg(config)
    validate_config(config)
    local copied = table.deepcopy(config)
    for name, collection in pairs(copied.collections) do
        collection.name = name
        collection.max_local_k = collection.max_local_k or DEFAULT_MAX_LOCAL_K
    end
    M.config = copied
end

M.search = search
M.storage_search = storage_search
M.internal = {
    merge = merge,
}

local global_vshard = rawget(_G, 'vshard')
if global_vshard == nil then
    global_vshard = {}
    rawset(_G, 'vshard', global_vshard)
end
global_vshard.vector = M

return M
