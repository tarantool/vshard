local t = require('luatest')
local vector = require('vshard.vector')

local g = t.group('vector')

local function cfg()
    vector.cfg({
        collections = {
            products = {
                collection_id = 1,
                generation = 1,
                space = 'products',
                index = 'vector',
                primary_index = 'primary',
                bucket_field = 'bucket_id',
                version_field = 'version',
                dimension = 2,
                distance = 'l2',
            },
        },
    })
end

local function response(replicaset_id, candidates)
    return {
        protocol_version = 1,
        collection_id = 1,
        generation = 1,
        replicaset_id = replicaset_id,
        metric = 'l2',
        dimension = 2,
        candidates = candidates,
    }
end

g.test_merge = function()
    cfg()
    local collection = vector.config.collections.products
    local result, meta = vector.internal.merge({
        ['rs-1'] = {response('rs-1', {
            {key = 2, bucket_id = 1, vector_version = 1, distance = 2},
            {key = 3, bucket_id = 2, vector_version = 1, distance = 3},
        })},
        ['rs-2'] = {response('rs-2', {
            {key = 1, bucket_id = 3, vector_version = 1, distance = 1},
            {key = 4, bucket_id = 4, vector_version = 1, distance = 4},
        })},
    }, collection, {k = 3, local_k = 3})
    t.assert_equals(meta.candidates, 4)
    t.assert_equals({result[1].key, result[2].key, result[3].key}, {1, 2, 3})
end

g.test_merge_rejects_duplicate_identity = function()
    cfg()
    local collection = vector.config.collections.products
    local result, err = vector.internal.merge({
        ['rs-1'] = {response('rs-1', {
            {key = 1, bucket_id = 1, vector_version = 1, distance = 1},
        })},
        ['rs-2'] = {response('rs-2', {
            {key = 1, bucket_id = 2, vector_version = 1, distance = 1},
        })},
    }, collection, {k = 1, local_k = 1})
    t.assert_equals(result, nil)
    t.assert_str_contains(err, 'duplicate candidate')
end

g.test_cfg_rejects_invalid_dimension = function()
    t.assert_error_msg_contains('invalid dimension', function()
        vector.cfg({
            collections = {
                products = {
                    collection_id = 1,
                    generation = 1,
                    space = 'products',
                    index = 'vector',
                    primary_index = 'primary',
                    bucket_field = 'bucket_id',
                    version_field = 'version',
                    dimension = 0,
                    distance = 'l2',
                },
            },
        })
    end)
end
