local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{engine = 'memtx'}, {engine = 'vinyl'}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {
        engine = 'memtx', memtx_use_mvcc_engine = true
    })
    table.insert(group_config, {
        engine = 'vinyl', memtx_use_mvcc_engine = true
    })
end

local test_group = t.group('storage', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 10
}
local cluster_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    cluster_cfg = vtest.config_new(cfg_template)

    vtest.storage_new(g, cluster_cfg)
    vtest.storage_bootstrap(g, cluster_cfg)
    vtest.storage_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_sharded_spaces = function(g)
    g.replica_1_a:exec(function(engine)
        ilt.assert_not_equals(engine, nil)
        local vinternal = ivshard.storage.internal
        --
        -- gh-96: public API to see all sharded spaces.
        --
        local function get_sharded_names()
            local result = {}
            for _, space in pairs(ivshard.storage.sharded_spaces()) do
                table.insert(result, space.name)
            end
            return result
        end
        local s1 = box.schema.create_space('test1', {engine = engine})
        s1:create_index('pk')
        -- No bucket_id index.
        ilt.assert_items_equals(get_sharded_names(), {})
        -- Wrong field type.
        local s1k = s1:create_index('bucket_id', {parts = {{2, 'string'}}})
        ilt.assert_items_equals(get_sharded_names(), {})
        s1k:drop()
        -- All fine, 2 fields and non-unique allowed.
        s1k = s1:create_index('bucket_id', {
            parts = {{1, 'unsigned'}, {2, 'unsigned'}},
            unique = false,
        })
        ilt.assert_items_equals(get_sharded_names(), {s1.name})
        --
        -- gh-74: allow to choose any name for shard indexes.
        --
        s1k:rename('vbuckets')
        vinternal.shard_index = 'vbuckets'
        ilt.assert_items_equals(get_sharded_names(), {s1.name})
        s1k:rename('bucket_id_tmp')
        vinternal.shard_index = 'bucket_id'
        ilt.assert_items_equals(get_sharded_names(), {})
        s1k:rename('bucket_id')

        local s2 = box.schema.create_space('test2', {engine = engine})
        s2:create_index('pk')
        s2:create_index('bucket_id', {parts = {{2, 'unsigned'}}})
        ilt.assert_items_equals(get_sharded_names(), {s1.name, s2.name})
        s1:drop()
        s2:drop()
        --
        -- gh-111: cache sharded spaces based on schema version
        --
        local cached_spaces = vinternal.cached_find_sharded_spaces()
        ilt.assert_is(cached_spaces, vinternal.cached_find_sharded_spaces())
        s1 = box.schema.create_space('test', {engine = engine})
        ilt.assert_is_not(cached_spaces, vinternal.cached_find_sharded_spaces())
        s1:drop()
    end, {g.params.engine})
end
