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
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
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

test_group.test_simultaneous_cfg = function(g)
    g.replica_1_a:exec(function(cfg)
        ivshard.storage.internal.errinj.ERRINJ_CFG_DELAY = true
        rawset(_G, 'fiber_cfg', ifiber.new(ivshard.storage.cfg, cfg, _G.get_uuid()))
        _G.fiber_cfg:set_joinable(true)
    end, {global_cfg})

    local function storage_cfg()
        return g.replica_1_a:exec(function(cfg)
            local _, err = pcall(ivshard.storage.cfg, cfg, _G.get_uuid())
            return err
        end, {global_cfg})
    end

    local err = storage_cfg()
    t.assert_str_contains(err.message, 'storage is in progress')

    g.replica_1_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_CFG_DELAY = false
        _G.fiber_cfg:join()
    end)

    err = storage_cfg()
    t.assert_equals(err, nil)
end

--
-- gh-400: _truncate deletions should be let through.
--
test_group.test_truncate_space_clear = function(g)
    g.replica_1_a:exec(function()
       local s = box.schema.create_space('test')
       s:create_index('pk')
       s:replace{1}
       s:truncate()
       local sid = s.id
       ilt.assert(box.space._truncate:get{sid} ~= nil)
       s:drop()
       ilt.assert(box.space._truncate:get{sid} == nil)
    end)
end

test_group.test_recovery_bucket_stat = function(g)
    g.replica_1_a:exec(function(bucket_count)
        local stat, err = ivshard.storage._call('recovery_bucket_stat',
                                                bucket_count + 1)
        ilt.assert_equals(stat, nil)
        ilt.assert_equals(err, nil)

        local bid = _G.get_first_bucket()
        stat, err = ivshard.storage._call('recovery_bucket_stat', bid)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(stat.id, bid)
        ilt.assert_equals(stat.status, ivconst.BUCKET.ACTIVE)
        ilt.assert(not stat.is_transfering)
    end, {cfg_template.bucket_count})

    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].replicas.replica_1_a.master = false
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    g.replica_1_a:exec(function()
        local stat, err = ivshard.storage._call('recovery_bucket_stat',
                                                _G.get_first_bucket())
        ilt.assert_equals(stat, nil)
        ilt.assert_equals(err.code, iverror.code.NON_MASTER)
    end)

    vtest.cluster_cfg(g, global_cfg)
end
