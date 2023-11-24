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
    bucket_count = 10,
    replication_timeout = 0.1,
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
        local vschema = require('vshard.storage.schema')
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
        vschema.shard_index = 'vbuckets'
        ilt.assert_items_equals(get_sharded_names(), {s1.name})
        s1k:rename('bucket_id_tmp')
        vschema.shard_index = 'bucket_id'
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
        local cached_spaces = vschema.find_sharded_spaces()
        ilt.assert_is(cached_spaces, vschema.find_sharded_spaces())
        s1 = box.schema.create_space('test', {engine = engine})
        ilt.assert_is_not(cached_spaces, vschema.find_sharded_spaces())
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

test_group.test_local_call = function(g)
    -- box.func was introduced in 2.2.1.
    t.run_only_if(vutil.version_is_at_least(2, 2, 1, nil, 0, 0))
    g.replica_1_a:exec(function()
        local body = [[function(a, b) return a + b end]]
        box.schema.func.create('sum', {body = body})
        local bid = _G.get_first_bucket()
        local ok, ret = ivshard.storage.call(bid, 'read', 'sum', {1, 2})
        ilt.assert_equals(ret, 3)
        ilt.assert_equals(ok, true)
        box.func.sum:drop()
    end)
end

--
-- gh-464: hot reload with named identification is broken.
--
test_group.test_named_hot_reload = function(g)
    t.run_only_if(vutil.feature.persistent_names)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.identification_mode = 'name_as_key'
    new_cfg_template.sharding['replicaset'] = new_cfg_template.sharding[1]
    new_cfg_template.sharding[1] = nil

    g.replica_1_a:exec(function()
        box.cfg{instance_name = 'replica_1_a', replicaset_name = 'replicaset'}
    end)
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    -- gh-464: hot reload with named identification is broken.
    g.replica_1_a:exec(function()
        package.loaded['vshard.storage'] = nil
        local ok, storage = pcall(require, 'vshard.storage')
        ilt.assert_equals(ok, true)
        _G.vshard.storage = storage
    end)
end

test_group.test_ref_with_lookup = function(g)
    g.replica_1_a:exec(function()
        local res, err
        local timeout = 0.1
        local rid = 42
        local bids = _G.get_n_buckets(2)
        local bid_extra = 3001

        -- Check for a single bucket.
        res, err = ivshard.storage._call(
            'storage_ref_with_lookup',
            rid,
            timeout,
            {bids[1]}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = rid, moved = {}})
        res, err = ivshard.storage._call('storage_unref', rid)
        ilt.assert_equals(err, nil)

        -- Check for multiple buckets.
        res, err = ivshard.storage._call(
            'storage_ref_with_lookup',
            rid,
            timeout,
            {bids[1], bids[2]}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = rid, moved = {}})
        res, err = ivshard.storage._call('storage_unref', rid)
        ilt.assert_equals(err, nil)

        -- Check for double referencing.
        res, err = ivshard.storage._call(
            'storage_ref_with_lookup',
            rid,
            timeout,
            {bids[1], bids[1]}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = rid, moved = {}})
        res, err = ivshard.storage._call('storage_unref', rid)
        ilt.assert_equals(err, nil)
        res, err = ivshard.storage._call('storage_unref', rid)
        t.assert_str_contains(err.message, 'Can not delete a storage ref: no ref')

        -- Check for an absent bucket.
        res, err = ivshard.storage._call(
            'storage_ref_with_lookup',
            rid,
            timeout,
            {bids[1], bids[2], bid_extra}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = rid, moved = {bid_extra}})
        ivshard.storage._call('storage_unref', rid)

        -- Check that we do not create a reference if there are no buckets.
        res, err = ivshard.storage._call(
            'storage_ref_with_lookup',
            rid,
            timeout,
            {bid_extra}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = nil, moved = {bid_extra}})
        res, err = vshard.storage._call('storage_unref', rid)
        t.assert_str_contains(err.message, 'Can not delete a storage ref: no ref')
        ilt.assert_equals(res, nil)

        -- Check for a timeout.
        -- Emulate a case when all buckets are not writable.
        local func = ivshard.storage.internal.bucket_are_all_rw
        ivshard.storage.internal.bucket_are_all_rw = function() return false end
        res, err = ivshard.storage._call(
            'storage_ref_with_lookup',
            rid,
            timeout,
            {bids[1]}
        )
        ivshard.storage.internal.bucket_are_all_rw = func
        t.assert_str_contains(err.message, 'Timeout exceeded')
        ilt.assert_equals(res, nil)
        -- Check that the reference was not created.
        res, err = ivshard.storage._call('storage_unref', rid)
        t.assert_str_contains(err.message, 'Can not delete a storage ref: no ref')
        ilt.assert_equals(res, nil)
    end)
end

test_group.test_absent_buckets = function(g)
    g.replica_1_a:exec(function()
        local res, err = ivshard.storage._call(
            'storage_absent_buckets',
            {_G.get_first_bucket()}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {})
    end)

    g.replica_1_a:exec(function()
        local bid_extra = 3001
        local res, err = ivshard.storage._call(
            'storage_absent_buckets',
            {_G.get_first_bucket(), bid_extra}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {bid_extra})
    end)
end
