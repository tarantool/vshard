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

--
-- gh-489: allow to turn off checking the bucket status.
--
test_group.test_bucket_skip_validate = function(g)
    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        internal.errinj.ERRINJ_SKIP_BUCKET_STATUS_VALIDATE = true

        local bid = _G.get_first_bucket()
        box.space._bucket:update(bid, {{'=', 2, 'receiving'}})
        box.space._bucket:delete(bid)
        box.space._bucket:insert({bid, 'active'})

        internal.errinj.ERRINJ_SKIP_BUCKET_STATUS_VALIDATE = false
    end)
end

test_group.test_ref_with_buckets_basic = function(g)
    g.replica_1_a:exec(function()
        local lref = require('vshard.storage.ref')
        local res, err, _
        local rid = 42
        local bids = _G.get_n_buckets(2)
        local bucket_count = ivshard.storage.internal.total_bucket_count

        -- No buckets.
        res, err = ivshard.storage._call(
            'storage_ref_with_buckets', rid, iwait_timeout, {})
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {moved = {}})
        ilt.assert_equals(lref.count, 0)

        -- Check for a single ok bucket.
        res, err = ivshard.storage._call(
            'storage_ref_with_buckets', rid, iwait_timeout, {bids[1]})
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = rid, moved = {}})
        ilt.assert_equals(lref.count, 1)
        _, err = ivshard.storage._call('storage_unref', rid)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(lref.count, 0)

        -- Check for multiple ok buckets.
        res, err = ivshard.storage._call(
            'storage_ref_with_buckets', rid, iwait_timeout, {bids[1], bids[2]})
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = rid, moved = {}})
        _, err = ivshard.storage._call('storage_unref', rid)
        ilt.assert_equals(err, nil)

        -- Check for double referencing.
        res, err = ivshard.storage._call(
            'storage_ref_with_buckets', rid, iwait_timeout, {bids[1], bids[1]})
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {rid = rid, moved = {}})
        ilt.assert_equals(lref.count, 1)
        _, err = ivshard.storage._call('storage_unref', rid)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(lref.count, 0)

        -- Bucket mix.
        res, err = ivshard.storage._call(
            'storage_ref_with_buckets', rid, iwait_timeout,
            {bucket_count + 1, bids[1], bucket_count + 2, bids[2],
             bucket_count + 3})
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {
            rid = rid,
            moved = {
                {id = bucket_count + 1},
                {id = bucket_count + 2},
                {id = bucket_count + 3},
            }
        })
        _, err = ivshard.storage._call('storage_unref', rid)
        ilt.assert_equals(err, nil)

        -- No ref when all buckets are missing.
        res, err = ivshard.storage._call(
            'storage_ref_with_buckets',
            rid,
            iwait_timeout,
            {bucket_count + 1, bucket_count + 2}
        )
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {moved = {
            {id = bucket_count + 1},
            {id = bucket_count + 2},
        }})
        ilt.assert_equals(lref.count, 0)
    end)
end

test_group.test_ref_with_buckets_timeout = function(g)
    g.replica_1_a:exec(function()
        local lref = require('vshard.storage.ref')
        local rid = 42
        local bids = _G.get_n_buckets(2)
        --
        -- Timeout when some buckets aren't writable. Doesn't have to be the
        -- same buckets as for moving.
        --
        _G.bucket_recovery_pause()
        box.space._bucket:update(
            {bids[1]}, {{'=', 2, ivconst.BUCKET.SENDING}})
        local res, err = ivshard.storage._call(
            'storage_ref_with_buckets', rid, 0.01, {bids[2]})
        box.space._bucket:update(
            {bids[1]}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        t.assert_str_contains(err.message, 'Timeout exceeded')
        ilt.assert_equals(res, nil)
        ilt.assert_equals(lref.count, 0)
        _G.bucket_recovery_continue()
    end)
end

test_group.test_ref_with_buckets_return_last_known_dst = function(g)
    g.replica_1_a:exec(function()
        local lref = require('vshard.storage.ref')
        local rid = 42
        local bid = _G.get_first_bucket()
        local luuid = require('uuid')
        local id = luuid.str()
        -- Make the bucket follow the correct state sequence. Another way is
        -- validated and not allowed.
        _G.bucket_recovery_pause()
        _G.bucket_gc_pause()
        box.space._bucket:update(
            {bid}, {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id}})
        box.space._bucket:update(
            {bid}, {{'=', 2, ivconst.BUCKET.SENT}})
        local res, err = ivshard.storage._call(
            'storage_ref_with_buckets', rid, iwait_timeout, {bid})
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {moved = {{
            id = bid,
            dst = id,
            status = ivconst.BUCKET.SENT,
        }}})
        ilt.assert_equals(lref.count, 0)
        -- Cleanup.
        _G.bucket_gc_continue()
        _G.bucket_gc_wait()
        box.space._bucket:insert({bid, ivconst.BUCKET.RECEIVING})
        box.space._bucket:update({bid}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        _G.bucket_recovery_continue()
    end)
end

test_group.test_ref_with_buckets_move_part_while_referencing = function(g)
    g.replica_1_a:exec(function()
        local lref = require('vshard.storage.ref')
        local _
        local rid = 42
        local bids = _G.get_n_buckets(3)
        local luuid = require('uuid')
        local id = luuid.str()
        --
        -- Was not moved until ref, but moved while ref was waiting.
        --
        _G.bucket_recovery_pause()
        _G.bucket_gc_pause()
        -- Block the refs for a while.
        box.space._bucket:update(
            {bids[3]}, {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id}})
        -- Start referencing.
        local session_id
        local f = ifiber.new(function()
            session_id = box.session.id()
            return ivshard.storage._call('storage_ref_with_buckets', rid,
                                         iwait_timeout, {bids[1], bids[2]})
        end)
        f:set_joinable(true)
        -- While waiting, one of the target buckets starts moving.
        box.space._bucket:update(
            {bids[2]}, {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id}})
        -- Now they are moved.
        box.space._bucket:update({bids[2]}, {{'=', 2, ivconst.BUCKET.SENT}})
        box.space._bucket:update({bids[3]}, {{'=', 2, ivconst.BUCKET.SENT}})
        _G.bucket_gc_continue()
        _G.bucket_gc_wait()
        local ok, res, err = f:join()
        t.assert(ok)
        t.assert_equals(err, nil)
        ilt.assert_equals(res, {
            moved = {{id = bids[2], dst = id}},
            rid = rid,
        })
        -- Ref was done, because at least one bucket was ok.
        ilt.assert_equals(lref.count, 1)
        -- Cleanup.
        _, err = lref.del(rid, session_id)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(lref.count, 0)
        box.space._bucket:insert({bids[2], ivconst.BUCKET.RECEIVING})
        box.space._bucket:update({bids[2]}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        box.space._bucket:insert({bids[3], ivconst.BUCKET.RECEIVING})
        box.space._bucket:update({bids[3]}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        _G.bucket_recovery_continue()
    end)
end

test_group.test_ref_with_buckets_move_all_while_referencing = function(g)
    g.replica_1_a:exec(function()
        local lref = require('vshard.storage.ref')
        local rid = 42
        local bids = _G.get_n_buckets(3)
        local luuid = require('uuid')
        local id = luuid.str()
        --
        -- Now same, but all buckets were moved. No ref should be left then.
        --
        _G.bucket_recovery_pause()
        _G.bucket_gc_pause()
        -- Block the refs for a while.
        box.space._bucket:update(
            {bids[3]}, {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id}})
        -- Start referencing.
        local f = ifiber.new(function()
            return ivshard.storage._call('storage_ref_with_buckets', rid,
                                         iwait_timeout, {bids[1], bids[2]})
        end)
        f:set_joinable(true)
        -- While waiting, all the target buckets start moving.
        box.space._bucket:update(
            {bids[1]}, {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id}})
        box.space._bucket:update(
            {bids[2]}, {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id}})
        -- Now they are moved.
        box.space._bucket:update({bids[1]}, {{'=', 2, ivconst.BUCKET.SENT}})
        box.space._bucket:update({bids[2]}, {{'=', 2, ivconst.BUCKET.SENT}})
        -- And the other bucket doesn't matter. Can revert it back.
        box.space._bucket:update({bids[3]}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        _G.bucket_gc_continue()
        _G.bucket_gc_wait()
        local ok, res, err = f:join()
        t.assert(ok)
        t.assert_equals(err, nil)
        ilt.assert_equals(res, {
            moved = {
                {id = bids[1], dst = id},
                {id = bids[2], dst = id},
            }
        })
        -- Ref was not done, because all the buckets moved out.
        ilt.assert_equals(lref.count, 0)
        -- Cleanup.
        box.space._bucket:insert({bids[1], ivconst.BUCKET.RECEIVING})
        box.space._bucket:update({bids[1]}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        box.space._bucket:insert({bids[2], ivconst.BUCKET.RECEIVING})
        box.space._bucket:update({bids[2]}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        _G.bucket_recovery_continue()
    end)
end

test_group.test_moved_buckets_various_statuses = function(g)
    g.replica_1_a:exec(function()
        local _bucket = box.space._bucket
        -- Make sure that if any bucket status is added/deleted/changed, the
        -- test gets an update.
        ilt.assert_equals(ivconst.BUCKET, {
            ACTIVE = 'active',
            PINNED = 'pinned',
            SENDING = 'sending',
            SENT = 'sent',
            RECEIVING = 'receiving',
            GARBAGE = 'garbage',
        })
        _G.bucket_recovery_pause()
        _G.bucket_gc_pause()
        local luuid = require('uuid')
        -- +1 to delete and make it a 404 bucket.
        local bids = _G.get_n_buckets(7)

        -- ACTIVE = bids[1].
        --
        -- PINNED = bids[2].
        local bid_pinned = bids[2]
        _bucket:update({bid_pinned},
            {{'=', 2, ivconst.BUCKET.PINNED}})

        -- SENDING = bids[3].
        local bid_sending = bids[3]
        local id_sending = luuid.str()
        _bucket:update({bid_sending},
            {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id_sending}})

        -- SENT = bids[4].
        local bid_sent = bids[4]
        local id_sent = luuid.str()
        _bucket:update({bid_sent},
            {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id_sent}})
        _bucket:update({bid_sent},
            {{'=', 2, ivconst.BUCKET.SENT}})

        -- RECEIVING = bids[5].
        local bid_receiving = bids[5]
        local id_receiving = luuid.str()
        _bucket:update({bid_receiving},
            {{'=', 2, ivconst.BUCKET.SENDING}})
        _bucket:update({bid_receiving},
            {{'=', 2, ivconst.BUCKET.SENT}})
        _bucket:update({bid_receiving},
            {{'=', 2, ivconst.BUCKET.GARBAGE}})
        _bucket:delete({bid_receiving})
        _bucket:insert({bid_receiving, ivconst.BUCKET.RECEIVING, id_receiving})

        -- GARBAGE = bids[6].
        local bid_garbage = bids[6]
        local id_garbage = luuid.str()
        _bucket:update({bid_garbage},
            {{'=', 2, ivconst.BUCKET.SENDING}, {'=', 3, id_garbage}})
        _bucket:update({bid_garbage},
            {{'=', 2, ivconst.BUCKET.SENT}})
        _bucket:update({bid_garbage},
            {{'=', 2, ivconst.BUCKET.GARBAGE}})

        -- NOT EXISTING = bids[7].
        local bid_404 = bids[7]
        _bucket:update({bid_404},
            {{'=', 2, ivconst.BUCKET.SENDING}})
        _bucket:update({bid_404},
            {{'=', 2, ivconst.BUCKET.SENT}})
        _bucket:update({bid_404},
            {{'=', 2, ivconst.BUCKET.GARBAGE}})
        _bucket:delete({bid_404})

        local res, err = ivshard.storage._call('moved_buckets', bids)
        ilt.assert_equals(err, nil)
        ilt.assert(res and res.moved)
        ilt.assert_items_equals(res.moved, {
            {
                id = bid_sent,
                dst = id_sent,
                status = ivconst.BUCKET.SENT,
            },
            {
                id = bid_garbage,
                dst = id_garbage,
                status = ivconst.BUCKET.GARBAGE,
            },
            {
                id = bid_404,
            }
        })
        --
        -- Cleanup.
        --
        -- NOT EXISTING.
        _bucket:insert({bid_404, ivconst.BUCKET.RECEIVING})
        _bucket:update({bid_404},
            {{'=', 2, ivconst.BUCKET.ACTIVE}})
        -- GARBAGE.
        _bucket:delete({bid_garbage})
        _bucket:insert({bid_garbage, ivconst.BUCKET.RECEIVING})
        _bucket:update({bid_garbage},
            {{'=', 2, ivconst.BUCKET.ACTIVE}})
        -- RECEIVING.
        _bucket:update({bid_receiving},
            {{'=', 2, ivconst.BUCKET.ACTIVE}})
        -- SENT.
        _bucket:update({bid_sent},
            {{'=', 2, ivconst.BUCKET.GARBAGE}})
        _bucket:delete({bid_sent})
        _bucket:insert({bid_sent, ivconst.BUCKET.RECEIVING})
        _bucket:update({bid_sent},
            {{'=', 2, ivconst.BUCKET.ACTIVE}})
        -- SENDING.
        _bucket:update({bid_sending},
            {{'=', 2, ivconst.BUCKET.ACTIVE}})
        -- PINNED.
        _bucket:update({bid_pinned},
            {{'=', 2, ivconst.BUCKET.ACTIVE}})

        _G.bucket_recovery_continue()
        _G.bucket_gc_continue()
    end)
end
