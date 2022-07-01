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

local test_group = t.group('bucket_gc', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
            },
        },
    },
    bucket_count = 20
}
local cluster_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    cluster_cfg = vtest.config_new(cfg_template)

    vtest.storage_new(g, cluster_cfg)
    vtest.storage_bootstrap(g, cluster_cfg)
    vtest.storage_exec_each_master(g, function(engine)
        local s = box.schema.create_space('test', {
            engine = engine,
            format = {
                {'id', 'unsigned'},
                {'bid', 'unsigned'}
            },
        })
        s:create_index('pk')
        s:create_index('bucket_id', {unique = false, parts = {2}})
    end, {g.params.engine})
    vtest.storage_wait_vclock_all(g)
    vtest.storage_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Fill a few buckets, "send" them, ensure they and their data are gone.
--
test_group.test_basic = function(g)
    g.replica_1_a:exec(function()
        local bucket_space = box.space._bucket
        local status_index = bucket_space.index.status

        -- Find all active buckets.
        local buckets = status_index:select({ivconst.BUCKET.ACTIVE})
        ilt.assert_not_equals(#buckets, 0, 'get active buckets')
        local all_bids = table.new(#buckets, 0)
        for i, bucket in pairs(buckets) do
            all_bids[i] = bucket.id
        end

        -- Fill the space with data for all active buckets. Remember the data in
        -- Lua to check later that it is gone.
        local tuple_count = 3210
        local bucket_data = {}
        local s = box.space.test
        local batch = 100
        box.begin()
        for i = 1, tuple_count do
            local bid = all_bids[i % #all_bids + 1]
            local data = bucket_data[bid]
            if not data then
                data = {}
                bucket_data[bid] = data
            end
            s:replace{i, bid}
            table.insert(data, i)
            if i % batch == 0 then
                box.commit()
                box.begin()
            end
        end
        box.commit()

        -- Simulate sending of some buckets.
        local sent_bids = {}
        for i, bid in pairs(all_bids) do
            if i % 3 == 0 then
                table.insert(sent_bids, bid)
                bucket_space:update({bid}, {{'=', 2, ivconst.BUCKET.SENT}})
            end
        end
        ilt.assert(#sent_bids > 0, 'sent some buckets')

        -- The master has GC fiber.
        ilt.assert_not_equals(
            ivshard.storage.internal.collect_bucket_garbage_fiber, nil)

        -- Bucket GC deletes the buckets eventually.
        _G.bucket_gc_wait()

        -- Ensure both the sent buckets and their data are gone.
        for _, bid in pairs(sent_bids) do
            ilt.assert_equals(bucket_space:get(bid), nil,
                              'sent bucket is deleted')
            local data = bucket_data[bid]
            for _, i in pairs(data) do
                ilt.assert_equals(s:get(i), nil, 'bucket data is deleted')
            end
            bucket_data[bid] = nil
        end

        -- The other buckets are not touched.
        for bid, data in pairs(bucket_data) do
            local bucket = bucket_space:get(bid)
            ilt.assert_not_equals(bucket, nil, 'active bucket is kept')
            ilt.assert_equals(bucket.status, ivconst.BUCKET.ACTIVE)

            for _, i in pairs(data) do
                local tuple = s:get(i)
                ilt.assert_not_equals(tuple, nil, 'bucket data is kept')
                ilt.assert_equals(tuple.bid, bid)
            end
        end

        -- Restore the buckets for next tests.
        for _, bid in pairs(sent_bids) do
            ivshard.storage.bucket_force_create(bid)
        end
        s:truncate()
    end)

    -- Ensure the replica received all that and didn't break in the middle.
    g.replica_1_b:wait_vclock_of(g.replica_1_a)
    g.replica_1_b:exec(function()
        -- Non-master doesn't have a GC bucket.
        ilt.assert_equals(
            ivshard.storage.internal.collect_bucket_garbage_fiber, nil)
    end)
end

test_group.test_yield_before_send_commit = function(g)
    t.run_only_if(cluster_cfg.memtx_use_mvcc_engine)

    g.replica_1_a:exec(function()
        local s = box.space.test
        local bucket_space = box.space._bucket
        local bid = _G.get_first_bucket()
        ilt.assert_not_equals(bid, nil, 'get any bucket')

        -- Fill a bucket with some data.
        local tuple_count = 10
        box.begin()
        for i = 1, tuple_count do
            s:replace{i, bid}
        end
        box.commit()

        -- Start its "sending" but yield before commit.
        local is_send_blocked = true
        local f_send = ifiber.new(function()
            box.begin()
            bucket_space:update({bid}, {{'=', 2, ivconst.BUCKET.SENT}})
            while is_send_blocked do
                ifiber.sleep(0.01)
            end
            box.commit()
        end)
        f_send:set_joinable(true)
        ifiber.sleep(0.01)
        ilt.assert_equals(bucket_space:get{bid}.status, ivconst.BUCKET.ACTIVE,
                        'sending is not visible yet')
        is_send_blocked = false
        ilt.assert(f_send:join(), 'long send succeeded')

        -- Bucket GC should react on commit. Not wakeup on replace, notice no
        -- changes, and go to sleep.
        _G.bucket_gc_wait()
        ilt.assert_equals(s:count(), 0, 'no garbage data')

        -- Restore the bucket for next tests.
        ivshard.storage.bucket_force_create(bid)
        s:truncate()
    end)

    -- Ensure the replica received all that and didn't break.
    g.replica_1_b:wait_vclock_of(g.replica_1_a)
end

--
-- If replace in _bucket space fails, the GC fiber logs it and retries
-- periodically.
--
test_group.test_fail_bucket_space_replace = function(g)
    --
    -- A couple of buckets. One becomes SENT but all deletions in _bucket fail.
    --
    local bid1_active, bid2_sent = g.replica_1_a:exec(function()
        local _bucket = box.space._bucket
        local bstatus = ivconst.BUCKET
        local bid1_active = 1
        local bid2_sent = 2
        ilt.assert_equals(_bucket:get{bid1_active}.status, bstatus.ACTIVE)
        ilt.assert_equals(_bucket:get{bid2_sent}.status, bstatus.ACTIVE)
        rawset(_G, 'bucket_rollback_on_delete', function(old, new)
            if old ~= nil and new == nil then
                box.rollback()
            end
        end)
        _bucket:on_replace(_G.bucket_rollback_on_delete)
        local s = box.space.test
        box.begin()
        s:replace{1, bid1_active}
        s:replace{2, bid2_sent}
        s:replace{3, bid1_active}
        s:replace{4, bid2_sent}
        box.commit()
        _bucket:replace{bid2_sent, ivconst.BUCKET.SENT}
        -- Flush garbage into the logs to separate the next greps from the old
        -- logs.
        require('log').info(string.rep('a', 1000))
        return bid1_active, bid2_sent
    end)
    --
    -- GC sees the error.
    --
    local errmsg = 'Error during garbage collection step'
    t.helpers.retrying({timeout = vtest.wait_timeout}, function()
        if g.replica_1_a:grep_log(errmsg, 1000) then
            return
        end
        g.replica_1_a:exec(function()
            ivshard.storage.garbage_collector_wakeup()
        end)
        error('Not found the error')
    end)
    --
    -- GC eventually succeeds when the error is gone.
    --
    g.replica_1_a:exec(function(bid1_active, bid2_sent)
        local total_bucket_count = ivshard.storage.internal.total_bucket_count
        local s = box.space.test
        -- Garbage bucket data is already gone.
        ilt.assert_equals(s:select(), {{1, bid1_active}, {3, bid1_active}})
        -- But the bucket itself is still here.
        local _bucket = box.space._bucket
        ilt.assert_equals(_bucket:count(), total_bucket_count)
        -- Let the deletions go.
        _bucket:on_replace(nil, _G.bucket_rollback_on_delete)
        -- Bucket is deleted fine now.
        _G.bucket_gc_wait()
        ilt.assert_equals(_bucket:count(), total_bucket_count - 1)
        ilt.assert_equals(s:select(), {{1, bid1_active}, {3, bid1_active}})

        --
        -- Cleanup.
        --
        s:truncate()
        _bucket:replace{bid2_sent, ivconst.BUCKET.ACTIVE}
        local bucket_count = total_bucket_count
        ilt.assert_equals(_bucket:count(), bucket_count)
        ilt.assert_equals(_bucket.index.status:count(ivconst.BUCKET.ACTIVE),
                          bucket_count)
    end, {bid1_active, bid2_sent})
end

--
-- GC can work fine with many buckets, when it takes several steps to delete
-- everything with yields in between.
--
test_group.test_huge_bucket_count = function(g)
    g.replica_1_a:exec(function(engine)
        ilt.assert_not_equals(engine, nil)
        local _bucket = box.space._bucket
        local space_opts = {
                engine = engine,
                format = {
                    {'id', 'unsigned'},
                    {'bid', 'unsigned'}
                }
        }
        local index_opts = {parts = {{2}}, unique = false}

        local s1 = box.schema.create_space('test1', space_opts)
        s1:create_index('pk')
        s1:create_index('bucket_id', index_opts)

        local s2 = box.schema.create_space('test2', space_opts)
        s2:create_index('pk')
        s2:create_index('bucket_id', index_opts)

        local bucket_count_old = ivshard.storage.internal.total_bucket_count
        -- Reduce chunk size. Otherwise the test runs too long due to a lot of
        -- single-statement transactions in GC fiber.
        local old_chunk_size = ivconst.BUCKET_CHUNK_SIZE
        ivconst.BUCKET_CHUNK_SIZE = 100
        ilt.assert_gt(old_chunk_size, ivconst.BUCKET_CHUNK_SIZE)
        local bucket_count_new = ivconst.BUCKET_CHUNK_SIZE * 4 + 123
        ilt.assert_gt(bucket_count_new, bucket_count_old)
        _G.bucket_gc_pause()
        --
        -- Fill the buckets with data.
        --
        box.begin()
        for i = 1, bucket_count_new do
            if i % ivconst.BUCKET_CHUNK_SIZE == 0 then
                box.commit()
                box.begin()
            end
            s1:replace{i, i}
            s2:replace{i, i}
        end
        box.commit()
        --
        -- Simulate buckets' sending.
        --
        box.begin()
        for bid = 1, bucket_count_new do
            if bid % ivconst.BUCKET_CHUNK_SIZE == 0 then
                box.commit()
                box.begin()
            end
            if bid % 2 == 0 then
                _bucket:replace{bid, ivconst.BUCKET.GARBAGE}
            else
                _bucket:replace{bid, ivconst.BUCKET.SENT}
            end
        end
        box.commit()

        _G.bucket_gc_continue()
        _G.bucket_gc_wait()
        ilt.assert_equals(_bucket:count(), 0)
        ilt.assert_equals(s1:count(), 0)
        ilt.assert_equals(s2:count(), 0)
        --
        -- Cleanup.
        --
        ivconst.BUCKET_CHUNK_SIZE = old_chunk_size
        s1:drop()
        s2:drop()
        box.begin()
        for bid = 1, bucket_count_old do
            _bucket:replace{bid, ivconst.BUCKET.ACTIVE}
        end
        box.commit()
    end, {g.params.engine})
end

--
-- Unit tests for gc_bucket_drop() internal function.
--
test_group.test_unit_gc_bucket_drop = function(g)
    g.replica_1_a:exec(function(engine)
        ilt.assert_not_equals(engine, nil)
        local _bucket = box.space._bucket
        local _bucket_idx_status = _bucket.index.status
        local gc_bucket_drop = ivshard.storage.internal.gc_bucket_drop
        local total_bucket_count = ivshard.storage.internal.total_bucket_count
        local bstatus = ivconst.BUCKET

        _G.bucket_gc_pause()
        _G.bucket_recovery_pause()

        local space_opts = {
                engine = engine,
                format = {
                    {'id', 'unsigned'},
                    {'bid', 'unsigned'}
                }
        }
        local index_opts = {parts = {{2}}, unique = false}

        local s1 = box.schema.create_space('test1', space_opts)
        s1:create_index('pk')
        s1:create_index('bucket_id', index_opts)

        local s2 = box.schema.create_space('test2', space_opts)
        s2:create_index('pk')
        s2:create_index('bucket_id', index_opts)

        local bid1_active = 1
        local bid2_receiving = 2
        local bid3_active = 3
        local bid4_sent = 4
        local bid5_garbage = 5
        local bid6_garbage = 6
        local bid7_active = 10
        local bid8_garbage = 13
        local bid9_active = 20

        local space1_data_by_bucket = {
            [bid1_active] = {1, 2},
            [bid2_receiving] = {3, 4},
            [bid4_sent] = {7},
            [bid5_garbage] = {8},
            [bid7_active] = {5, 6},
            [bid8_garbage] = (function()
                local data = table.new(1200, 0)
                for i = 9, 1200 do
                    table.insert(data, i)
                end
                return data
            end)(),
            [bid9_active] = {9, 10},
        }
        local space2_data_by_bucket = {
            [bid1_active] = {1},
            [bid3_active] = {3},
            [bid4_sent] = {6},
            [bid5_garbage] = {7},
            [bid7_active] = {5},
            [bid8_garbage] = {4},
            [bid6_garbage] = {8},
            [bid9_active] = {9, 10, 11, 12},
        }
        local space_data_by_bucket = {
            [s1.id] = space1_data_by_bucket,
            [s2.id] = space2_data_by_bucket,
        }

        local function data_prepare()
            box.begin()
            for sid, space_data in pairs(space_data_by_bucket) do
                local s = box.space[sid]
                for bid, ids in pairs(space_data) do
                    for _, id in pairs(ids) do
                        s:replace{id, bid}
                    end
                end
            end
            box.commit()

            _bucket:replace{bid2_receiving, bstatus.RECEIVING}
            _bucket:replace{bid4_sent, bstatus.SENT, 'destination1'}
            _bucket:replace{bid5_garbage, bstatus.GARBAGE}
            _bucket:replace{bid6_garbage, bstatus.GARBAGE, 'destination2'}
            _bucket:replace{bid8_garbage, bstatus.GARBAGE}
            ilt.assert_equals(_bucket:count(), total_bucket_count)
        end

        local function data_validate()
            for sid, space_data in pairs(space_data_by_bucket) do
                local s = box.space[sid]
                for bid, ids in pairs(space_data) do
                    if _bucket:get(bid) == nil then
                        ilt.assert_equals(s.index.bucket_id:select{bid}, {})
                        break
                    end
                    for _, id in pairs(ids) do
                        ilt.assert_equals(s:get{id}[2], bid)
                    end
                end
            end
        end

        local bucket_count = total_bucket_count
        --
        -- Collect GARBAGE.
        --
        data_prepare()
        local route_map = {}
        bucket_count = bucket_count - _bucket_idx_status:count(bstatus.GARBAGE)
        ilt.assert(gc_bucket_drop(bstatus.GARBAGE, route_map))
        ilt.assert_equals(route_map, {[bid6_garbage] = 'destination2'})
        ilt.assert_equals(_bucket:count(), bucket_count)
        ilt.assert_equals(_bucket.index.status:select(bstatus.GARBAGE), {})
        data_validate()
        --
        -- Collect SENT.
        --
        route_map = {}
        bucket_count = bucket_count - _bucket_idx_status:count(bstatus.SENT)
        ilt.assert(gc_bucket_drop(bstatus.SENT, route_map))
        ilt.assert_equals(route_map, {[bid4_sent] = 'destination1'})
        ilt.assert_equals(_bucket:count(), bucket_count)
        ilt.assert_equals(_bucket.index.status:select(bstatus.SENT), {})
        data_validate()
        --
        -- Collect second time and nothing happens.
        --
        route_map = {}
        ilt.assert(gc_bucket_drop(bstatus.GARBAGE, route_map))
        ilt.assert(gc_bucket_drop(bstatus.SENT, route_map))
        ilt.assert_equals(_bucket:count(), bucket_count)
        ilt.assert_equals(route_map, {})
        data_validate()
        --
        -- Continuous background collection.
        --
        data_prepare()
        bucket_count = total_bucket_count
        bucket_count = bucket_count - _bucket_idx_status:count(bstatus.GARBAGE)
        bucket_count = bucket_count - _bucket_idx_status:count(bstatus.SENT)
        _G.bucket_gc_continue()
        _G.bucket_gc_wait()
        ilt.assert_equals(_bucket:count(), bucket_count)
        ilt.assert_equals(_bucket.index.status:select(bstatus.GARBAGE), {})
        ilt.assert_equals(_bucket.index.status:select(bstatus.SENT), {})
        data_validate()
        --
        -- Cleanup.
        --
        s1:drop()
        s2:drop()
        _bucket:replace{bid2_receiving, bstatus.ACTIVE}
        _bucket:replace{bid4_sent, bstatus.ACTIVE}
        _bucket:replace{bid5_garbage, bstatus.ACTIVE}
        _bucket:replace{bid6_garbage, bstatus.ACTIVE}
        _bucket:replace{bid8_garbage, bstatus.ACTIVE}
        ilt.assert_equals(_bucket:count(), total_bucket_count)
        ilt.assert_equals(_bucket.index.status:count(bstatus.ACTIVE),
                          total_bucket_count)
        _G.bucket_recovery_continue()
    end, {g.params.engine})
end

--
-- Unit tests for vshard.storage.bucket_delete_garbage().
--
test_group.test_unit_bucket_delete_garbage = function(g)
    g.replica_1_a:exec(function(engine)
        ilt.assert_not_equals(engine, nil)
        local _bucket = box.space._bucket
        local delete_garbage = ivshard.storage.bucket_delete_garbage
        local total_bucket_count = ivshard.storage.internal.total_bucket_count
        local bstatus = ivconst.BUCKET
        local s = box.space.test
        local bid1 = 1
        local bid2 = 2
        ilt.assert_equals(_bucket:get{bid1}.status, bstatus.ACTIVE)
        ilt.assert_equals(_bucket:get{bid2}.status, bstatus.ACTIVE)

        local function data_prepare()
            box.begin()
            s:replace{1, bid1}
            s:replace{2, bid2}
            s:replace{3, bid1}
            s:replace{4, bid2}
            box.commit()
        end

        _G.bucket_gc_pause()
        _G.bucket_recovery_pause()

        -- Bad usage.
        ilt.assert_error_msg_contains('Usage: ', delete_garbage)
        ilt.assert_error_msg_contains('Usage: ', delete_garbage, bid2, 10000)

        -- Delete an existing garbage bucket.
        data_prepare()
        _bucket:replace{bid2, ivconst.BUCKET.GARBAGE}
        delete_garbage(bid2)
        ilt.assert_equals(s:select{}, {{1, bid1}, {3, bid1}})

        -- Delete data from a not existing bucket.
        _bucket:delete{bid2}
        data_prepare()
        delete_garbage(bid2)
        ilt.assert_equals(s:select{}, {{1, bid1}, {3, bid1}})

        -- Fail to delete a not garbage bucket.
        data_prepare()
        ilt.assert_equals(s:count(), 4)
        _bucket:replace{bid2, ivconst.BUCKET.ACTIVE}
        ilt.assert_equals(s:select{}, {{1, 1}, {2, 2}, {3, 1}, {4, 2}})
        ilt.assert_equals(s:count(), 4)
        ilt.assert_error_msg_contains('Can not delete not garbage bucket',
                                      delete_garbage, bid2)
        ilt.assert_equals(s:select{}, {{1, 1}, {2, 2}, {3, 1}, {4, 2}})
        ilt.assert_equals(s:count(), 4)

        -- 'Force' option ignores the error.
        delete_garbage(bid2, {force = true})
        ilt.assert_equals(s:select{}, {{1, bid1}, {3, bid1}})

        --
        -- Cleanup.
        --
        s:truncate()
        ilt.assert_equals(_bucket:count(), total_bucket_count)
        ilt.assert_equals(_bucket.index.status:count(bstatus.ACTIVE),
                          total_bucket_count)
        _G.bucket_recovery_continue()
        _G.bucket_gc_continue()
    end, {g.params.engine})
end

--
-- Unit tests for vshard.storage._call('bucket_test_gc').
--
test_group.test_unit_bucket_test_gc = function(g)
    g.replica_1_a:exec(function()
        local bucket_count = ivshard.storage.internal.total_bucket_count
        local _bucket = box.space._bucket
        -- Ensure the bucket IDs are {1..20} so as could hardcode some bids.
        ilt.assert_equals(_bucket.index.status:count(ivconst.BUCKET.ACTIVE),
                          bucket_count)
        ilt.assert_equals(_bucket.index[0]:min().id, 1)
        ilt.assert_equals(_bucket.index[0]:max().id, bucket_count)
        ilt.assert_equals(bucket_count, 20)

        _G.bucket_gc_pause()
        _G.bucket_recovery_pause()

        local test_gc = function(bids)
            local res, err = ivshard.storage._call('bucket_test_gc', bids)
            if not res then
                return nil, err
            end
            return res.bids_not_ok
        end

        local sent_bids = {1, 5, 15, 16, 17}
        box.begin()
        for _, bid in pairs(sent_bids) do
            _bucket:update({bid}, {{'=', 2, ivconst.BUCKET.SENT}})
        end
        box.commit()
        --
        -- All is available for GC.
        --
        ilt.assert_equals(test_gc({}), {})
        ilt.assert_equals(test_gc({1}), {})
        ilt.assert_equals(test_gc({1, 5, 15}), {})
        --
        -- One bucket ID is out of range.
        --
        local res, err = test_gc({5, 15, 21})
        ilt.assert_covers(err, {
            code = iverror.code.WRONG_BUCKET,
            bucket_id = 21
        })
        ilt.assert_equals(res, nil)
        --
        -- Some buckets still have RO refs.
        --
        local sent_ro_bids = {5, 16, 17}
        box.begin()
        for _, bid in pairs(sent_ro_bids) do
            _bucket:update({bid}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
            ilt.assert(ivshard.storage.bucket_refro(bid))
            _bucket:update({bid}, {{'=', 2, ivconst.BUCKET.SENT}})
        end
        box.commit()
        ilt.assert_equals(test_gc(sent_bids), sent_ro_bids)
        box.begin()
        for _, bid in pairs(sent_ro_bids) do
            ilt.assert(ivshard.storage.bucket_unrefro(bid))
            _bucket:update({bid}, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        end
        box.commit()
        --
        -- Long bucket list iteration should yield.
        --
        -- Reduce the chunk size to speed the test up.
        local old_chunk_size = ivconst.BUCKET_CHUNK_SIZE
        ivconst.BUCKET_CHUNK_SIZE = 100
        local bucket_count_new = ivconst.BUCKET_CHUNK_SIZE * 4 + 10
        ilt.assert_gt(bucket_count_new, bucket_count)
        sent_bids = {}
        sent_ro_bids = {}
        box.begin()
        for i = 1, bucket_count_new do
            _bucket:replace({i, ivconst.BUCKET.ACTIVE})
            if i % 2 == 0 then
                table.insert(sent_bids, i)
                if i % 3 == 0 then
                    table.insert(sent_ro_bids, i)
                    ilt.assert(ivshard.storage.bucket_refro(i))
                end
                _bucket:replace({i, ivconst.BUCKET.SENT})
            end
        end
        box.commit()
        local csw1 = ifiber.self():csw()
        ilt.assert_equals(test_gc(sent_bids), sent_ro_bids)
        local csw2 = ifiber.self():csw()
        ilt.assert_equals(csw2, csw1 + 2)
        --
        -- Cleanup
        --
        ivconst.BUCKET_CHUNK_SIZE = old_chunk_size
        for _, bid in pairs(sent_ro_bids) do
            ilt.assert(ivshard.storage.bucket_unrefro(bid))
        end
        box.begin()
        for bid = 1, bucket_count do
            _bucket:replace({bid, ivconst.BUCKET.ACTIVE})
        end
        for bid = bucket_count + 1, bucket_count_new do
            _bucket:delete({bid})
        end
        box.commit()
        _G.bucket_recovery_continue()
        _G.bucket_gc_continue()
    end)
end
