local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local wait_timeout = 120

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
    g.replica_1_a:exec(function(timeout)
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
        _G.wait_bucket_gc(timeout)

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
    end, {wait_timeout})

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

    g.replica_1_a:exec(function(timeout)
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
        _G.wait_bucket_gc(timeout)
        ilt.assert_equals(s:count(), 0, 'no garbage data')

        -- Restore the bucket for next tests.
        ivshard.storage.bucket_force_create(bid)
        s:truncate()
    end, {wait_timeout})

    -- Ensure the replica received all that and didn't break.
    g.replica_1_b:wait_vclock_of(g.replica_1_a)
end
