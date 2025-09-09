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
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
            },
        },
        {
            replicas = {
                replica_3_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 15,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_wait_vclock_all(g)

    vtest.cluster_exec_each_master(g, function()
        box.schema.create_space('test_space')
        box.space.test_space:format({
            {name = 'pk', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
        })
        box.space.test_space:create_index('primary', {parts = {'pk'}})
        box.space.test_space:create_index(
            'bucket_id', {parts = {'bucket_id'}, unique = false})
    end)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Test that manual vshard.storage.bucket_send() cannot lead to
-- doubled buckets (gh-414).
--
test_group.test_manual_bucket_send_doubled_buckets = function(g)
    vtest.cluster_exec_each_master(g, function()
        _G.bucket_recovery_pause()
    end)

    local uuid_2 = g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LONG_RECEIVE = true
        return ivutil.replicaset_uuid()
    end)

    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert(iverror.is_timeout(err))
        ilt.assert_not(ok, 'bucket_send not ok')
        return bid
    end, {uuid_2})

    g.replica_2_a:exec(function(bid, uuid)
        ivshard.storage.internal.errinj.ERRINJ_LONG_RECEIVE = false
        ilt.assert_equals(box.space._bucket:get(bid).status,
                          ivconst.BUCKET.ACTIVE)
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err, nil, 'bucket_send no error')
        ilt.assert(ok, 'bucket_send ok')
        _G.bucket_recovery_continue()
    end, {bid, g.replica_3_a:replicaset_uuid()})

    g.replica_3_a:exec(function(bid)
        ilt.assert_equals(box.space._bucket:get(bid).status,
                          ivconst.BUCKET.ACTIVE)
    end, {bid})

    g.replica_1_a:exec(function(bid)
        _G.bucket_recovery_continue()
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        ilt.assert_equals(box.space._bucket:get(bid), nil)
    end, {bid})
end

local function start_partial_bucket_move(src_storage, dest_storage, bucket_id)
    src_storage:exec(function(bucket_id, replicaset_id)
        local res, err = ivshard.storage.bucket_send(bucket_id, replicaset_id)
        t.assert_not(res)
        t.assert(err)
            -- The bucket on src_storage must be in "sending" state. The
            -- recovery service on src_storage should not erase this bucket.
        t.assert_equals(box.space._bucket:get(bucket_id).status, 'sending')
    end, {bucket_id, dest_storage:replicaset_uuid()})

    dest_storage:exec(function(bucket_id)
        t.helpers.retrying({timeout = 10}, function()
            -- The recovery service on dest_storage should clear this bucket.
            t.assert_equals(box.space._bucket:select(bucket_id), {})
        end)
    end, {bucket_id})
end

local function wait_for_bucket_is_transferred(src_storage, dest_storage,
                                              bucket_id)
    src_storage:exec(function(bucket_id)
        t.helpers.retrying({timeout = 10}, function()
            t.assert_equals(box.space._bucket:select(bucket_id), {})
        end)
    end, {bucket_id})
    dest_storage:exec(function(bucket_id)
        t.helpers.retrying({timeout = 10}, function()
            t.assert_equals(box.space._bucket:get(bucket_id).status, 'active')
        end)
    end, {bucket_id})
end

local function move_bucket(src_storage, dest_storage, bucket_id)
    src_storage:exec(function(bucket_id, replicaset_id)
        t.helpers.retrying({timeout = 60}, function()
            local res, err = ivshard.storage.bucket_send(bucket_id,
                                                         replicaset_id)
            t.assert_not(err)
            t.assert(res)
        end)
    end, {bucket_id, dest_storage:replicaset_uuid()})
    wait_for_bucket_is_transferred(src_storage, dest_storage, bucket_id)
end

--
-- Reduce spam of "Finish bucket recovery step" logs and add logging of
-- recovered buckets in recovery service (gh-212).
--
test_group.test_no_logs_while_unsuccess_recovery = function(g)
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = true
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = function(service_name, ...)
            if service_name == 'recovery_bucket_stat' then
                return error('TimedOut')
            end
            return _G.old_call(service_name, ...)
        end
    end)
    local hung_bucket_id_1 = vtest.storage_first_bucket(g.replica_1_a)
    start_partial_bucket_move(g.replica_1_a, g.replica_2_a, hung_bucket_id_1)
    local hung_bucket_id_2 = vtest.storage_first_bucket(g.replica_1_a)
    start_partial_bucket_move(g.replica_1_a, g.replica_2_a, hung_bucket_id_2)
    t.helpers.retrying({}, function()
        g.replica_1_a:exec(function() ivshard.storage.recovery_wakeup() end)
        t.assert(g.replica_1_a:grep_log('Error during recovery of bucket'))
    end)
    t.assert_not(g.replica_1_a:grep_log('Finish bucket recovery step, 0'))
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
        ivshard.storage._call = _G.old_call
    end)
    t.helpers.retrying({timeout = 60}, function()
        g.replica_2_a:exec(function()
            ivshard.storage.garbage_collector_wakeup()
            ivshard.storage.recovery_wakeup()
        end)
        g.replica_1_a:exec(function() ivshard.storage.recovery_wakeup() end)
        -- In some rare cases the recovery service can recover buckets one
        -- by one. As a result we get multiple "Finish bucket recovery" and
        -- "Recovery buckets" logs with different bucket ids and buckets'
        -- count. That is why we should grep general logs without buckets'
        -- count and bucket ids to avoid flakiness.
        t.assert(g.replica_1_a:grep_log('Finish bucket recovery step'))
        t.assert(g.replica_1_a:grep_log('Recovered buckets'))
    end)
    wait_for_bucket_is_transferred(g.replica_2_a, g.replica_1_a,
                                   hung_bucket_id_1)
    wait_for_bucket_is_transferred(g.replica_2_a, g.replica_1_a,
                                   hung_bucket_id_2)
end

--
-- Add logging of routes in rebalancer service (gh-212).
--
test_group.test_rebalancer_routes_logging = function(g)
    local moved_bucket_from_2 = vtest.storage_first_bucket(g.replica_2_a)
    move_bucket(g.replica_2_a, g.replica_1_a, moved_bucket_from_2)
    local moved_bucket_from_3 = vtest.storage_first_bucket(g.replica_3_a)
    move_bucket(g.replica_3_a, g.replica_1_a, moved_bucket_from_3)
    vtest.cluster_rebalancer_enable(g)
    t.helpers.retrying({timeout = 60}, function()
        g.replica_1_a:exec(function() ivshard.storage.rebalancer_wakeup() end)
        t.assert(g.replica_1_a:grep_log('Apply rebalancer routes with 1 ' ..
                                        'workers'))
    end)
    local rebalancer_routes_msg = string.format(
        'The following rebalancer routes were sent: {\"%s\":',
        g.replica_1_a:replicaset_uuid())
    local rebalancer_transfer_msg = string.format(
        '1 buckets were successfully sent to %s',
        g.replica_2_a:replicaset_uuid())
    t.helpers.retrying({}, function()
        t.assert(g.replica_1_a:grep_log(rebalancer_routes_msg))
        t.assert(g.replica_1_a:grep_log(rebalancer_transfer_msg))
        g.replica_1_a:exec(function() ivshard.storage.rebalancer_wakeup() end)
        g.replica_1_a:grep_log('The cluster is balanced ok.')
    end)
    vtest.cluster_rebalancer_disable(g)
end
