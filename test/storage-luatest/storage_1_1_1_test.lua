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

local rebalancer_recovery_group = t.group('rebalancer-recovery-logging')

local function move_bucket(src_storage, dest_storage, bucket_id)
    src_storage:exec(function(bucket_id, replicaset_id)
        ivshard.storage.bucket_send(bucket_id, replicaset_id)
    end, {bucket_id, dest_storage:replicaset_uuid()})

    dest_storage:exec(function(bucket_id)
        t.helpers.retrying({timeout = 10}, function()
            t.assert(box.space._bucket:select(bucket_id))
        end)
    end, {bucket_id})
end

local function test_only_one_record_appears_in_logs(server, record, wait_time)
    local first_log_record = nil
    t.helpers.retrying({timeout = 10}, function()
        first_log_record = server:grep_log(record)
        t.assert(first_log_record)
    end)
    -- We need to wait a bit in order to catch how much as possible
    -- spam in server's logs.
    require('fiber').sleep(wait_time)
    local last_log_record = server:grep_log(record)
    t.assert(last_log_record)
    t.assert_equals(first_log_record, last_log_record,
                    'There are two identical records in logs')
end

rebalancer_recovery_group.before_all(function(g)
    global_cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(g, global_cfg)
    g.router = vtest.router_new(g, 'router', global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
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
    g.replicaset_not_connected_pattern = '%d+-%d+-%d+ %d+:%d+:%d+.%d+ .* '
    g.replcaset_not_connected_msg = 'Some buckets in replicaset %s ' ..
                                    'are not active! '
    g.rebalancer_wait_interval = 0.01
end)

rebalancer_recovery_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Improve logging of rebalancer and recovery (gh-212).
--
rebalancer_recovery_group.test_no_logs_while_unsuccess_recovery = function(g)
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
    local hanged_bucket_id = vtest.storage_first_bucket(g.replica_1_a)
    move_bucket(g.replica_1_a, g.replica_2_a, hanged_bucket_id)
    t.helpers.retrying({}, function()
        t.assert(g.replica_1_a:grep_log('Error during recovery of bucket 1'))
    end)
    t.assert_not(g.replica_1_a:grep_log('Finish bucket recovery step, 0'))
    g.replica_2_a:exec(function(hanged_bucket_id)
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
        ivshard.storage._call = _G.old_call
        ivshard.storage.recovery_wakeup()
        t.helpers.retrying({}, function()
            t.assert_equals(box.space._bucket:select(hanged_bucket_id), {})
        end)
    end, {hanged_bucket_id})
    g.replica_1_a:exec(function(bucket_id)
        t.helpers.retrying({timeout = 10}, function()
            t.assert_equals(box.space._bucket:select(bucket_id)[1].status,
                            'active')
        end)
    end, {hanged_bucket_id})
end

rebalancer_recovery_group.test_rebalancer_routes_logging = function(g)
    local moved_bucket_from_2 = vtest.storage_first_bucket(g.replica_2_a)
    local moved_bucket_from_3 = vtest.storage_first_bucket(g.replica_3_a)
    move_bucket(g.replica_2_a, g.replica_1_a, moved_bucket_from_2)
    move_bucket(g.replica_3_a, g.replica_1_a, moved_bucket_from_3)
    g.replica_1_a:exec(function()
        ivshard.storage.rebalancer_wakeup()
    end)
    t.helpers.retrying({timeout = 60}, function()
        t.assert(g.replica_1_a:grep_log(
            'Apply rebalancer routes with 1 workers'))
        end)
    t.assert(g.replica_1_a:grep_log('Move 1 bucket'))
    local route_1_to_2 = string.format('from %s to %s',
                                       g.replica_1_a:replicaset_uuid(),
                                       g.replica_2_a:replicaset_uuid())
    local route_1_to_3 = string.format('from %s to %s',
                                       g.replica_1_a:replicaset_uuid(),
                                       g.replica_3_a:replicaset_uuid())
    t.assert(g.replica_1_a:grep_log(route_1_to_2))
    t.assert(g.replica_1_a:grep_log(route_1_to_3))
    move_bucket(g.replica_1_a, g.replica_2_a, moved_bucket_from_2)
    move_bucket(g.replica_1_a, g.replica_3_a, moved_bucket_from_3)
end

rebalancer_recovery_group.test_no_log_spam_when_buckets_no_active = function(g)
    local replicaset_2_uuid = g.replica_2_a:replicaset_uuid()
    g.replica_2_a:stop()
    g.replica_1_a:exec(function()
        rawset(_G, 'old_rebalancer_interval', ivconst.REBALANCER_WORK_INTERVAL)
        rawset(_G, 'old_rebalancer_timeout',
        ivconst.REBALANCER_GET_STATE_TIMEOUT)
        ivconst.REBALANCER_WORK_INTERVAL = 0.01
        ivconst.REBALANCER_GET_STATE_TIMEOUT = 0.01
    end)
    local rs_not_connected_log = g.replicaset_not_connected_pattern ..
                                 string.format(g.replcaset_not_connected_msg,
                                               replicaset_2_uuid)
    test_only_one_record_appears_in_logs(g.replica_1_a, rs_not_connected_log,
                                         g.rebalancer_wait_interval * 2)
    g.replica_1_a:exec(function()
        ivconst.REBALANCER_WORK_INTERVAL = _G.old_rebalancer_interval
        ivconst.REBALANCER_GET_STATE_TIMEOUT = _G.old_rebalancer_timeout
    end)
    g.replica_2_a:start()
end
