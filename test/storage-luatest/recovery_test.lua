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
                    master = true
                },
                replica_1_b = {}
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
                replica_2_b = {},
            },
        },
        {
            replicas = {
                replica_3_a = {
                    master = true,
                },
                replica_3_b = {},
            },
        },
        {
            replicas = {
                replica_4_a = {
                    master = true,
                },
                replica_4_b = {},
            },
        },
    },
    bucket_count = 30,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_exec_each_master(g, function(engine)
        local s = box.schema.space.create('test', {
            engine = engine,
            format = {
                {'id', 'unsigned'},
                {'bucket_id', 'unsigned'},
            },
        })
        s:create_index('id', {parts = {'id'}})
        s:create_index('bucket_id', {
            parts = {'bucket_id'}, unique = false
        })
    end, {g.params.engine})
    vtest.cluster_wait_vclock_all(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.after_each(function(g)
    vtest.cluster_exec_each_master(g, function()
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
    end)
end)

--
-- Checks a base case of recovery service when bucket is hanged in "receiving"
-- state on the receiver node and the recovery service is launched on it. The
-- bucket should be restored as "receiving" -> "garbage" -> nil on this node.
--
test_group.test_recovery_of_bucket_from_receiving_to_garbage = function(g)
    g.replica_2_a:exec(function()
        -- This errinj helps to make the bucket hanged in "receiving" state
        -- on the receiver node and in "sending" state on the sennder node.
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = true
        -- We need to stop GC in order to have time to track "garbage" status.
        _G.bucket_gc_pause()
        _G.bucket_recovery_pause()
    end)
    -- Start a failed bucket transfer
    vtest.storage_wait_bucket_sync(g.replica_1_a)
    vtest.storage_wait_bucket_sync(g.replica_2_a)
    local bucket_id, generation = g.replica_1_a:exec(function(dest_id)
        local _bucket = box.space._bucket
        local bucket_id = _G.get_first_bucket()
        local bucket = _bucket:get(bucket_id)
        local generation = bucket.opts and bucket.opts.generation or 0
        ilt.assert_equals(bucket.status, 'active')
        ilt.assert_not(ivshard.storage.bucket_send(bucket_id, dest_id))
        bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'sending')
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        return bucket_id, generation
    end, {g.replica_2_a:replicaset_uuid()})
    -- Checks a bucket recovery: "receiving" -> "garbage" -> nil
    g.replica_2_a:exec(function(bucket_id, generation)
        local _bucket = box.space._bucket
        local bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'receiving')
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        _G.bucket_recovery_continue()
        t.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            bucket = _bucket:get(bucket_id)
            ilt.assert_equals(bucket.status, 'garbage')
        end)
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        _G.bucket_gc_continue()
        t.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(_bucket:get(bucket_id))
        end)
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
    end, {bucket_id, generation})
    g.replica_1_a:exec(function(bucket_id, generation)
        local bucket
        local _bucket = box.space._bucket
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            bucket = _bucket:get(bucket_id)
            ilt.assert_equals(bucket.status, 'active')
        end)
        ilt.assert_equals(bucket.opts.generation, generation + 1)
    end, {bucket_id, generation})
end

--
-- Checks a base case of recovery service when bucket is hanged in "sending"
-- state on the sender node and the recovery service is launched on it. The
-- bucket should be restored as "sending" -> "active" on this node.
--
test_group.test_recovery_of_bucket_from_sending_to_active = function(g)
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = true
    end)
    -- Start a failed bucket transfer
    vtest.storage_wait_bucket_sync(g.replica_1_a)
    vtest.storage_wait_bucket_sync(g.replica_2_a)
    local bucket_id = g.replica_1_a:exec(function(dest_id)
        local _bucket = box.space._bucket
        local bucket_id = _G.get_first_bucket()
        local bucket = _bucket:get(bucket_id)
        local generation = bucket.opts and bucket.opts.generation or 0
        ilt.assert_equals(bucket.status, 'active')
        ilt.assert_not(ivshard.storage.bucket_send(bucket_id, dest_id))
        bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'sending')
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            bucket = _bucket:get(bucket_id)
            ilt.assert_equals(bucket.status, 'active')
        end)
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        return bucket_id
    end, {g.replica_2_a:replicaset_uuid()})
    g.replica_2_a:exec(function(bucket_id)
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(box.space._bucket:get(bucket_id))
        end)
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
    end, {bucket_id})
end

--
-- Checks a base case of recovery service when bucket is hanged in "receiving"
-- state on the receiver node, in "sent" and the recovery service is launched
-- on it. The bucket should be restored as "receiving" -> "active" on this
-- node.
--
test_group.test_recovery_of_bucket_from_receiving_to_active = function(g)
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = true
    end)
    -- Start a failed bucket transfer
    vtest.storage_wait_bucket_sync(g.replica_1_a)
    vtest.storage_wait_bucket_sync(g.replica_2_a)
    local bucket_id, generation = g.replica_1_a:exec(function(dest_id)
        _G.bucket_gc_pause()
        local _bucket = box.space._bucket
        local bucket_id = _G.get_first_bucket()
        local bucket = _bucket:get(bucket_id)
        local generation = bucket.opts and bucket.opts.generation or 0
        ilt.assert_equals(bucket.status, 'active')
        ilt.assert_not(ivshard.storage.bucket_send(bucket_id, dest_id,
                       {chunk_timeout = 0.1}))
        bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'sent')
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        return bucket_id, generation
    end, {g.replica_2_a:replicaset_uuid()})
    -- Checks a desired bucket recovery: "receiving" -> "active"
    g.replica_2_a:exec(function(bucket_id, generation)
        local _bucket = box.space._bucket
        local bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'receiving')
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            bucket = _bucket:get(bucket_id)
            ilt.assert_equals(bucket.status, 'active')
        end)
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = false
    end, {bucket_id, generation})
    g.replica_1_a:exec(function(bucket_id)
        _G.bucket_gc_continue()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
end

--
-- A helper function which reproduces the main "doubled buckets" scenario which
-- is described in vshard RFC (619) (gh-214). Other test function uses it in
-- order to test recovery service when the remote bucket is missed and when the
-- remote bucket has greater generation than local bucket.
--
local function resend_hanging_bucket_template(bucket_id, node1, node2, node3)
    node2:exec(function()
        _G.bucket_recovery_pause()
        -- We introduce new errinj in order to simulate network lag.
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_DELAY = true
    end)
    -- Loose message during tranfering from node1 to node2.
    vtest.storage_wait_bucket_sync(node1)
    vtest.storage_wait_bucket_sync(node2)
    vtest.storage_wait_bucket_sync(node3)
    local generation = node1:exec(function(bucket_id, dest_id)
        _G.bucket_recovery_pause()
        local _bucket = box.space._bucket
        local bucket = _bucket:get(bucket_id)
        local generation = bucket.opts and bucket.opts.generation or 0
        ilt.assert_equals(bucket.status, 'active')
        -- Using negative timeout here is not possible, since the request
        -- can fail during connection establishing, which is not an option
        -- here, the request must be done for other instance to hang.
        ilt.assert_not(ivshard.storage.bucket_send(bucket_id, dest_id,
                       {chunk_timeout = 0.1}))
        bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'sending')
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        _G.bucket_recovery_continue()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            bucket = _bucket:get(bucket_id)
            ilt.assert_equals(bucket.status, 'active')
        end)
        ilt.assert_equals(bucket.opts.generation, generation + 1)
        return generation
    end, {bucket_id, node2:replicaset_uuid()})
    -- Check that the bucket hasn't arrived yet on node2.
    node2:exec(function(bucket_id)
        ilt.assert_not(box.space._bucket:get(bucket_id))
    end, {bucket_id})
    -- Transfer the bucket from node1 to node3.
    node1:exec(function(bucket_id, dest_id, generation)
        ilt.assert(ivshard.storage.bucket_send(bucket_id, dest_id))
        local _bucket = box.space._bucket
        local bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'sent')
        ilt.assert_equals(bucket.opts.generation, generation + 2)
    end, {bucket_id, node3:replicaset_uuid(), generation})
    -- Check that bucket successfully arrived at node3.
    node3:exec(function(bucket_id, generation)
        local _bucket = box.space._bucket
        local bucket = _bucket:get(bucket_id)
        ilt.assert_equals(bucket.status, 'active')
        ilt.assert_equals(bucket.opts.generation, generation + 2)
    end, {bucket_id, generation})
    -- Simulate network recovery. The old bucket received by replica_2_a.
    node2:exec(function(bucket_id, generation)
        local bucket
        local _bucket = box.space._bucket
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_DELAY = false
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            bucket = _bucket:get(bucket_id)
            ilt.assert(bucket)
            ilt.assert_equals(bucket.status, 'receiving')
        end)
        t.assert_equals(bucket.opts.generation, generation + 1)
    end, {bucket_id, generation})
end

test_group.test_recovery_of_remote_bucket_with_greater_gen = function(g)
    local bucket_id = vtest.storage_first_bucket(g.replica_1_a)
    g.replica_1_a:exec(function()
        _G.bucket_gc_pause()
    end)
    resend_hanging_bucket_template(
        bucket_id, g.replica_1_a, g.replica_2_a, g.replica_3_a)
    g.replica_1_a:exec(function(bucket_id)
        -- Sent to replica_3_a.
        ilt.assert_equals(box.space._bucket:get(bucket_id).status, 'sent')
    end, {bucket_id})
    g.replica_2_a:exec(function(bucket_id)
        _G.bucket_recovery_continue()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
    g.replica_1_a:exec(function(bucket_id)
        _G.bucket_gc_continue()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
    -- Restore balance.
    g.replica_3_a:exec(function(bucket_id, dest_id)
        ilt.assert(ivshard.storage.bucket_send(bucket_id, dest_id))
    end, {bucket_id, g.replica_1_a:replicaset_uuid()})
end

test_group.test_recovery_of_missed_bucket = function(g)
    local bucket_id = vtest.storage_first_bucket(g.replica_1_a)
    resend_hanging_bucket_template(
        bucket_id, g.replica_1_a, g.replica_2_a, g.replica_3_a)
    g.replica_1_a:exec(function(bucket_id)
        -- Sent to replica_3_a. Garbage collected.
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            t.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
    g.replica_2_a:exec(function(bucket_id)
        _G.bucket_recovery_continue()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
    -- Restore balance.
    g.replica_3_a:exec(function(bucket_id, dest_id)
        ilt.assert(ivshard.storage.bucket_send(bucket_id, dest_id))
    end, {bucket_id, g.replica_1_a:replicaset_uuid()})
end

test_group.test_send_several_times = function(g)
    local bucket_id = vtest.storage_first_bucket(g.replica_1_a)
    -- Bucket is sent from 1 to 2, hangs, resend to 3.
    resend_hanging_bucket_template(
        bucket_id, g.replica_1_a, g.replica_2_a, g.replica_3_a)
    g.replica_1_a:exec(function(bucket_id)
        -- Sent to replica_3_a. Garbage collected.
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            t.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
    -- Bucket is sent from 3 to 4, hangs, resend to 1.
    resend_hanging_bucket_template(
        bucket_id, g.replica_3_a, g.replica_4_a, g.replica_1_a)
    -- The only active bucket in cluster after these manipulations.
    g.replica_1_a:exec(function(bucket_id)
        ilt.assert_equals(box.space._bucket:get(bucket_id).status, 'active')
    end, {bucket_id})
    for _, r in ipairs({'replica_2_a', 'replica_3_a', 'replica_4_a'}) do
        g[r]:exec(function(bucket_id)
            _G.bucket_recovery_continue()
            ilt.helpers.retrying({timeout = iwait_timeout}, function()
                ivshard.storage.recovery_wakeup()
                ivshard.storage.garbage_collector_wakeup()
                ilt.assert_not(box.space._bucket:get(bucket_id))
            end)
        end, {bucket_id})
    end
end

--
-- 1. The rs1 sends the bucket to rs2, message is lost, bucket is recovered.
--    Generation is N on rs1.
-- 2. The bucket is sent from the rs1 to the rs3. Generation is N + 1 on rs1
--    and rs3.
-- 3. Master switches in the rs3, new node doesn't have the bucket at all.
-- 4. rs2 gets the message, makes the bucket RECEIVING with generation N, scans
--    masters only, doesn't find the bucket at all and recovers it to ACTIVE.
-- 5. New master of rs3 gets the change from the old master, two ACTIVE buckets.
--
test_group.test_recovery_with_master_sync = function(g)
    local bucket_id = vtest.storage_first_bucket(g.replica_1_a)
    resend_hanging_bucket_template(
        bucket_id, g.replica_1_a, g.replica_2_a, g.replica_3_a)
    -- Switch master in rs3.
    vtest.storage_stop(g.replica_3_a)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[3].replicas.replica_3_a.master = false
    new_cfg_template.sharding[3].replicas.replica_3_b.master = true
    local new_global_cfg = vtest.config_new(new_cfg_template)
    for _, r in ipairs({'replica_3_b', 'replica_2_a'}) do
        vtest.storage_cfg(g[r], new_global_cfg)
    end
    g.replica_2_a:exec(function()
        _G.bucket_recovery_continue()
    end)
    t.helpers.retrying({timeout = vtest.wait_timeout}, function()
        g.replica_2_a:exec(function() ivshard.storage.recovery_wakeup() end)
        t.assert(g.replica_2_a:grep_log('Error during searching.*' ..
            'has not synchronized yet'))
    end)
    g.replica_2_a:exec(function(bucket_id)
        local info = ivshard.storage.buckets_info(bucket_id)[bucket_id]
        ilt.assert_equals(info.status, ivconst.BUCKET.RECEIVING)
    end, {bucket_id})
    vtest.storage_start(g.replica_3_a, new_global_cfg)
    vtest.storage_wait_bucket_sync(g.replica_3_b)
    g.replica_2_a:exec(function(bucket_id)
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
    g.replica_1_a:exec(function(bucket_id)
        _G.bucket_gc_continue()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_not(box.space._bucket:get(bucket_id))
        end)
    end, {bucket_id})
    -- Restore balance.
    vtest.cluster_cfg(g, global_cfg)
    vtest.storage_wait_bucket_sync(g.replica_3_a)
    vtest.storage_wait_bucket_sync(g.replica_1_a)
    g.replica_3_a:exec(function(bucket_id, dest_id)
        ilt.assert(ivshard.storage.bucket_send(bucket_id, dest_id))
    end, {bucket_id, g.replica_1_a:replicaset_uuid()})
end
