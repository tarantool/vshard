test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage()')
util.push_rs_filters(test_run)

--
-- gh-147: scheduler helps to share time fairly between incompatible but
-- necessary operations - storage refs and bucket moves. Refs are used for the
-- consistent map-reduce feature when the whole cluster can be scanned without
-- being afraid that some data may slip through requests on behalf of the
-- rebalancer.
--

_ = test_run:switch('storage_1_a')

vshard.storage.rebalancer_disable()
vshard.storage.bucket_force_create(1, 1500)

_ = test_run:switch('storage_2_a')
vshard.storage.rebalancer_disable()
vshard.storage.bucket_force_create(1501, 1500)

_ = test_run:switch('storage_1_a')
--
-- Bucket_send() uses the scheduler.
--
lsched = require('vshard.storage.sched')
assert(lsched.move_strike == 0)
assert(lsched.move_count == 0)
big_timeout = 1000000
big_timeout_opts = {timeout = big_timeout}
vshard.storage.bucket_send(1, util.replicasets[2], big_timeout_opts)
assert(lsched.move_strike == 1)
assert(lsched.move_count == 0)
wait_bucket_is_collected(1)

_ = test_run:switch('storage_2_a')
lsched = require('vshard.storage.sched')
--
-- Bucket_recv() uses the scheduler.
--
assert(lsched.move_strike == 1)
assert(lsched.move_count == 0)

--
-- When move is in progress, it is properly accounted.
--
_ = test_run:switch('storage_1_a')
vshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = true

_ = test_run:switch('storage_2_a')
big_timeout = 1000000
big_timeout_opts = {timeout = big_timeout}
ok, err = nil
assert(lsched.move_strike == 1)
_ = fiber.create(function()                                                     \
    ok, err = vshard.storage.bucket_send(1, util.replicasets[1],                \
                                         big_timeout_opts)                      \
end)
-- Strike increase does not mean the move finished. It means it was successfully
-- scheduled.
assert(lsched.move_strike == 2)

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return lsched.move_strike == 2 end)

--
-- Ref is not allowed during move.
--
small_timeout = 0.000001
lref = require('vshard.storage.ref')
ok, err = lref.add(0, 0, small_timeout)
assert(not ok)
err.message
-- Put it to wait until move is done.
ok, err = nil
_ = fiber.create(function() ok, err = lref.add(0, 0, big_timeout) end)
vshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = false

_ = test_run:switch('storage_2_a')
test_run:wait_cond(function() return ok or err end)
ok, err
assert(lsched.move_count == 0)
wait_bucket_is_collected(1)

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return ok or err end)
ok, err
assert(lsched.move_count == 0)
assert(lsched.ref_count == 1)
lref.del(0, 0)
assert(lsched.ref_count == 0)

--
-- Refs can't block sends infinitely. The scheduler must be fair and share time
-- between ref/move.
--
do_refs = true
ref_worker_count = 10
function ref_worker()                                                           \
    while do_refs do                                                            \
        lref.add(0, 0, big_timeout)                                             \
        fiber.sleep(small_timeout)                                              \
        lref.del(0, 0)                                                          \
    end                                                                         \
    ref_worker_count = ref_worker_count - 1                                     \
end
-- Simulate many fibers doing something with a ref being kept.
for i = 1, ref_worker_count do fiber.create(ref_worker) end
assert(lref.count > 0)
assert(lsched.ref_count > 0)
-- Ensure it passes with default opts (when move is in great unfairness). It is
-- important. Because moves are expected to be much longer than refs, and must
-- not happen too often with ref load in progress. But still should eventually
-- be processed.
bucket_count = 100
bucket_id = 1
bucket_worker_count = 5
function bucket_worker()                                                        \
    while bucket_id <= bucket_count do                                          \
        local id = bucket_id                                                    \
        bucket_id = bucket_id + 1                                               \
        assert(vshard.storage.bucket_send(id, util.replicasets[2]))             \
    end                                                                         \
    bucket_worker_count = bucket_worker_count - 1                               \
end
-- Simulate many rebalancer fibers like when max_sending is increased.
for i = 1, bucket_worker_count do fiber.create(bucket_worker) end
test_run:wait_cond(function() return bucket_worker_count == 0 end)

do_refs = false
test_run:wait_cond(function() return ref_worker_count == 0 end)
assert(lref.count == 0)
assert(lsched.ref_count == 0)

for i = 1, bucket_count do wait_bucket_is_collected(i) end

--
-- Refs can't block recvs infinitely.
--
do_refs = true
for i = 1, ref_worker_count do fiber.create(ref_worker) end

_ = test_run:switch('storage_2_a')
bucket_count = 100
bucket_id = 1
bucket_worker_count = 5
function bucket_worker()                                                        \
    while bucket_id <= bucket_count do                                          \
        local id = bucket_id                                                    \
        bucket_id = bucket_id + 1                                               \
        assert(vshard.storage.bucket_send(id, util.replicasets[1]))             \
    end                                                                         \
    bucket_worker_count = bucket_worker_count - 1                               \
end
for i = 1, bucket_worker_count do fiber.create(bucket_worker) end
test_run:wait_cond(function() return bucket_worker_count == 0 end)

_ = test_run:switch('storage_1_a')
do_refs = false
test_run:wait_cond(function() return ref_worker_count == 0 end)
assert(lref.count == 0)
assert(lsched.ref_count == 0)

_ = test_run:switch('storage_2_a')
for i = 1, bucket_count do wait_bucket_is_collected(i) end

_ = test_run:switch("default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
_ = test_run:cmd('clear filter')
