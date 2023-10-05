test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b' }
REPLICASET_2 = { 'box_2_a', 'box_2_b' }

test_run:create_cluster(REPLICASET_1, 'rebalancer')
test_run:create_cluster(REPLICASET_2, 'rebalancer')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')
util.push_rs_filters(test_run)
--
-- gh-122: bucket_ref/refro/refrw/unref... is an API to do not
-- allow bucket transfer or GC during a request execution.
--
_ = test_run:switch('box_1_a')
vshard.storage.bucket_force_create(1, 100)
_ = test_run:switch('box_2_a')
vshard.storage.bucket_force_create(101, 100)
_ = test_run:switch('box_1_a')

vshard.storage.bucket_refro(1)
vshard.storage.buckets_info(1)

vshard.storage.bucket_refrw(1)
vshard.storage.buckets_info(1)

vshard.storage.bucket_unrefro(1)
vshard.storage.buckets_info(1)

-- When an RW ref exists, RO is taken with no _bucket lookup.
vshard.storage.bucket_refro(1)
vshard.storage.buckets_info(1)

vshard.storage.bucket_unrefro(1)
vshard.storage.bucket_unrefrw(1)
vshard.storage.buckets_info(1)

-- Test locks. For this open some RO requests and then send the
-- bucket. During and after sending new RW refs are not allowed.
-- RO requests are not allowed only after successfull transfer.
-- What is more, a bucket under RO ref can not be deleted.
f1 = fiber.create(function() vshard.storage.call(1, 'read', 'make_ref') end)
vshard.storage.buckets_info(1)
_ = test_run:switch('box_1_a')
vshard.storage.internal.errinj.ERRINJ_LAST_SEND_DELAY = true
f2 = fiber.create(function()                                                    \
    vshard.storage.bucket_send(1, util.replicasets[2])                          \
end)
test_run:wait_cond(function()                                                   \
    local i = vshard.storage.buckets_info(1)[1]                                 \
    return i.status == vshard.consts.BUCKET.SENDING                             \
end)
vshard.storage.buckets_info(1)
vshard.storage.bucket_ref(1, 'write')
vshard.storage.bucket_unref(1, 'write') -- Error, no refs.
vshard.storage.bucket_ref(1, 'read')
vshard.storage.bucket_unref(1, 'read')
-- Force GC to take an RO lock on the bucket now.
vshard.storage.buckets_info(1)
vshard.storage.internal.errinj.ERRINJ_LAST_SEND_DELAY = false
test_run:wait_cond(function() return f2:status() == 'dead' end)
test_run:wait_cond(function()                                                   \
    local i = vshard.storage.buckets_info(1)[1]                                 \
    return i.status == vshard.consts.BUCKET.SENT and i.ro_lock                  \
end)
vshard.storage.buckets_info(1)
vshard.storage.bucket_refro(1)
finish_refs = true
while f1:status() ~= 'dead' do fiber.sleep(0.01) end
wait_bucket_is_collected(1)
_ = test_run:switch('box_2_a')
vshard.storage.buckets_info(1)

--
-- Test that when bucket_send waits for rw == 0, it is waked up
-- immediately once it happened.
--
f1 = fiber.create(function() vshard.storage.call(1, 'write', 'make_ref') end)
vshard.storage.buckets_info(1)
f2 = fiber.create(function() vshard.storage.bucket_send(1, util.replicasets[1], {timeout = 0.3}) end)
while not vshard.storage.buckets_info(1)[1].rw_lock do fiber.sleep(0.01) end
fiber.sleep(0.2)
vshard.storage.buckets_info(1)
finish_refs = true
wait_bucket_is_collected(1)
_ = test_run:switch('box_1_a')
vshard.storage.buckets_info(1)

--
-- Rebalancer takes buckets starting from the minimal id. If a
-- bucket with that ID is locked, it should try another. The case
-- makes bucket with minimal ID locked for RW requests. The only
-- function taking the lock is bucket_send, so to test that a
-- manual bucket_send is called before rebalancer.
--
keep_lock = true
send_result = nil
-- To make first bucket_send keeping rw_lock it is necessary to
-- make rw ref counter > 0.
function keep_ref(id)								\
	vshard.storage.bucket_refrw(1)						\
	while keep_lock do							\
		fiber.sleep(0.01)						\
	end									\
	vshard.storage.bucket_unrefrw(1)					\
end
fiber_to_ref = fiber.create(keep_ref, 1)
while vshard.storage.buckets_info(1)[1].ref_rw ~= 1 do fiber.sleep(0.01) end

-- Now bucket_send on that bucket blocks.
function do_send(id)								\
	send_result = {								\
		vshard.storage.bucket_send(id, util.replicasets[2],		\
					   {timeout = 9999999})			\
	}									\
end
fiber_to_lock = fiber.create(do_send, 1)
while not vshard.storage.buckets_info(1)[1].rw_lock do fiber.sleep(0.01) end


cfg.sharding[util.replicasets[1]].weight = 99
cfg.sharding[util.replicasets[2]].weight = 101
cfg.rebalancer_disbalance_threshold = 0
vshard.storage.cfg(cfg, box.info.uuid)
wait_rebalancer_state('The cluster is balanced ok', test_run)

-- Cleanup after the test.
keep_lock = false
while not send_result do fiber.sleep(0.01) end
send_result
cfg.sharding[util.replicasets[1]].weight = nil
cfg.sharding[util.replicasets[2]].weight = nil
vshard.storage.cfg(cfg, box.info.uuid)
wait_rebalancer_state('The cluster is balanced ok', test_run)

--
-- Cancel during bucket_send. In that case all the locks should
-- be freed, obviously.
--
keep_lock = true
fiber_to_ref = fiber.create(keep_ref, 1)
while vshard.storage.buckets_info(1)[1].ref_rw ~= 1 do fiber.sleep(0.01) end

send_result = nil
fiber_to_lock = fiber.create(do_send, 1)
while not vshard.storage.buckets_info(1)[1].rw_lock do fiber.sleep(0.01) end

fiber_to_lock:cancel()
while not send_result do fiber.sleep(0.01) end
assert(not send_result[1])
util.portable_error(send_result[2])
vshard.storage.buckets_info(1)

-- Cleanup after the test.
wait_rebalancer_state('The cluster is balanced ok', test_run)

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
