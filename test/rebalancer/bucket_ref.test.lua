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
box.space._bucket:replace{1, vshard.consts.BUCKET.ACTIVE}

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
_ = test_run:switch('box_2_a')
vshard.storage.internal.errinj.ERRINJ_LONG_RECEIVE = true
_ = test_run:switch('box_1_a')
vshard.storage.bucket_send(1, util.replicasets[2])
vshard.storage.buckets_info(1)
vshard.storage.bucket_ref(1, 'write')
vshard.storage.bucket_unref(1, 'write') -- Error, no refs.
vshard.storage.bucket_ref(1, 'read')
vshard.storage.bucket_unref(1, 'read')
-- Force GC to take an RO lock on the bucket now.
vshard.storage.garbage_collector_wakeup()
vshard.storage.buckets_info(1)
while box.space._bucket:get{1}.status ~= vshard.consts.BUCKET.GARBAGE do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.01) end
vshard.storage.garbage_collector_wakeup()
vshard.storage.buckets_info(1)
vshard.storage.bucket_refro(1)
finish_refs = true
while f1:status() ~= 'dead' do fiber.sleep(0.01) end
vshard.storage.buckets_info(1)
while box.space._bucket:get{1} do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.01) end
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
while vshard.storage.buckets_info(1)[1].rw_lock do fiber.sleep(0.01) end
while box.space._bucket:get{1} do fiber.sleep(0.01) end
_ = test_run:switch('box_1_a')
vshard.storage.buckets_info(1)

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
