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
-- Test that a bucket consisting of multiple spaces of different
-- engines with big tuple count is sent ok.
--

_ = test_run:switch('box_1_a')
_bucket = box.space._bucket
for i = 1, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

_ = test_run:switch('box_2_a')
_bucket = box.space._bucket
for i = 101, 200 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
create_simple_space('test3', {engine = 'vinyl'})
create_simple_space('test4')
create_simple_space('test5', {engine = 'vinyl'})

_ = test_run:switch('box_1_a')
wait_rebalancer_state("The cluster is balanced ok", test_run)
create_simple_space('test3', {engine = 'vinyl'})
create_simple_space('test4')
create_simple_space('test5', {engine = 'vinyl'})

for i = 1, 10000 do box.space.test:replace{i, 1, 1} box.space.test2:replace{i, 1, 2} box.space.test3:replace{i, 1, 3} end
for i = 1, 500 do box.space.test4:replace{i, 1, 4} box.space.test5:replace{i, 1, 5} end
box.snapshot()
box.space.test:count()
box.space.test2:count()
box.space.test3:count()
box.space.test4:count()
box.space.test5:count()

vshard.storage.bucket_send(1, util.replicasets[2], {timeout = 10})
box.space._bucket:get{1}

_ = test_run:switch('box_2_a')
box.space.test:count()
box.space.test2:count()
box.space.test3:count()
box.space.test4:count()
box.space.test5:count()
for i = 1, 10000 do assert(box.space.test:get{i}[3] == 1) assert(box.space.test2:get{i}[3] == 2) assert(box.space.test3:get{i}[3] == 3) end
for i = 1, 500 do assert(box.space.test4:get{i}[3] == 4) assert(box.space.test5:get{i}[3] == 5) end
box.space._bucket:get{1}

--
-- Ensure the partially received bucket is correctly cleaned up.
--
_ = test_run:switch('box_1_a')
while box.space._bucket:get{1} do fiber.sleep(0.01) end
vshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = true
_ = test_run:switch('box_2_a')
vshard.storage.bucket_send(1, util.replicasets[1], {timeout = 10})
box.space._bucket:get{1}
_ = test_run:switch('box_1_a')
box.space._bucket:get{1}
while box.space._bucket:get{1} do fiber.sleep(0.01) end
vshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
_ = test_run:switch('box_2_a')
box.space._bucket:get{1}

--
-- gh-149: a bucket can be activated on two storages in the
-- following case: bucket_send sends a last message, it hangs in
-- the network. Bucket_send catches timeout error. Recovery takes
-- the bucket, gets from the destination its status: receiving and
-- not transferring now. Then the bucket is activated on the
-- source again. But now the first message reaches the destination
-- and activates the bucket here as well.
--
_ = test_run:switch('box_1_a')
vshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = true
_ = test_run:switch('box_2_a')
_, err = vshard.storage.bucket_send(101, util.replicasets[1], {timeout = 0.1})
err.trace = nil
err
box.space._bucket:get{101}
while box.space._bucket:get{101}.status ~= vshard.consts.BUCKET.ACTIVE do vshard.storage.recovery_wakeup() fiber.sleep(0.01) end
box.space._bucket:get{101}
_ = test_run:switch('box_1_a')
while _bucket:get{101} do fiber.sleep(0.01) end
vshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = false
fiber.sleep(0.1)
box.space._bucket:get{101}

--
-- gh-122 and gh-73: a bucket can be transferred not completely,
-- if it has a vinyl space, which has an active transaction before
-- the transfer is started.
--
_ = test_run:switch('box_2_a')
finish_long_thing = false
test_run:cmd("setopt delimiter ';'")
function do_long_thing()
    box.begin()
    box.space.test3:replace{100, 1, 'new'}
    while not finish_long_thing do
        fiber.sleep(0.01)
    end
    box.commit()
end;
test_run:cmd("setopt delimiter ''");
ret = nil
err = nil
f = fiber.create(function() vshard.storage.call(1, 'write', 'do_long_thing') end)
while f:status() ~= 'suspended' do fiber.sleep(0.01) end
vshard.storage.buckets_info(1)
f1 = fiber.create(function() ret, err = vshard.storage.bucket_send(1, util.replicasets[1], {timeout = 0.3}) end)
while f1:status() ~= 'suspended' do fiber.sleep(0.01) end
vshard.storage.buckets_info(1)
vshard.storage.bucket_refrw(1)
while f1:status() ~= 'dead' do fiber.sleep(0.01) end
ret, err
finish_long_thing = true
while f:status() ~= 'dead' do fiber.sleep(0.01) end
vshard.storage.buckets_info(1)
box.space.test3:select{100}
_ = test_run:switch('box_1_a')
box.space.test3:select{100}
-- Now the bucket is unreferenced and can be transferred.
_ = test_run:switch('box_2_a')
vshard.storage.bucket_send(1, util.replicasets[1], {timeout = 0.3})
vshard.storage.buckets_info(1)
while box.space._bucket:get{1} do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.01) end
vshard.storage.buckets_info(1)
_ = test_run:switch('box_1_a')
box.space._bucket:get{1}
box.space.test3:select{100}

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
