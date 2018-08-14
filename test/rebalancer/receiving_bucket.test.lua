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
s = box.schema.create_space('test3', {engine = 'vinyl'})
_ = s:create_index('pk')
_ = s:create_index('vbucket', {parts = {{2, 'unsigned'}}, unique = false})

_ = test_run:switch('box_1_a')
wait_rebalancer_state("The cluster is balanced ok", test_run)
s = box.schema.create_space('test3', {engine = 'vinyl'})
_ = s:create_index('pk')
_ = s:create_index('vbucket', {parts = {{2, 'unsigned'}}, unique = false})

for i = 1, 10000 do box.space.test:replace{i, 1, 1} box.space.test2:replace{i, 1, 2} box.space.test3:replace{i, 1, 3} end
box.snapshot()
box.space.test:count()
box.space.test2:count()
box.space.test3:count()

vshard.storage.bucket_send(1, util.replicasets[2], {timeout = 10})
box.space._bucket:get{1}

_ = test_run:switch('box_2_a')
box.space.test:count()
box.space.test2:count()
box.space.test3:count()
for i = 1, 10000 do assert(box.space.test:get{i}[3] == 1) assert(box.space.test2:get{i}[3] == 2) assert(box.space.test3:get{i}[3] == 3) end
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

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
