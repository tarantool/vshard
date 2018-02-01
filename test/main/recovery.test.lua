test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

test_run:cmd("switch storage_1_a")
vshard.storage.rebalancer_disable()

rs2_uuid = replicasets[2]
-- Create buckets sending to rs2 and restart - recovery must
-- garbage some of them and activate others. Receiving buckets
-- must be garbaged on bootstrap.
_bucket = box.space._bucket

_bucket:replace{1, vshard.consts.BUCKET.SENDING, rs2_uuid}
_bucket:replace{2, vshard.consts.BUCKET.SENDING, rs2_uuid}
_bucket:replace{3, vshard.consts.BUCKET.RECEIVING, rs2_uuid}

test_run:cmd('switch storage_2_a')
_bucket = box.space._bucket
rs1_uuid = replicasets[1]
_bucket:replace{1, vshard.consts.BUCKET.RECEIVING, rs1_uuid}
_bucket:replace{2, vshard.consts.BUCKET.ACTIVE, rs1_uuid}
_bucket:replace{3, vshard.consts.BUCKET.SENDING, rs1_uuid}

test_run:cmd('stop server storage_1_a')
test_run:cmd('start server storage_1_a')
test_run:cmd('switch storage_1_a')
fiber = require('fiber')
vshard.storage.recovery_wakeup()
_bucket = box.space._bucket
_bucket:select{}
while _bucket:count() ~= 1 do fiber.sleep(0.1) end

--
-- Test a case, when a destination is down. The recovery fiber
-- must restore buckets, when the destination is up.
--
rs2_uuid = replicasets[2]
_bucket:replace{1, vshard.consts.BUCKET.SENDING, rs2_uuid}
test_run:cmd('switch storage_2_a')
_bucket:replace{1, vshard.consts.BUCKET.ACTIVE, rs1_uuid}
test_run:cmd('switch default')
test_run:cmd('stop server storage_2_a')
test_run:cmd('stop server storage_1_a')
test_run:cmd('start server storage_1_a')
test_run:cmd('switch storage_1_a')
_bucket = box.space._bucket
_bucket:select{}
for i = 1, 10 do vshard.storage.recovery_wakeup() end
_bucket:select{}
test_run:cmd('start server storage_2_a')
fiber = require('fiber')
while _bucket:count() ~= 0 do vshard.storage.recovery_wakeup() fiber.sleep(0.1) end
_bucket:select{}

test_run:cmd('switch storage_2_a')
_bucket = box.space._bucket
_bucket:select{}

test_run:cmd("switch default")

test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
