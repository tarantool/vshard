test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b' }
REPLICASET_2 = { 'box_2_a', 'box_2_b' }

test_run:create_cluster(REPLICASET_1, 'rebalancer')
test_run:create_cluster(REPLICASET_2, 'rebalancer')
util = require('lua_libs.util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')

--
-- gh-113: during rebalancing the recoverer can make a bucket be
-- ACTIVE on two replicasets. It is possible, if bucket_send
-- during marking a bucket to be SENDING does too long WAL write.
-- During the write the recovery fiber wakes up, checks that the
-- bucket does not exist on the destination replicaset and makes
-- it ACTIVE on the source. Then bucket_send finalizes WAL write
-- and sends data. If the data is applied too long on the
-- destination, bucket_send catches TIMEOUT error and exits. So
-- now the bucket is ACTIVE both on the source and the
-- destination.
--

test_run:switch('box_1_a')
_bucket = box.space._bucket
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('Total active bucket count is not equal to total', test_run)
vshard.storage.rebalancer_disable()
for i = 1, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('box_2_a')
_bucket = box.space._bucket
for i = 101, 200 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
vshard.storage.internal.errinj.ERRINJ_LONG_RECEIVE = true

test_run:switch('box_1_a')
vshard.storage.rebalancer_enable()
wait_rebalancer_state('The cluster is balanced ok', test_run)
errinj = box.error.injection
errinj.set('ERRINJ_WAL_DELAY', true)
cfg.sharding[rs[1]].weight = 0.5
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
--
-- The rebalancer tries to send a bucket. It made the bucket be
-- SENDING and started to persist that in WAL.
--
wait_rebalancer_state('Apply rebalancer routes', test_run)

--
-- During that the recovery fiber wakes up, sees SENDING bucket,
-- checks that it does not exist on the destination, and makes it
-- ACTIVE. It is wrong - the recovery fiber must not recovery
-- buckets that are beeing sent right now.
--
fiber = require('fiber')
while not test_run:grep_log('box_1_a', 'Finish bucket recovery step') do vshard.storage.recovery_wakeup() fiber.sleep(0.1) end
errinj.set('ERRINJ_WAL_DELAY', false)

--
-- WAL is working again. Bucket_send wakes up, and tries to send
-- the bucket. On the destination the bucket is applied too long,
-- and the bucket_send gets timeout error.
--
while not test_run:grep_log('box_1_a', 'Can not apply routes') do vshard.storage.rebalancer_wakeup() fiber.sleep(0.01) end
test_run:grep_log('box_1_a', 'Timeout exceeded') ~= nil

test_run:switch('box_2_a')
vshard.storage.internal.errinj.ERRINJ_LONG_RECEIVE = false

test_run:switch('box_1_a')
wait_rebalancer_state('The cluster is balanced ok', test_run)
_bucket.index.status:count{vshard.consts.BUCKET.ACTIVE}

test_run:switch('box_2_a')
_bucket.index.status:count{vshard.consts.BUCKET.ACTIVE}

test_run:switch('default')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
