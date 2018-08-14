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
-- Since 113 was fixed, now recovery does not work with a bucket
-- sending right now, so the issue is not so actual anymore.
--

test_run:switch('box_1_a')
_bucket = box.space._bucket
vshard.storage.cfg(cfg, util.name_to_uuid.box_1_a)
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
log.info(string.rep('a', 1000))
cfg.sharding[util.replicasets[1]].weight = 0.5
vshard.storage.cfg(cfg, util.name_to_uuid.box_1_a)
--
-- The rebalancer tries to send a bucket. It made the bucket be
-- SENDING and started to persist that in WAL.
--
if not test_run:grep_log('box_1_a', 'Apply rebalancer routes', 1000) then wait_rebalancer_state('Apply rebalancer routes', test_run) end

--
-- During that the recovery fiber wakes up, sees SENDING bucket,
-- checks that it does not exist on the destination, and makes it
-- ACTIVE. It is wrong - the recovery fiber must not recovery
-- buckets that are beeing sent right now.
--
fiber = require('fiber')
for i = 1, 10 do vshard.storage.recovery_wakeup() fiber.sleep(0.1) end
not test_run:grep_log('box_1_a', 'Finish bucket recovery step')
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

--
-- Test that it is possible to send multiple bucket at the same
-- time, even using the public bucket_send function.
--
box.error.injection.set('ERRINJ_WAL_DELAY', true)
_ = test_run:switch('box_1_a')
_bucket:get{35}
_bucket:get{36}
ret1 = nil
ret2 = nil
err1 = nil
err2 = nil
f1 = fiber.create(function() ret1, err1 = vshard.storage.bucket_send(35, util.replicasets[2]) end)
f2 = fiber.create(function() ret2, err2 = vshard.storage.bucket_send(36, util.replicasets[2]) end)
_ = test_run:switch('box_2_a')
box.error.injection.set('ERRINJ_WAL_DELAY', false)
_ = test_run:switch('box_1_a')
while f1:status() ~= 'dead' or f2:status() ~= 'dead' do fiber.sleep(0.001) end
ret1, err1
ret2, err2
_bucket:get{35}
_bucket:get{36}
-- Buckets became 'active' on box_2_a, but still are sending on
-- box_1_a. Wait until it is marked as garbage on box_1_a by the
-- recovery fiber.
while _bucket:get{35} ~= nil or _bucket:get{36} ~= nil do vshard.storage.recovery_wakeup() fiber.sleep(0.001) end
_ = test_run:switch('box_2_a')
_bucket:get{35}
_bucket:get{36}

--
-- Test that bucket_send is correctly handled by recovery. When
-- bucket_send is in progress, the recovery should not touch its
-- bucket.
--
_ = test_run:switch('box_2_a')
box.error.injection.set('ERRINJ_WAL_DELAY', true)
_ = test_run:switch('box_1_a')
f1 = fiber.create(function() ret1, err1 = vshard.storage.bucket_send(36, util.replicasets[2]) end)
_ = test_run:switch('box_2_a')
while not _bucket:get{36} do fiber.sleep(0.0001) end
_ = test_run:switch('box_1_a')
while _bucket:get{36} do vshard.storage.recovery_wakeup() vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end
_bucket:get{36}
_ = test_run:switch('box_2_a')
_bucket:get{36}
box.error.injection.set('ERRINJ_WAL_DELAY', false)
_ = test_run:switch('box_1_a')
while _bucket:get{36} and _bucket:get{36}.status == vshard.consts.BUCKET.ACTIVE do fiber.sleep(0.001) end

test_run:switch('default')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
