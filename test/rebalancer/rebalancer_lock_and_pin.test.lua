test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b' }
REPLICASET_2 = { 'box_2_a', 'box_2_b' }
REPLICASET_3 = { 'box_3_a', 'box_3_b' }

test_run:create_cluster(REPLICASET_1, 'rebalancer')
test_run:create_cluster(REPLICASET_2, 'rebalancer')
util = require('lua_libs.util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')

--
-- A replicaset can be locked. Locked replicaset can neither
-- receive new buckets nor send own ones during rebalancing.
--

test_run:switch('box_2_a')
vshard.storage.bucket_force_create(101, 100)

test_run:switch('box_1_a')
vshard.storage.bucket_force_create(1, 100)

wait_rebalancer_state('The cluster is balanced ok', test_run)

--
-- Check that a weight = 0 will not do anything with a locked
-- replicaset. Moreover, this cluster is considered to be balanced
-- ok.
--
test_run:switch('box_2_a')
rs1_cfg = cfg.sharding[names.rs_uuid[1]]
rs1_cfg.lock = true
rs1_cfg.weight = 0
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)

test_run:switch('box_1_a')
rs1_cfg = cfg.sharding[names.rs_uuid[1]]
rs1_cfg.lock = true
rs1_cfg.weight = 0
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

wait_rebalancer_state('The cluster is balanced ok', test_run)
vshard.storage.is_locked()
info = vshard.storage.info().bucket
info.active
info.lock

--
-- Check that a locked replicaset not only blocks bucket sending,
-- but blocks receiving as well.
--
test_run:switch('box_2_a')
rs1_cfg.weight = 2
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)

test_run:switch('box_1_a')
rs1_cfg.weight = 2
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

wait_rebalancer_state('The cluster is balanced ok', test_run)
info = vshard.storage.info().bucket
info.active
info.lock

--
-- Vshard ensures that if a replicaset is locked, then it will not
-- allow to change its bucket set even if a rebalancer does not
-- know about a lock yet. For example, a locked replicaset could
-- be reconfigured a bit earlier.
--
test_run:switch('box_2_a')
rs1_cfg.lock = false
rs2_cfg = cfg.sharding[names.rs_uuid[2]]
rs2_cfg.lock = true
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)

test_run:switch('box_1_a')
rs1_cfg.lock = false
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

wait_rebalancer_state('Replicaset is locked', test_run)

rs2_cfg = cfg.sharding[names.rs_uuid[2]]
rs2_cfg.lock = true
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

wait_rebalancer_state('The cluster is balanced ok', test_run)

--
-- Check that when a new replicaset is added, buckets are spreaded
-- on non-locked replicasets as if locked replicasets and buckets
-- do not exist.
--

test_run:switch('default')
test_run:create_cluster(REPLICASET_3, 'rebalancer')
util.wait_master(test_run, REPLICASET_3, 'box_3_a')

test_run:switch('box_2_a')
rs1_cfg.lock = true
rs1_cfg.weight = 1
-- Return default configuration.
rs2_cfg.lock = false
rs2_cfg.weight = 0.5
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)

test_run:switch('box_3_a')
rs1_cfg = cfg.sharding[names.rs_uuid[1]]
rs1_cfg.lock = true
rs1_cfg.weight = 1
rs2_cfg = cfg.sharding[names.rs_uuid[2]]
rs2_cfg.weight = 0.5
vshard.storage.cfg(cfg, names.replica_uuid.box_3_a)

test_run:switch('box_1_a')
rs1_cfg.lock = true
rs1_cfg.weight = 1
rs2_cfg.lock = false
rs2_cfg.weight = 0.5
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

wait_rebalancer_state('The cluster is balanced ok', test_run)
info = vshard.storage.info().bucket
info.active
info.lock

test_run:switch('box_2_a')
vshard.storage.info().bucket.active

test_run:switch('box_3_a')
vshard.storage.info().bucket.active

--
-- Test bucket pinning. At first, return to the default
-- configuration.
--
test_run:switch('box_2_a')
rs1_cfg.lock = false
rs1_cfg.weight = 1
rs2_cfg.lock = false
rs2_cfg.weight = 1
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
test_run:switch('box_3_a')
rs1_cfg.lock = false
rs1_cfg.weight = 1
rs2_cfg.lock = false
rs2_cfg.weight = 1
vshard.storage.cfg(cfg, names.replica_uuid.box_3_a)
test_run:switch('box_1_a')
rs1_cfg.lock = false
rs1_cfg.weight = 1
rs2_cfg.lock = false
rs2_cfg.weight = 1
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('The cluster is balanced ok', test_run)
vshard.storage.info().bucket.active
status = box.space._bucket.index.status
first_id = status:select(vshard.consts.BUCKET.ACTIVE, {limit = 1})[1].id
-- Test that double pin is ok.
vshard.storage.bucket_pin(first_id)
box.space._bucket:get{first_id}.status
vshard.storage.bucket_pin(first_id)
box.space._bucket:get{first_id}.status

-- Test that double unpin after pin is ok.
vshard.storage.bucket_unpin(first_id)
box.space._bucket:get{first_id}.status
vshard.storage.bucket_unpin(first_id)
box.space._bucket:get{first_id}.status

-- Test that can not pin other buckets.
test_run:cmd("setopt delimiter ';'")
box.begin()
box.space._bucket:update({first_id}, {{'=', 2, vshard.consts.BUCKET.SENDING}})
ok, err = vshard.storage.bucket_pin(first_id)
assert(not ok and err)
ok, err = vshard.storage.bucket_unpin(first_id)
assert(not ok and err)
box.space._bucket:update({first_id}, {{'=', 2, vshard.consts.BUCKET.RECEIVING}})
ok, err = vshard.storage.bucket_pin(first_id)
assert(not ok and err)
ok, err = vshard.storage.bucket_unpin(first_id)
assert(not ok and err)
box.space._bucket:update({first_id}, {{'=', 2, vshard.consts.BUCKET.SENT}})
ok, err = vshard.storage.bucket_pin(first_id)
assert(not ok and err)
ok, err = vshard.storage.bucket_unpin(first_id)
assert(not ok and err)
box.space._bucket:update({first_id}, {{'=', 2, vshard.consts.BUCKET.GARBAGE}})
ok, err = vshard.storage.bucket_pin(first_id)
assert(not ok and err)
ok, err = vshard.storage.bucket_unpin(first_id)
assert(not ok and err)
box.rollback()
test_run:cmd("setopt delimiter ''");

--
-- Now pin some buckets and create such disbalance, that the
-- rebalancer will face with unreachability of the perfect
-- balance.
--
for i = 1, 60 do local ok, err = vshard.storage.bucket_pin(first_id - 1 + i) assert(ok) end
status:count({vshard.consts.BUCKET.PINNED})
rs1_cfg.weight = 0.5
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('The cluster is balanced ok', test_run)
-- The perfect balance is now 40-80-80, but on the replicaset 1
-- 60 buckets are pinned, so the actual balance is 60-70-70.
info = vshard.storage.info().bucket
info.active
info.pinned
test_run:switch('box_2_a')
vshard.storage.info().bucket.active

test_run:switch('box_3_a')
vshard.storage.info().bucket.active

test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_3)
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
