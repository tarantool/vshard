test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b' }
REPLICASET_2 = { 'box_2_a', 'box_2_b' }

test_run:create_cluster(REPLICASET_1, 'rebalancer')
test_run:create_cluster(REPLICASET_2, 'rebalancer')
util = require('lua_libs.util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')

--
-- Test configuration: two replicasets. Rebalancer, according to
-- its implementation, works on a master with the smallest UUID -
-- here it is the box_1_a server.
-- Test follows the plan:
-- 1) Check the rebalancer does nothing until a cluster is
--    bootstraped;
-- 2) Rebalance slightly disbalanced cluster (send buckets from a
--    rebalancer host);
-- 3) Same as 2, but send buckets to a rebalancer host;
-- 4) Multiple times rebalanced cluster must became stable and
--    balanced after a time;
-- 5) Rebalancer can be turned off;
-- 6) Rebalancer can not start next rebalancing session, if a
--    previous one is not finished yet;
-- 7) Garbage collector cleans sent buckets;
-- 8) Rebalancer can be moved to another master, if a
--    configuration has changed.
--

test_run:switch('box_1_a')
fiber = require('fiber')
_bucket = box.space._bucket
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

--
-- Test the rebalancer on not bootstraped cluster.
-- (See point (1) in the test plan)
--
wait_rebalancer_state('Total active bucket count is not equal to total', test_run)

--
-- Fill the cluster with buckets saving the balance 100 buckets
-- per replicaset.
--
vshard.storage.rebalancer_disable()
cfg.rebalancer_max_receiving = 10
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
for i = 1, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('box_2_a')
_bucket = box.space._bucket
for i = 101, 200 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('box_1_a')
vshard.storage.rebalancer_enable()
wait_rebalancer_state("The cluster is balanced ok", test_run)

--
-- Send buckets to create a disbalance. Wait until the rebalancer
-- repairs it. (See point (2) in the test plan)
--
vshard.storage.rebalancer_disable()
test_run:switch('box_2_a')
for i = 1, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('box_1_a')
_bucket:truncate()
vshard.storage.rebalancer_enable()
vshard.storage.rebalancer_wakeup()
wait_rebalancer_state("Rebalance routes are sent", test_run)

wait_rebalancer_state('The cluster is balanced ok', test_run)
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})

test_run:switch('box_2_a')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})

--
-- Send buckets again, but now from a replicaset with no
-- rebalancer. Since the rebalancer is global and single per
-- cluster, it must detect disbalance.
-- (See point (3) in the test plan)
--
test_run:switch('box_1_a')
-- Set threshold to 300%
cfg.rebalancer_disbalance_threshold = 300
vshard.storage.rebalancer_disable()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
for i = 101, 200 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
test_run:switch('box_2_a')
_bucket:truncate()
test_run:switch('box_1_a')
-- The cluster is balanced with maximal disbalance = 100% < 300%,
-- set above.
vshard.storage.rebalancer_enable()
wait_rebalancer_state('The cluster is balanced ok', test_run)
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
-- Return 1%.
cfg.rebalancer_disbalance_threshold = 0.01
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('Rebalance routes are sent', test_run)
wait_rebalancer_state('The cluster is balanced ok', test_run)
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:min({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:max({vshard.consts.BUCKET.ACTIVE})

test_run:switch('box_2_a')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:min({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:max({vshard.consts.BUCKET.ACTIVE})

--
-- After multiple rebalancing a next rebalance must not change
-- anything.
-- (See point (4) in the test plan)
--
test_run:switch('box_1_a')
wait_rebalancer_state("The cluster is balanced ok", test_run)

--
-- Test disabled rebalancer.
-- (See point (5) in the test plan)
--
vshard.storage.rebalancer_disable()
wait_rebalancer_state("Rebalancer is disabled", test_run)

--
-- Test rebalancer does noting if not all buckets are active or
-- sent.
-- (See point (6) in the test plan)
--
vshard.storage.rebalancer_enable()
_bucket:update({150}, {{'=', 2, vshard.consts.BUCKET.RECEIVING}})
wait_rebalancer_state("Some buckets are not active", test_run)
_bucket:update({150}, {{'=', 2, vshard.consts.BUCKET.ACTIVE}})

--
-- Test garbage collector deletes sent buckets and their data.
-- (See point (7) in the test plan)
--
vshard.storage.rebalancer_disable()
for i = 91, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
space = box.space.test
space:replace{1, 91}
space:replace{2, 92}
space:replace{3, 93}
space:replace{4, 150}
space:replace{5, 151}

test_run:switch('box_2_a')
space = box.space.test
for i = 91, 100 do _bucket:delete{i} end

test_run:switch('box_1_a')
_bucket:get{91}.status
vshard.storage.rebalancer_enable()
vshard.storage.rebalancer_wakeup()
--
-- Now rebalancer makes a bucket SENT. After it the garbage
-- collector cleans it and deletes after a timeout.
--
while _bucket:get{91}.status ~= vshard.consts.BUCKET.SENT do fiber.sleep(0.01) end
while _bucket:get{91} ~= nil do fiber.sleep(0.1) end
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:min({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:max({vshard.consts.BUCKET.ACTIVE})

test_run:switch('box_2_a')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:min({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:max({vshard.consts.BUCKET.ACTIVE})
space:select{}

--
-- Test reconfiguration. On master change the rebalancer can move
-- to a new location.
-- (See point (8) in the test plan)
--
cfg.rebalancer_max_receiving = 10
switch_rs1_master()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)

test_run:switch('box_1_a')
switch_rs1_master()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

test_run:switch('box_1_b')
switch_rs1_master()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_b)

test_run:switch('box_2_b')
switch_rs1_master()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_b)

fiber = require('fiber')
while not test_run:grep_log('box_2_a', "rebalancer_f has been started") do fiber.sleep(0.1) end
while not test_run:grep_log('box_1_a', "Rebalancer location has changed") do fiber.sleep(0.1) end

--
-- gh-40: introduce custom replicaset weights. Weight allows to
-- move all buckets out of replicaset with weight = 0.
--
test_run:switch('box_1_a')
nullify_rs_weight()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)

test_run:switch('box_1_b')
nullify_rs_weight()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_b)

test_run:switch('box_2_b')
nullify_rs_weight()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_b)

test_run:switch('box_2_a')
nullify_rs_weight()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)

vshard.storage.rebalancer_wakeup()
_bucket = box.space._bucket
fiber = require('fiber')
test_run:cmd("setopt delimiter ';'")
while _bucket.index.status:count{vshard.consts.BUCKET.ACTIVE} ~= 200 do
	fiber.sleep(0.1)
	vshard.storage.rebalancer_wakeup()
end;
test_run:cmd("setopt delimiter ''");

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
