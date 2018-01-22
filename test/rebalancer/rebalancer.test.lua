test_run = require('test_run').new()

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
test_run:wait_fullmesh(REPLICASET_1)
test_run:wait_fullmesh(REPLICASET_2)

--
-- Test configuration: two replicasets. Rebalancer, according to
-- its implementation, works on a master with the smallest UUID -
-- here it is the storage_2_a server.
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

test_run:switch('storage_2_a')
fiber = require('fiber')
_bucket = box.space._bucket
log = require('log')
test_run:cmd("setopt delimiter ';'")
function wait_state(state)
	log.info(string.rep('a', 1000))
	vshard.storage.rebalancer_wakeup()
	while not test_run:grep_log("storage_2_a", state, 1000) do
		fiber.sleep(0.1)
		vshard.storage.rebalancer_wakeup()
	end
end;
test_run:cmd("setopt delimiter ''");

--
-- Test the rebalancer on not bootstraped cluster.
-- (See point (1) in the test plan)
--
wait_state('Total active bucket count is not equal to BUCKET_COUNT')

--
-- Fill the cluster with buckets saving the balance 100 buckets
-- per replicaset.
--
vshard.storage.rebalancer_disable()
vshard.consts.BUCKET_COUNT = 200
vshard.consts.REBALANCER_MAX_RECEIVING = 10
for i = 1, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('storage_1_a')
_bucket = box.space._bucket
for i = 101, 200 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('storage_2_a')
vshard.storage.rebalancer_enable()
wait_state("The cluster is balanced ok")

--
-- Send buckets to create a disbalance. Wait until the rebalancer
-- repairs it. (See point (2) in the test plan)
--
vshard.storage.rebalancer_disable()
test_run:switch('storage_1_a')
for i = 1, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('storage_2_a')
_bucket:truncate()
vshard.storage.rebalancer_enable()
vshard.storage.rebalancer_wakeup()
wait_state("Rebalance routes are sent")

wait_state('The cluster is balanced ok')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})

test_run:switch('storage_1_a')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})

--
-- Send buckets again, but now from a replicaset with no
-- rebalancer. Since the rebalancer is global and single per
-- cluster, it must detect disbalance.
-- (See point (3) in the test plan)
--
test_run:switch('storage_2_a')
-- Set threshold to 300%
vshard.consts.REBALANCER_DISBALANCE_THRESHOLD = 300
vshard.storage.rebalancer_disable()
for i = 101, 200 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
test_run:switch('storage_1_a')
_bucket:truncate()
test_run:switch('storage_2_a')
-- The cluster is balanced with maximal disbalance = 100% < 300%,
-- set above.
vshard.storage.rebalancer_enable()
wait_state('The cluster is balanced ok')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
-- Return 1%.
vshard.consts.REBALANCER_DISBALANCE_THRESHOLD = 0.01
wait_state('Rebalance routes are sent')
wait_state('The cluster is balanced ok')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:min({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:max({vshard.consts.BUCKET.ACTIVE})

test_run:switch('storage_1_a')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:min({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:max({vshard.consts.BUCKET.ACTIVE})

--
-- After multiple rebalancing a next rebalance must not change
-- anything.
-- (See point (4) in the test plan)
--
test_run:switch('storage_2_a')
wait_state("The cluster is balanced ok")

--
-- Test disabled rebalancer.
-- (See point (5) in the test plan)
--
vshard.storage.rebalancer_disable()
wait_state("Rebalancer is disabled")

--
-- Test rebalancer does noting if not all buckets are active or
-- sent.
-- (See point (6) in the test plan)
--
vshard.storage.rebalancer_enable()
_bucket:update({150}, {{'=', 2, vshard.consts.BUCKET.RECEIVING}})
wait_state("Some buckets are not active")
_bucket:update({150}, {{'=', 2, vshard.consts.BUCKET.ACTIVE}})

--
-- Test garbage collector deletes sent buckets and their data.
-- (See point (7) in the test plan)
--
vshard.storage.rebalancer_disable()
for i = 91, 100 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
space = box.schema.create_space('test', {format = {{'f1', 'unsigned'}, {'bucket_id', 'unsigned'}}})
pk = space:create_index('pk')
sk = space:create_index('bucket_id', {parts = {{2, 'unsigned'}}})
space:replace{1, 91}
space:replace{2, 92}
space:replace{3, 93}
space:replace{4, 150}
space:replace{5, 151}

test_run:switch('storage_1_a')
space = box.schema.create_space('test', {format = {{'f1', 'unsigned'}, {'bucket_id', 'unsigned'}}})
pk = space:create_index('pk')
sk = space:create_index('bucket_id', {parts = {{2, 'unsigned'}}})
for i = 91, 100 do _bucket:delete{i} end

test_run:switch('storage_2_a')
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

test_run:switch('storage_1_a')
_bucket.index.status:count({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:min({vshard.consts.BUCKET.ACTIVE})
_bucket.index.status:max({vshard.consts.BUCKET.ACTIVE})
space:select{}

--
-- Test reconfiguration. On master change the rebalancer can move
-- to a new location.
-- (See point (8) in the test plan)
--
cfg.sharding[replicasets[2]].replicas[names.storage_2_a].master = nil
cfg.sharding[replicasets[2]].replicas[names.storage_2_b].master = true
vshard.storage.cfg(cfg, names.storage_1_a)

test_run:switch('storage_2_a')
cfg.sharding[replicasets[2]].replicas[names.storage_2_a].master = nil
cfg.sharding[replicasets[2]].replicas[names.storage_2_b].master = true
vshard.storage.cfg(cfg, names.storage_2_a)

test_run:switch('storage_2_b')
vshard.consts.BUCKET_COUNT = 200
vshard.consts.REBALANCER_MAX_RECEIVING = 10
cfg.sharding[replicasets[2]].replicas[names.storage_2_a].master = nil
cfg.sharding[replicasets[2]].replicas[names.storage_2_b].master = true
vshard.storage.cfg(cfg, names.storage_2_b)

test_run:switch('storage_1_b')
cfg.sharding[replicasets[2]].replicas[names.storage_2_a].master = nil
cfg.sharding[replicasets[2]].replicas[names.storage_2_b].master = true
vshard.storage.cfg(cfg, names.storage_1_b)

fiber = require('fiber')
while not test_run:grep_log('storage_2_b', "Run rebalancer") do fiber.sleep(0.1) end
while not test_run:grep_log('storage_2_a', "Rebalancer location has changed") do fiber.sleep(0.1) end

--
-- gh-40: introduce custom replicaset weights. Weight allows to
-- move all buckets out of replicaset with weight = 0.
--
test_run:switch('storage_1_a')
cfg.sharding[replicasets[1]].weight = 0
vshard.storage.cfg(cfg, names.storage_1_a)

test_run:switch('storage_1_b')
cfg.sharding[replicasets[1]].weight = 0
vshard.storage.cfg(cfg, names.storage_1_b)

test_run:switch('storage_2_a')
cfg.sharding[replicasets[1]].weight = 0
vshard.storage.cfg(cfg, names.storage_2_a)

test_run:switch('storage_2_b')
cfg.sharding[replicasets[1]].weight = 0
vshard.storage.cfg(cfg, names.storage_2_b)

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
