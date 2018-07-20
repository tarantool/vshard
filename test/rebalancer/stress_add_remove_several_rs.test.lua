test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b' }
REPLICASET_2 = { 'box_2_a', 'box_2_b' }
REPLICASET_3 = { 'box_3_a', 'box_3_b' }
REPLICASET_4 = { 'box_4_a', 'box_4_b' }

test_run:create_cluster(REPLICASET_1, 'rebalancer')
test_run:create_cluster(REPLICASET_2, 'rebalancer')
util = require('lua_libs.util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')

test_run:cmd('create server router_1 with script="rebalancer/router_1.lua"')
test_run:cmd('start server router_1')
test_run:switch('router_1')

--
-- Test the following case:
-- 1) start cluster and data loading;
-- 2) add replicaset, wait until rebalancing starts;
-- 3) add second replicaset when rebalancer is in progress;
-- 4) wait until all is finished;
-- 5) remove one replicaset (set weight = 0), wait until
--    rebalancing starts;
-- 6) remove second replicaset (set weight = 0).
--

test_run:switch('box_2_a')
cfg.rebalancer_max_receiving = 2
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
for i = 1, 200 do box.space._bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:switch('box_1_a')
cfg.rebalancer_max_receiving = 2
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('The cluster is balanced ok', test_run)

test_run:switch('router_1')
util = require('rebalancer_utils')
vshard.router.cfg(cfg)
vshard.router.discovery_wakeup()
util.start_loading()

-- At first, add one replicaset.

test_run:switch('default')
test_run:create_cluster(REPLICASET_3, 'rebalancer')
util.wait_master(test_run, REPLICASET_3, 'box_3_a')
test_run:switch('box_2_a')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
fiber.sleep(0.5)
test_run:switch('box_2_b')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_b)
fiber.sleep(0.5)
test_run:switch('box_1_b')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_b)
fiber.sleep(0.5)

test_run:switch('router_1')
add_replicaset()
vshard.router.cfg(cfg)
fiber.sleep(0.5)

test_run:switch('box_1_a')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('Rebalance routes are sent', test_run)

-- Now, add a second replicaset.

test_run:switch('default')
test_run:create_cluster(REPLICASET_4, 'rebalancer')
util.wait_master(test_run, REPLICASET_4, 'box_4_a')
test_run:switch('box_1_a')
add_second_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
fiber.sleep(0.5)
test_run:switch('box_2_a')
add_second_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
fiber.sleep(0.5)
test_run:switch('box_2_b')
add_second_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_b)
fiber.sleep(0.5)
test_run:switch('box_1_b')
add_second_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_b)
fiber.sleep(0.5)
test_run:switch('box_3_a')
add_second_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_3_a)
fiber.sleep(0.5)
test_run:switch('box_3_b')
add_second_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_3_b)
fiber.sleep(0.5)

test_run:switch('router_1')
add_second_replicaset()
vshard.router.cfg(cfg)
fiber.sleep(0.5)

test_run:switch('box_1_a')
wait_rebalancer_state('The cluster is balanced ok', test_run)
test_run:switch('router_1')
util.stop_loading()
util.check_loading_result()

test_run:switch('box_1_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()
test_run:switch('box_2_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()
test_run:switch('box_3_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()
test_run:switch('box_4_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()

--
-- Now reverse the actions above: remove replicaset, wait
-- rebalancing and remove second one.
--
test_run:switch('router_1')
util.start_loading()
test_run:switch('box_4_a')
remove_second_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_4_a)
fiber.sleep(0.5)
test_run:switch('box_3_a')
remove_second_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_3_a)
fiber.sleep(0.5)
test_run:switch('box_2_a')
remove_second_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
fiber.sleep(0.5)
test_run:switch('router_1')
remove_second_replicaset_first_stage()
vshard.router.cfg(cfg)
fiber.sleep(0.5)
test_run:switch('box_1_a')
remove_second_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('Rebalance routes are sent', test_run)
-- Rebalancing has been started - now remove second replicaset.
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('The cluster is balanced ok', test_run)

test_run:switch('box_4_a')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_4_a)
fiber.sleep(0.5)
test_run:switch('box_3_a')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_3_a)
fiber.sleep(0.5)
test_run:switch('box_2_a')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
fiber.sleep(0.5)
test_run:switch('router_1')
remove_replicaset_first_stage()
vshard.router.cfg(cfg)
fiber.sleep(0.5)
util.stop_loading()
util.check_loading_result()

test_run:switch('box_1_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()

test_run:switch('box_2_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()

test_run:switch('box_3_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()

test_run:switch('box_4_a')
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
check_consistency()

test_run:switch('default')
test_run:cmd('stop server router_1')
test_run:cmd('cleanup server router_1')
test_run:drop_cluster(REPLICASET_4)
test_run:drop_cluster(REPLICASET_3)
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
