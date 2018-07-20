test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b' }
REPLICASET_2 = { 'box_2_a', 'box_2_b' }
REPLICASET_3 = { 'box_3_a', 'box_3_b' }

test_run:create_cluster(REPLICASET_1, 'rebalancer')
test_run:create_cluster(REPLICASET_2, 'rebalancer')
util = require('lua_libs.util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')

test_run:cmd('create server router_1 with script="rebalancer/router_1.lua"')
test_run:cmd('start server router_1')
test_run:switch('router_1')

--
-- Test case: start a router, which sends write and read queries.
-- During it
-- 1) add replicaset;
-- 2) remove replicaset in two steps - make it weight be 0 and then remove it
--    from config;
-- At the end of each step the router must be able to read all
-- data, that were written during the step execution.
--
test_run:switch('box_2_a')
for i = 1, 200 do box.space._bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
test_run:switch('box_1_a')
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
wait_rebalancer_state('The cluster is balanced ok', test_run)
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}

--
-- Test the first case of described above.
--
test_run:switch('router_1')
util = require('rebalancer_utils')
vshard.router.cfg(cfg)
vshard.router.discovery_wakeup()
util.start_loading()

test_run:switch('default')
test_run:create_cluster(REPLICASET_3, 'rebalancer')
util.wait_master(test_run, REPLICASET_3, 'box_3_a')
test_run:switch('box_1_a')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
fiber.sleep(0.5)
test_run:switch('box_1_b')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_b)
fiber.sleep(0.5)
test_run:switch('box_2_a')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
fiber.sleep(0.5)
test_run:switch('box_2_b')
add_replicaset()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_b)
fiber.sleep(0.5)

test_run:switch('router_1')
add_replicaset()
vshard.router.cfg(cfg)
fiber.sleep(0.5)

test_run:switch('box_1_a')
wait_rebalancer_state('The cluster is balanced ok', test_run)
#box.space._bucket.index.status:select{vshard.consts.BUCKET.ACTIVE}
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

--
-- Test a second case, when a replicaset is removed during
-- rebalancing and under loading.
--
-- At first, make replicaset weight = 0 to move all its buckets
-- out.
--
test_run:switch('router_1')
util.start_loading()
test_run:switch('box_3_a')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_3_a)
fiber.sleep(0.5)

test_run:switch('box_3_b')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_3_b)
fiber.sleep(0.5)

test_run:switch('box_2_a')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
fiber.sleep(0.5)

test_run:switch('box_2_b')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_b)
fiber.sleep(0.5)

test_run:switch('box_1_a')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
fiber.sleep(0.5)

test_run:switch('box_1_b')
remove_replicaset_first_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_b)
fiber.sleep(0.5)

test_run:switch('router_1')
remove_replicaset_first_stage()
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

-- When buckets are moved out from third replicaset, remove it
-- from config.
test_run:switch('router_1')
util.start_loading()
test_run:switch('box_2_a')
remove_replicaset_second_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_a)
fiber.sleep(0.5)

test_run:switch('box_2_b')
remove_replicaset_second_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_2_b)
fiber.sleep(0.5)

test_run:switch('box_1_a')
remove_replicaset_second_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_a)
fiber.sleep(0.5)

test_run:switch('box_1_b')
remove_replicaset_second_stage()
vshard.storage.cfg(cfg, names.replica_uuid.box_1_b)
fiber.sleep(0.5)

test_run:switch('default')
test_run:drop_cluster(REPLICASET_3)
test_run:switch('router_1')
remove_replicaset_second_stage()
vshard.router.cfg(cfg)
util.stop_loading()
util.check_loading_result()
alerts = vshard.router.info().alerts
for _, a in ipairs(alerts) do assert(a[1] == 'SUBOPTIMAL_REPLICA' or a[1] == 'UNKNOWN_BUCKETS') end

test_run:switch('default')
test_run:cmd('stop server router_1')
test_run:cmd('cleanup server router_1')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
