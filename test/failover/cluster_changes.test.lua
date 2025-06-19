test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b', 'box_1_c', 'box_1_d' }
REPLICASET_2 = { 'box_2_a', 'box_2_b', 'box_2_c' }
REPLICASET_3 = { 'box_3_a', 'box_3_b' }

test_run:create_cluster(REPLICASET_1, 'failover')
test_run:create_cluster(REPLICASET_2, 'failover')
test_run:create_cluster(REPLICASET_3, 'failover')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')
util.wait_master(test_run, REPLICASET_3, 'box_3_b')

test_run:cmd('create server router_1 with script="failover/router_1.lua"')
test_run:cmd('start server router_1')
test_run:switch('router_1')

--
-- In a case of configuration change all replicaset objects are
-- recreated, replica and replica candidate connections are
-- left in old objects, and are garbage collected. Test, that it
-- does not affect new replicasets, and failover uses new weights
-- and new topology.
--

--
-- First test case: reverse weights, when only replica exists,
-- and no replica candidate.
--
vshard.router.cfg(cfg)
info = vshard.router.info()
while #info.alerts ~= 1 do fiber.sleep(0.1) info = vshard.router.info() end
info.alerts

reverse_weights()
vshard.router.cfg(cfg)
info = vshard.router.info()
while #info.alerts ~= 1 do fiber.sleep(0.1) info = vshard.router.info() end
info

test_run:switch('box_1_a')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_1_b')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_1_c')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_1_d')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_2_a')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_2_b')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_2_c')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_3_a')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_3_b')
reverse_weights()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])

--
-- Test removal of candidate and replica.
--
test_run:switch('router_1')
remove_some_replicas()
vshard.router.cfg(cfg)
info = vshard.router.info()
while #info.alerts ~= 6 do fiber.sleep(0.1) info = vshard.router.info() end
info
test_run:switch('box_1_b')
remove_some_replicas()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_1_d')
remove_some_replicas()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])
test_run:switch('box_2_b')
remove_some_replicas()
vshard.storage.cfg(cfg, names.replica_uuid[NAME])

--
-- Test addition of new replicas.
--
test_run:switch('router_1')
add_some_replicas()
vshard.router.cfg(cfg)
info = vshard.router.info()
while #info.alerts ~= 1 do fiber.sleep(0.1) info = vshard.router.info() end
info

test_run:switch('default')
test_run:cmd('stop server router_1')
test_run:cmd('cleanup server router_1')

test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_3)
