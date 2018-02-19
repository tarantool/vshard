test_run = require('test_run').new()

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'misc')
test_run:create_cluster(REPLICASET_2, 'misc')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

test_run:cmd('stop server storage_1_b')
test_run:switch('storage_1_a')
box.space._bucket:replace({1, vshard.consts.BUCKET.ACTIVE})
customer_add({customer_id = 1, bucket_id = 1, name = 'name_1', accounts = {}})

test_run:switch('default')
test_run:cmd('stop server storage_1_a')
test_run:cmd('start server storage_1_b')
test_run:switch('storage_1_b')
cfg.sharding[replicasets[1]].replicas[names.storage_1_b].master = true
cfg.sharding[replicasets[1]].replicas[names.storage_1_a].master = false
vshard.storage.cfg(cfg, names.storage_1_b)
box.space._bucket:replace({1, vshard.consts.BUCKET.ACTIVE})
customer_add({customer_id = 1, bucket_id = 1, name = 'name_2', accounts = {}})

--
-- Test that the replication is broken - one insert must be newer,
-- then another, but here the replication stops. This situation
-- occurs, when a master is down, is repliced with another master,
-- and then becames master again.
--
test_run:cmd('start server storage_1_a')
test_run:switch('storage_1_a')
fiber = require('fiber')
cfg.sharding[replicasets[1]].replicas[names.storage_1_b].master = true
cfg.sharding[replicasets[1]].replicas[names.storage_1_a].master = false
vshard.storage.cfg(cfg, names.storage_1_a)
customer_lookup(1)
while not test_run:grep_log('storage_1_a', 'error applying row') do fiber.sleep(0.1) end

test_run:switch('storage_1_b')
customer_lookup(1)

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
