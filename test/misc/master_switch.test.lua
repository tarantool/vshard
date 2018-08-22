test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'misc')
test_run:create_cluster(REPLICASET_2, 'misc')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')

_ = test_run:cmd('stop server storage_1_b')
_ = test_run:switch('storage_1_a')
box.space._bucket:replace({1, vshard.consts.BUCKET.ACTIVE})
box.space.test:insert{1, 1, 1}

_ = test_run:switch('default')
_ = test_run:cmd('stop server storage_1_a')
_ = test_run:cmd('start server storage_1_b')
_ = test_run:switch('storage_1_b')
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_b].master = true
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_a].master = false
cfg.replication_connect_quorum = 1
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_b)
box.space._bucket:replace({1, vshard.consts.BUCKET.ACTIVE})
box.space.test:insert{1, 1, 2}

--
-- Test that the replication is broken - one insert must be newer,
-- then another, but here the replication stops. This situation
-- occurs, when a master is down, is repliced with another master,
-- and then becames master again.
--
_ = test_run:cmd('start server storage_1_a')
_ = test_run:switch('storage_1_a')
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_b].master = true
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_a].master = false
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_a)
box.space.test:get{1}
while not test_run:grep_log('storage_1_a', 'error applying row') do fiber.sleep(0.1) end

_ = test_run:switch('storage_1_b')
box.space.test:get{1}

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
