test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'misc')
test_run:create_cluster(REPLICASET_2, 'misc')
util = require('util')
test_run:wait_fullmesh(REPLICASET_1)
test_run:wait_fullmesh(REPLICASET_2)
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')

--
-- gh-83: use fullmesh topology in vshard. Scenario of the test:
-- start writing a tuple on a master. Then switch master. After
-- switch the tuple still should be sent from the slave to a new
-- master.
--

_ = test_run:switch('storage_1_a')
-- Block new requests sending.
box.error.injection.set("ERRINJ_WAL_DELAY", true)
f = fiber.create(function() box.space.test:replace{1, 1} end)
box.space.test:select{}
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_b].master = true
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_a].master = false
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_a)
_ = test_run:switch('storage_1_b')
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_b].master = true
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_a].master = false
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_b)
box.space.test:select{}
_ = test_run:switch('storage_1_a')
box.error.injection.set("ERRINJ_WAL_DELAY", false)
_ = test_run:switch('storage_1_b')
while box.space.test:count() == 0 do fiber.sleep(0.1) end
box.space.test:select{}

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
