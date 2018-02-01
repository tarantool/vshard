test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

test_run:switch('storage_1_a')
box.cfg.read_only
s = box.schema.create_space('test')
pk = s:create_index('pk')
function on_master_enable() s:replace{1} end
function on_master_disable() s:replace{2} end
vshard.storage.on_master_enable(on_master_enable)
vshard.storage.on_master_disable(on_master_disable)
s:select{}

test_run:switch('storage_1_b')
box.cfg.read_only
box.schema.create_space('test2')
fiber = require('fiber')
while box.space.test == nil do fiber.sleep(0.1) end
s = box.space.test
function on_master_enable() s:replace{3} end
function on_master_disable() if not box.cfg.read_only then s:replace{4} end end
vshard.storage.on_master_enable(on_master_enable)
vshard.storage.on_master_disable(on_master_disable)
-- Yes, there is no 3 or 4, because trigger is set after a replica
-- becames slave.
s:select{}

-- Check that after master change the read_only is updated, and
-- that triggers on master role switch can change spaces.
cfg.sharding[replicasets[1]].replicas[names.storage_1_b].master = true
cfg.sharding[replicasets[1]].replicas[names.storage_1_a].master = false
vshard.storage.cfg(cfg, names.storage_1_b)
box.cfg.read_only
s:select{}

test_run:switch('storage_1_a')
cfg.sharding[replicasets[1]].replicas[names.storage_1_b].master = true
cfg.sharding[replicasets[1]].replicas[names.storage_1_a].master = false
vshard.storage.cfg(cfg, names.storage_1_a)
box.cfg.read_only
fiber = require('fiber')
while s:count() ~= 3 do fiber.sleep(0.1) end
s:select{}

test_run:switch('storage_1_b')
-- Yes, there is no {2}, because replication source is unset
-- already.
s:select{}

test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
