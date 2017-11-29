test_run = require('test_run').new()

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
test_run:wait_fullmesh(REPLICASET_1)
test_run:wait_fullmesh(REPLICASET_2)

test_run:switch('storage_1_a')
vshard.storage.wait_discovery()
test_run:switch('storage_1_b')
vshard.storage.wait_discovery()
test_run:switch('default')

REPLICASET_3 = {'storage_3_a'}
test_run:create_cluster(REPLICASET_3, 'main')
test_run:switch('storage_3_a')
vshard.storage.wait_discovery()
test_run:switch('default')

test_run:cmd('stop server storage_1_b')

test_run:switch('storage_1_a')
cfg.sharding[1][2] = nil
cfg.sharding[2][1].master = nil
cfg.sharding[2][2].master = true
cfg.sharding[3] = {{uri = "storage:storage@127.0.0.1:3306", name = 'storage_3_a', master = true}}
vshard.storage.cfg(cfg, 'storage_1_a')

test_run:switch('storage_2_a')
cfg.sharding[1][2] = nil
cfg.sharding[2][1].master = nil
cfg.sharding[2][2].master = true
cfg.sharding[3] = {{uri = "storage:storage@127.0.0.1:3306", name = 'storage_3_a', master = true}}
vshard.storage.cfg(cfg, 'storage_2_a')
vshard.storage.wait_discovery()

test_run:switch('storage_2_b')
cfg.sharding[1][2] = nil
cfg.sharding[2][1].master = nil
cfg.sharding[2][2].master = true
cfg.sharding[3] = {{uri = "storage:storage@127.0.0.1:3306", name = 'storage_3_a', master = true}}
vshard.storage.cfg(cfg, 'storage_2_b')
vshard.storage.wait_discovery()

test_run:switch('default')

REPLICASET_1 = {'storage_1_a'}
test_run:wait_fullmesh(REPLICASET_1)
test_run:wait_fullmesh(REPLICASET_2)
test_run:wait_fullmesh(REPLICASET_3)

-- Check correctness on each replicaset.
test_run:switch('storage_1_a')
vshard.storage.wait_discovery()
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication

test_run:switch('storage_2_a')
vshard.storage.wait_discovery()
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication

test_run:switch('storage_2_b')
vshard.storage.wait_discovery()
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication

test_run:switch('storage_3_a')
vshard.storage.wait_discovery()
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication

test_run:switch('default')

test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_3)
