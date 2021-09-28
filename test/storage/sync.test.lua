test_run = require('test_run').new()
netbox = require('net.box')
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
engine = test_run:get_cfg('engine')
test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

_ = test_run:switch('storage_1_a')
s = box.schema.create_space('test')
_ = s:create_index('pk')

vshard.storage.sync(0.5)

_ = test_run:cmd('stop server storage_1_b')
s:replace{1}
box.info.replication[2].downstream.status
ok, err = vshard.storage.sync(0.5)
ok, err.code == box.error.TIMEOUT or err

_ = test_run:cmd('start server storage_1_b')

vshard.storage.sync(1)

_ = test_run:switch("default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
