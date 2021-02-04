test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'router')
test_run:create_cluster(REPLICASET_2, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')
util.push_rs_filters(test_run)
_ = test_run:cmd("create server router_1 with script='router/router_1.lua'")
-- Discovery should not interfere in some first tests.
_ = test_run:cmd("start server router_1 with args='discovery_disable'")

_ = test_run:switch("router_1")

--
-- gh-147: map-reduce.
--
big_timeout = 1000000
vshard.router.cfg(cfg)
vshard.router.bootstrap({timeout = big_timeout})

vshard.router.map_callrw('echo', {1, 2, 3}, {timeout = big_timeout})

_ = test_run:switch('default')
_ = test_run:cmd("stop server router_1")
_ = test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
_ = test_run:cmd('clear filter')