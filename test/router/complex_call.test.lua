test_run = require('test_run').new()
REPLICASET_1 = { 'box_1_a', 'box_1_b', 'box_1_c' }
test_run:create_cluster(REPLICASET_1, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.map_evals(test_run, {REPLICASET_1}, 'bootstrap_storage(\'memtx\')')
_ = test_run:cmd("create server router_2 with script='router/router_2.lua'")
_ = test_run:cmd("start server router_2")
_ = test_run:switch("router_2")

vshard.router.bootstrap()
--
-- gh-157: introduce vshard.router.callre to prefer slaves to
-- execute a user defined function.
--
vshard.router.callre(1, 'echo', {'ok'})
vshard.router.call(1, {mode = 'read', prefer_replica = true}, 'echo', {'ok'})
rs = vshard.router.route(1)
res = rs:callre('echo', {'ok'})
res
_ = test_run:switch('box_1_b')
echo_count
echo_count = 0

_ = test_run:switch('router_2')
_ = test_run:cmd('stop server box_1_b')
vshard.router.callre(1, 'echo', {'ok'})
_ = test_run:switch('box_1_c')
echo_count
echo_count = 0

_ = test_run:switch('router_2')
_ = test_run:cmd('stop server box_1_c')
vshard.router.callre(1, 'echo', {'ok'})
_ = test_run:switch('box_1_a')
echo_count
echo_count = 0

_ = test_run:cmd('start server box_1_b')
_ = test_run:cmd('start server box_1_c')

_ = test_run:switch("default")
_ = test_run:cmd("stop server router_2")
_ = test_run:cmd("cleanup server router_2")
test_run:drop_cluster(REPLICASET_1)
