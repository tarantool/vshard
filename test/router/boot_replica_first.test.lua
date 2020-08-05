test_run = require('test_run').new()
REPLICASET_1 = { 'box_1_a', 'box_1_b', 'box_1_c' }
test_run:create_cluster(REPLICASET_1, 'router', {args = 'boot_before_cfg'})
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
_ = test_run:cmd("create server router with script='router/router_2.lua'")
_ = test_run:cmd("start server router")

--
-- gh-237: replica should be able to boot before master. Before the issue was
-- fixed, replica always tried to install a trigger on _bucket space even when
-- it was not created on a master yet - that lead to an exception in
-- storage.cfg. Now it should not install the trigger at all, because anyway it
-- is not needed on replica for anything.
--

test_run:switch('box_1_b')
vshard.storage.cfg(cfg, instance_uuid)
-- _bucket is not created yet. Will fail.
util.check_error(vshard.storage.call, 1, 'read', 'echo', {100})

test_run:switch('default')
util.map_evals(test_run, {REPLICASET_1}, 'bootstrap_storage(\'memtx\')')

test_run:switch('box_1_a')
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('box_1_b')
test_run:wait_lsn('box_1_b', 'box_1_a')
-- Fails, but gracefully. Because the bucket is not found here.
vshard.storage.call(1, 'read', 'echo', {100})
-- Should not have triggers.
#box.space._bucket:on_replace()

test_run:switch('router')
vshard.router.bootstrap()
vshard.router.callro(1, 'echo', {100})

test_run:switch("default")
test_run:cmd('stop server router')
test_run:cmd('delete server router')
test_run:drop_cluster(REPLICASET_1)
