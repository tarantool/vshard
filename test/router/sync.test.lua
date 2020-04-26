test_run = require('test_run').new()
test_run:cmd("push filter 'line: *[0-9]+' to 'line: <line>'")
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'router')
test_run:create_cluster(REPLICASET_2, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')
_ = test_run:cmd("create server router_1 with script='router/router_1.lua'")
_ = test_run:cmd("start server router_1")
_ = test_run:switch("router_1")
util = require('util')

vshard.router.bootstrap()

vshard.router.sync(-1)
res, err = vshard.router.sync(0)
util.portable_error(err)

--
-- gh-190: router should not ignore cfg.sync_timeout.
--
test_run:cmd('stop server storage_1_b')
test_run:switch('storage_1_a')
cfg.sync_timeout = 0.01
vshard.storage.cfg(cfg, box.info.uuid)

test_run:switch('router_1')
cfg.sync_timeout = 0.1
vshard.router.cfg(cfg)
start = fiber.time()
ok, err = vshard.router.sync()
ok, err ~= nil
-- Storage 1a has no 1b replica available. Its sync would fail in
-- ~0.01 seconds by timeout by default. But router should pass its
-- own sync_timeout - 0.1.
fiber.time() - start >= 0.1
cfg.sync_timeout = nil
vshard.router.cfg(cfg)

test_run:switch('storage_1_a')
cfg.sync_timeout = nil
vshard.storage.cfg(cfg, box.info.uuid)

test_run:switch('router_1')
test_run:cmd('start server storage_1_b')

test_run:cmd('stop server storage_1_a')
ok, err = nil, nil
-- Check that explicit timeout overwrites automatic ones.
for i = 1, 10 do ok, err = vshard.router.sync(0.01) end
ok, err ~= nil
test_run:cmd('start server storage_1_a')

_ = test_run:switch("default")
_ = test_run:cmd("stop server router_1")
_ = test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
