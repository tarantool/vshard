test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'router')
test_run:create_cluster(REPLICASET_2, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
test_run:cmd("create server router_1 with script='router/router_1.lua'")
test_run:cmd("start server router_1")
--
-- gh-77: garbage collection options and Lua garbage collection.
--
test_run:switch('router_1')
fiber = require('fiber')
cfg.collect_lua_garbage = true
iters = vshard.consts.COLLECT_LUA_GARBAGE_INTERVAL / vshard.consts.DISCOVERY_INTERVAL
vshard.router.cfg(cfg)
a = setmetatable({}, {__mode = 'v'})
a.k = {b = 100}
for i = 1, iters + 1 do vshard.router.discovery_wakeup() fiber.sleep(0.01) end
a.k
cfg.collect_lua_garbage = false
vshard.router.cfg(cfg)
a.k = {b = 100}
for i = 1, iters + 1 do vshard.router.discovery_wakeup() fiber.sleep(0.01) end
a.k ~= nil

test_run:switch("default")
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
