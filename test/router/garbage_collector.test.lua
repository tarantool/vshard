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
lua_gc = require('vshard.lua_gc')
cfg.collect_lua_garbage = true
vshard.router.cfg(cfg)
lua_gc.internal.bg_fiber ~= nil
-- Check that `collectgarbage()` was really called.
a = setmetatable({}, {__mode = 'v'})
a.k = {b = 100}
iterations = lua_gc.internal.iterations
lua_gc.internal.bg_fiber:wakeup()
while lua_gc.internal.iterations < iterations + 1 do fiber.sleep(0.01) end
a.k
lua_gc.internal.interval = 0.001
cfg.collect_lua_garbage = false
vshard.router.cfg(cfg)
lua_gc.internal.bg_fiber == nil
iterations = lua_gc.internal.iterations
fiber.sleep(0.01)
iterations == lua_gc.internal.iterations

test_run:switch("default")
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
