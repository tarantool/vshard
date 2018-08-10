test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
engine = test_run:get_cfg('engine')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'%s\')', engine)

_ = test_run:switch('storage_2_a')
vshard.storage.rebalancer_disable()
_ = test_run:switch('storage_1_a')
vshard.storage.bucket_force_create(1)
vshard.storage.bucket_force_create(2)
vshard.storage.bucket_force_create(3)

test = box.space.test
test:replace{10, 1}
test:replace{11, 2}
for i = 12, 1112 do test:replace{i, 3} end

_ = test_run:switch('storage_1_b')
test = box.space.test
-- Wait for data.
while test:count() ~= 1103 do fiber.sleep(0.1) end
box.space._bucket:select{}

_ = test_run:switch('storage_1_a')
box.space._bucket:replace{3, vshard.consts.BUCKET.SENT}
vshard.storage.bucket_force_drop(2)

-- Wait until garbage collector deletes data and 'sent' bucket.
while box.space._bucket:get{3} ~= nil do fiber.sleep(0.1) end
test:select{}

_ = test_run:switch('storage_1_b')
-- Ensure replica also has deleted garbage.
while box.space._bucket:get{3} ~= nil do fiber.sleep(0.1) end
test:select{}

--
-- gh-77: garbage collection options and Lua garbage collection.
--
_ = test_run:switch('storage_1_a')
lua_gc = require('vshard.lua_gc')
cfg.collect_lua_garbage = true
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_a)
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
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_a)
lua_gc.internal.bg_fiber == nil
iterations = lua_gc.internal.iterations
fiber.sleep(0.01)
iterations == lua_gc.internal.iterations

_ = test_run:switch('default')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
