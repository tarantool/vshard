test_run = require('test_run').new()

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

test_run:switch('storage_1_a')
vshard.storage.rebalancer_disable()
vshard.storage.bucket_force_create(1)
vshard.storage.bucket_force_create(2)
vshard.storage.bucket_force_create(3)

customer = box.space.customer
customer:replace{10, 1, 'user1'}
customer:replace{11, 2, 'user2'}
for i = 12, 1112 do customer:replace{i, 3, 'user'..tostring(i)} end

test_run:switch('storage_1_b')
customer = box.space.customer
-- Wait for data.
fiber = require('fiber')
while customer:count() ~= 1103 do fiber.sleep(0.1) end
box.space._bucket:select{}

test_run:switch('storage_1_a')
fiber = require('fiber')
box.space._bucket:replace{3, vshard.consts.BUCKET.SENT}
vshard.storage.bucket_force_drop(2)

-- Wait until garbage collector deletes data and 'sent' bucket.
while box.space._bucket:get{3} ~= nil do fiber.sleep(0.1) end
customer:select{}

test_run:switch('storage_1_b')
-- Ensure replica also has deleted garbage.
while box.space._bucket:get{3} ~= nil do fiber.sleep(0.1) end
customer:select{}

--
-- gh-77: garbage collection options and Lua garbage collection.
--
test_run:switch('storage_1_a')
fiber = require('fiber')
log = require('log')
cfg.collect_lua_garbage = true
vshard.storage.cfg(cfg, names.storage_1_a)
-- Create a weak reference to a able {b = 100} - it must be
-- deleted on the next GC.
a = setmetatable({}, {__mode = 'v'})
a.k = {b = 100}
iters = vshard.consts.COLLECT_LUA_GARBAGE_INTERVAL / vshard.consts.DEFAULT_GARBAGE_COLLECT_INTERVAL
-- Wait until Lua GC deletes a.k.
for i = 1, iters + 1 do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.01) end
a.k
cfg.collect_lua_garbage = false
vshard.storage.cfg(cfg, names.storage_1_a)
a.k = {b = 100}
for i = 1, iters + 1 do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.01) end
a.k ~= nil

test_run:switch('default')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
