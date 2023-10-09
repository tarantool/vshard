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
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'%s\')', engine)
util.push_rs_filters(test_run)

_ = test_run:switch("storage_2_a")
vshard.storage.rebalancer_disable()
_ = test_run:switch("storage_1_a")

-- Ensure the trigger is called right after setting, and does not
-- wait until reconfiguration.
master_enabled = false
master_disabled = false
function on_master_enable() master_enabled = true end
function on_master_disable() master_disabled = true end
_ = vshard.storage.on_master_enable(on_master_enable)
_ = vshard.storage.on_master_disable(on_master_disable)
master_enabled
master_disabled
-- Test the same about master disabling.
_ = test_run:switch('storage_1_b')
master_enabled = false
master_disabled = false
function on_master_enable() master_enabled = true end
function on_master_disable() master_disabled = true end
_ = vshard.storage.on_master_enable(on_master_enable)
_ = vshard.storage.on_master_disable(on_master_disable)
master_enabled
master_disabled

_ = test_run:switch('storage_1_a')

vshard.storage.info().replicasets[util.replicasets[1]] or vshard.storage.info()
vshard.storage.info().replicasets[util.replicasets[2]] or vshard.storage.info()
-- Try to call info on a replicaset with no master.
rs1 = vshard.storage.internal.replicasets[util.replicasets[1]]
saved_master = rs1.master
rs1.master = nil
assert(vshard.storage.internal.is_master)
vshard.storage.internal.is_master = false
vshard.storage.info()
vshard.storage.internal.is_master = true
rs1.master = saved_master

-- Sync API
vshard.storage.sync()
util.check_error(vshard.storage.sync, "xxx")
vshard.storage.sync(100500)

vshard.storage.buckets_info()
vshard.storage.bucket_force_create(1)
vshard.storage.buckets_info()
ok, err = vshard.storage.bucket_force_create(1)
assert(not ok and err.message:match("Duplicate key exists"))

_ = test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1}, false)

test_run:switch('storage_1_a')
vshard.storage.bucket_force_drop(1)
vshard.storage.sync()

_ = test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1}, true)

test_run:switch('storage_1_a')
vshard.storage.buckets_info()
vshard.storage.bucket_force_create(1)
vshard.storage.bucket_force_create(2)
_ = test_run:switch("storage_2_a")
vshard.storage.bucket_force_create(3)
vshard.storage.bucket_force_create(4)
_ = test_run:switch("storage_2_b")
box.cfg{replication_timeout = 0.01}
vshard.storage.info()
_ = test_run:cmd("stop server storage_2_a")
box.cfg{replication_timeout = 0.01}
alerts = vshard.storage.info().alerts
#alerts == 1 and alerts[1][1] or alerts
_ = test_run:cmd("start server storage_2_a")
_ = test_run:switch("storage_2_a")
fiber = require('fiber')
info = vshard.storage.info()
--
-- gh-144: do not warn about low redundancy if it is a desired
-- case.
--
while #info.alerts ~= 0 do fiber.sleep(0.1) info = vshard.storage.info() end
info
_ = test_run:cmd("stop server storage_2_b")
vshard.storage.info()
_ = test_run:cmd("start server storage_2_b")
_ = test_run:switch("storage_2_b")
vshard.storage.info()
_ = test_run:switch("storage_2_a")
vshard.storage.info()

_ = test_run:switch("storage_1_a")

_ = test_run:cmd("setopt delimiter ';'")
box.begin()
for id = 1, 8 do
    local bucket_id = id % 4
    box.space.test:insert({id, bucket_id})
    for id2 = id * 10, id * 10 + 2 do
        box.space.test2:insert({id2, id, bucket_id})
    end
end
box.commit();
_ = test_run:cmd("setopt delimiter ''");

box.space.test:select()
box.space.test2:select()

vshard.storage.bucket_collect(1)
vshard.storage.bucket_collect(2)

box.space.test:get{1}
vshard.storage.call(1, 'read', 'space_get', {'test', {1}})
vshard.storage.call(100500, 'read', 'space_get', {'test', {1}})

--
-- Test not existing uuid.
--
vshard.storage.bucket_recv(100, 'from_uuid', {{1000, {{1}}}})
--
-- Test not existing space in bucket data.
--
res, err = vshard.storage.bucket_recv(4, util.replicasets[2], {{1000, {{1}}}})
util.portable_error(err)
while box.space._bucket:get{4} do vshard.storage.recovery_wakeup() fiber.sleep(0.01) end
--
-- gh-275: detailed info when couldn't insert into a space.
--
res, err = vshard.storage.bucket_recv(                                          \
    4, util.replicasets[2], {{box.space.test.id, {{9, 4}, {10, 4}, {1, 4}}}})
assert(not res)
assert(err.space == 'test')
assert(err.bucket_id == 4)
assert(tostring(err.tuple) == '[1, 4]')
assert(err.reason:match('Duplicate key exists') ~= nil)
err = err.message
assert(err:match('bucket 4 data in space "test" at tuple %[1, 4%]') ~= nil)
assert(err:match('Duplicate key exists') ~= nil)
while box.space._bucket:get{4} do                                               \
    vshard.storage.recovery_wakeup() fiber.sleep(0.01)                          \
end
assert(box.space.test:get{9} == nil and box.space.test:get{10} == nil)

--
-- Bucket transfer
--

-- Transfer to unknown replicaset.
vshard.storage.bucket_send(1, 'unknown uuid')
-- gh-217: transfer to self.
vshard.storage.bucket_send(1, util.replicasets[1])

-- Successful transfer.
vshard.storage.bucket_send(1, util.replicasets[2])
wait_bucket_is_collected(1)
_ = test_run:switch("storage_2_a")
vshard.storage.buckets_info()
_ = test_run:switch("storage_1_a")
vshard.storage.buckets_info()

--
-- Part of gh-76: check that netbox old connections are reused on
-- reconfiguration.
--
old_connections = {}
connection_count = 0
old_replicasets = vshard.storage.internal.replicasets
_ = test_run:cmd("setopt delimiter ';'")
for _, old_replicaset in pairs(old_replicasets) do
	for uuid, old_replica in pairs(old_replicaset.replicas) do
		old_connections[uuid] = old_replica.conn
		if old_replica.conn then
			connection_count = connection_count + 1
		end
	end
end;
_ = test_run:cmd("setopt delimiter ''");
connection_count > 0
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_a)
new_replicasets = vshard.storage.internal.replicasets
new_replicasets ~= old_replicasets
_ = test_run:cmd("setopt delimiter ';'")
for _, new_replicaset in pairs(new_replicasets) do
	for uuid, new_replica in pairs(new_replicaset.replicas) do
		assert(old_connections[uuid] == new_replica.conn)
	end
end;
_ = test_run:cmd("setopt delimiter ''");

-- gh-114: Check non-dynamic option change during reconfigure.
non_dynamic_cfg = table.copy(cfg)
non_dynamic_cfg.bucket_count = require('vshard.consts').DEFAULT_BUCKET_COUNT + 1
util.check_error(vshard.storage.cfg, non_dynamic_cfg, util.name_to_uuid.storage_1_a)

-- Error during reconfigure process.
_, rs = next(vshard.storage.internal.replicasets)
rs:callro('echo', {'some_data'})
vshard.storage.internal.errinj.ERRINJ_CFG = true
old_internal = table.copy(vshard.storage.internal)
util.check_error(vshard.storage.cfg, cfg, util.name_to_uuid.storage_1_a)
vshard.storage.internal.errinj.ERRINJ_CFG = false
util.has_same_fields(old_internal, vshard.storage.internal)
_, rs = next(vshard.storage.internal.replicasets)
rs:callro('echo', {'some_data'})

--
-- Bucket count is calculated properly.
--
-- Cleanup after the previous tests.
_ = test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1, REPLICASET_2}, false)

_ = test_run:switch('storage_1_a')
buckets = vshard.storage.buckets_info()
for bid, _ in pairs(buckets) do vshard.storage.bucket_force_drop(bid) end
vshard.storage.sync()
_ = test_run:switch('storage_2_a')
buckets = vshard.storage.buckets_info()
for bid, _ in pairs(buckets) do vshard.storage.bucket_force_drop(bid) end
vshard.storage.sync()

_ = test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1, REPLICASET_2}, true)

_ = test_run:switch('storage_1_a')
assert(vshard.storage.buckets_count() == 0)
test_run:wait_lsn('storage_1_b', 'storage_1_a')
_ = test_run:switch('storage_1_b')
--
-- gh-276: bucket count cache should be properly updated on the replica nodes.
-- For that the replicas must also install on_replace trigger on _bucket space
-- to watch for changes.
--
assert(vshard.storage.buckets_count() == 0)

_ = test_run:switch('storage_1_a')
vshard.storage.bucket_force_create(1, 5)
assert(vshard.storage.buckets_count() == 5)
test_run:wait_lsn('storage_1_b', 'storage_1_a')
_ = test_run:switch('storage_1_b')
assert(vshard.storage.buckets_count() == 5)

_ = test_run:switch('storage_1_a')
vshard.storage.bucket_force_create(6, 5)
assert(vshard.storage.buckets_count() == 10)
test_run:wait_lsn('storage_1_b', 'storage_1_a')
_ = test_run:switch('storage_1_b')
assert(vshard.storage.buckets_count() == 10)

--
-- Bucket_generation_wait() registry function.
--
_ = test_run:switch('storage_1_a')
lstorage = require('vshard.registry').storage
ok, err = lstorage.bucket_generation_wait(-1)
assert(not ok and err.message)

ok, err = lstorage.bucket_generation_wait(0)
assert(not ok and err.message)

small_timeout = 0.000001
ok, err = lstorage.bucket_generation_wait(small_timeout)
assert(not ok and err.message)

ok, err = nil
big_timeout = 1000000
_ = fiber.create(function()                                                     \
    ok, err = lstorage.bucket_generation_wait(big_timeout)                      \
end)
fiber.sleep(small_timeout)
assert(not ok and not err)

_ = test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1}, false)

_ = test_run:switch('storage_1_a')
vshard.storage.bucket_force_drop(10)
vshard.storage.sync()

_ = test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1}, true)

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return ok or err end)
assert(ok)

-- Cancel should interrupt the waiting.
ok, err = nil
f = fiber.create(function()                                                     \
    ok, err = lstorage.bucket_generation_wait(big_timeout)                      \
end)
fiber.sleep(small_timeout)
assert(not ok and not err)
f:cancel()
_ = test_run:wait_cond(function() return ok or err end)
assert(not ok and err.message)

--
-- Bucket_are_all_rw() registry function.
--
assert(lstorage.bucket_are_all_rw())
vshard.storage.internal.errinj.ERRINJ_RECOVERY_PAUSE = true
-- Let it stuck in the errinj.
vshard.storage.recovery_wakeup()
vshard.storage.bucket_force_create(10)
box.space._bucket:update(10, {{'=', 2, vshard.consts.BUCKET.SENDING}})
assert(not lstorage.bucket_are_all_rw())
box.space._bucket:update(10, {{'=', 2, vshard.consts.BUCKET.ACTIVE}})
assert(lstorage.bucket_are_all_rw())
vshard.storage.internal.errinj.ERRINJ_RECOVERY_PAUSE = false

--
-- Internal info function.
--
vshard.storage._call('info')
_ = test_run:switch('storage_1_b')
vshard.storage._call('info')

--
-- gh-123, gh-298: storage auto-enable/disable depending on instance state.
--
_ = test_run:switch('default')
_ = test_run:cmd('stop server storage_1_a')
_ = test_run:cmd('start server storage_1_a with args="boot_before_cfg"')
-- We should wait for upstream on storage_1_b, as overwise we can get
-- IPROTO_VOTE from it, which tries to access box.cfg.read_only for ballot
-- generation. It won't be able to do that as at this point box.cfg will be
-- a function, and tarantool will fail with panic.
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
_ = test_run:switch('storage_1_a')
-- Leaving box.cfg() not called won't work because at 1.10 test-run somewhy
-- raises an error when try to start an instance without box.cfg(). It can only
-- be emulated.
old_cfg = box.cfg
assert(type(old_cfg) == 'table')
box.cfg = function(...) return old_cfg(...) end

ok, err = pcall(vshard.storage.call, 1, 'read', 'echo', {100})
assert(not ok and err.code == vshard.error.code.STORAGE_IS_DISABLED)
assert(err.message:match('box seems not to be configured') ~= nil)
box.cfg = old_cfg

-- Disabled until box is loaded.
vshard.storage.internal.errinj.ERRINJ_CFG_DELAY = true
f = fiber.create(vshard.storage.cfg, cfg, instance_uuid)
f:set_joinable(true)

old_info = box.info
box.info = {status = 'loading'}
ok, err = pcall(vshard.storage.call, 1, 'read', 'echo', {100})
assert(not ok and err.code == vshard.error.code.STORAGE_IS_DISABLED)
assert(err.message:match('instance status is "loading"') ~= nil)
box.info = old_info

-- Disabled until storage is configured.
test_run:wait_cond(function() return box.info.status == 'running' end)
ok, err = pcall(vshard.storage.call, 1, 'read', 'echo', {100})
assert(not ok and err.code == vshard.error.code.STORAGE_IS_DISABLED)
assert(err.message:match('storage is not configured') ~= nil)
vshard.storage.internal.errinj.ERRINJ_CFG_DELAY = false
f:join()

-- Enabled when all criteria are finally satisfied.
ok, res = vshard.storage.call(1, 'read', 'echo', {100})
assert(ok and res == 100)

--
-- Manual enable/disable.
--
vshard.storage.disable()
ok, err = pcall(vshard.storage.call, 1, 'read', 'echo', {100})
assert(not ok and err.code == vshard.error.code.STORAGE_IS_DISABLED)
assert(err.message:match('storage is disabled explicitly') ~= nil)

vshard.storage.enable()
ok, res = vshard.storage.call(1, 'read', 'echo', {100})
assert(ok and res == 100)

_ = test_run:switch("default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
_ = test_run:cmd('clear filter')
