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
vshard.storage.on_master_enable(on_master_enable)
vshard.storage.on_master_disable(on_master_disable)
master_enabled
master_disabled
-- Test the same about master disabling.
_ = test_run:switch('storage_1_b')
master_enabled = false
master_disabled = false
function on_master_enable() master_enabled = true end
function on_master_disable() master_disabled = true end
vshard.storage.on_master_enable(on_master_enable)
vshard.storage.on_master_disable(on_master_disable)
master_enabled
master_disabled

_ = test_run:switch('storage_1_a')

vshard.storage.info().replicasets[util.replicasets[1]] or vshard.storage.info()
vshard.storage.info().replicasets[util.replicasets[2]] or vshard.storage.info()
-- Try to call info on a replicaset with no master.
rs1 = vshard.storage.internal.replicasets[util.replicasets[1]]
saved_master = rs1.master
rs1.master = nil
vshard.storage.info()
rs1.master = saved_master

-- Sync API
vshard.storage.sync()
util.check_error(vshard.storage.sync, "xxx")
vshard.storage.sync(100500)

vshard.storage.buckets_info()
vshard.storage.bucket_force_create(1)
vshard.storage.buckets_info()
vshard.storage.bucket_force_create(1) -- error
vshard.storage.bucket_force_drop(1)

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
vshard.storage.info()
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
vshard.storage.bucket_recv(4, util.replicasets[2], {{1000, {{1}}}})
while box.space._bucket:get{4} do vshard.storage.recovery_wakeup() fiber.sleep(0.01) end

--
-- Bucket transfer
--

-- Transfer to unknown replicaset.
vshard.storage.bucket_send(1, 'unknown uuid')

-- Successful transfer.
vshard.storage.bucket_send(1, util.replicasets[2])
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

_ = test_run:switch("default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
_ = test_run:cmd('clear filter')
