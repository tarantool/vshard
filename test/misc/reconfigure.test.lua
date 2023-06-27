test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'misc')
test_run:create_cluster(REPLICASET_2, 'misc')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

test_run:cmd('create server router_1 with script="misc/router_1.lua", wait=True, wait_load=True')
test_run:cmd('start server router_1')

_ = test_run:switch('default')
cfg = dofile('localcfg.lua')
cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.storage_1_b] = nil
cfg.sharding[util.replicasets[2]].replicas[util.name_to_uuid.storage_2_a].master = nil
cfg.sharding[util.replicasets[2]].replicas[util.name_to_uuid.storage_2_b].master = true
cfg.sharding[util.replicasets[3]] = {replicas = {[util.name_to_uuid.storage_3_a] = {uri = "storage:storage@127.0.0.1:3306", name = 'storage_3_a', master = true}}}

REPLICASET_3 = {'storage_3_a'}
test_run:create_cluster(REPLICASET_3, 'misc')

-- test for unknown uuid
_ = test_run:switch('storage_1_a')
util.check_error(vshard.storage.cfg, cfg, 'unknow uuid')

--
-- Ensure that in a case of error storage internals are not
-- changed.
--
not vshard.storage.internal.collect_lua_garbage
vshard.storage.internal.sync_timeout
cfg.sync_timeout = 100
cfg.collect_lua_garbage = true
cfg.rebalancer_max_receiving = 1000
cfg.invalid_option = 'kek'
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_a)
not vshard.storage.internal.collect_lua_garbage
vshard.storage.internal.sync_timeout
vshard.storage.internal.rebalancer_max_receiving ~= 1000
cfg.sync_timeout = nil
cfg.collect_lua_garbage = nil
cfg.rebalancer_max_receiving = nil
cfg.invalid_option = nil

--
-- gh-59: provide trigger on master enable/disable.
--
disable_count = 0
enable_count = 0
function on_master_disable() disable_count = disable_count + 1 end
function on_master_enable() enable_count = enable_count + 1 end
_ = vshard.storage.on_master_disable(on_master_disable)
_ = vshard.storage.on_master_enable(on_master_enable)

-- test without master
for _, rs in pairs(cfg.sharding) do for _, s in pairs(rs.replicas) do s.master = nil end end
vshard.storage.cfg(cfg, box.info.uuid)

disable_count

_ = test_run:switch('default')

_ = test_run:cmd('stop server storage_1_b')

cmd = 'cfg.sharding = require"json".decode([[' .. require"json".encode(cfg.sharding) .. ']])'
_ = test_run:cmd('eval storage_1_a \'' .. cmd .. '\'')
_ = test_run:cmd('eval storage_2_a \'' .. cmd .. '\'')
_ = test_run:cmd('eval storage_2_b \'' .. cmd .. '\'')
_ = test_run:cmd('eval storage_3_a \'' .. cmd .. '\'')
_ = test_run:cmd('eval router_1 \'' .. cmd .. '\'')
_ = test_run:switch('storage_1_a')
vshard.storage.cfg(cfg, util.name_to_uuid.storage_1_a)
enable_count

_ = test_run:switch('storage_2_a')
vshard.storage.cfg(cfg, util.name_to_uuid.storage_2_a)

_ = test_run:switch('storage_2_b')
vshard.storage.cfg(cfg, util.name_to_uuid.storage_2_b)

_ = test_run:switch('router_1')
--
-- Ensure that in a case of error router internals are not
-- changed.
--
not vshard.router.static.collect_lua_garbage
cfg.collect_lua_garbage = true
cfg.invalid_option = 'kek'
vshard.router.cfg(cfg)
not vshard.router.static.collect_lua_garbage
cfg.invalid_option = nil
cfg.collect_lua_garbage = nil
vshard.router.cfg(cfg)

_ = test_run:switch('default')

REPLICASET_1 = {'storage_1_a'}
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.wait_master(test_run, REPLICASET_3, 'storage_3_a')

-- Check correctness on each replicaset.
_ = test_run:switch('storage_1_a')
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication
box.cfg.read_only
assert(#box.space._bucket:on_replace() == 1)

_ = test_run:switch('storage_2_a')
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication
box.cfg.read_only
--
-- gh-276: replica should have triggers. This is important for proper update of
-- caches and in future for discarding refs in scope of gh-173.
--
assert(#box.space._bucket:on_replace() == 1)

_ = test_run:switch('storage_2_b')
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication
box.cfg.read_only
assert(#box.space._bucket:on_replace() == 1)

_ = test_run:switch('storage_3_a')
info = vshard.storage.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris
box.cfg.replication
box.cfg.read_only
assert(#box.space._bucket:on_replace() == 1)

_ = test_run:switch('router_1')
info = vshard.router.info()
uris = {}
for k,v in pairs(info.replicasets) do table.insert(uris, v.master.uri) end
table.sort(uris)
uris

_ = test_run:switch('default')

_ = test_run:cmd('stop server router_1')
_ = test_run:cmd('cleanup server router_1')
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_3)
