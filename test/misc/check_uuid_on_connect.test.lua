test_run = require('test_run').new()
fiber = require('fiber')

REPLICASET_1 = { 'bad_uuid_1_a', 'bad_uuid_1_b' }
REPLICASET_2 = { 'bad_uuid_2_a', 'bad_uuid_2_b' }

test_run:create_cluster(REPLICASET_1, 'misc')
test_run:create_cluster(REPLICASET_2, 'misc')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'bad_uuid_1_a')
util.wait_master(test_run, REPLICASET_2, 'bad_uuid_2_a')

test_run:switch('bad_uuid_1_a')
util = require('util')
vshard.storage.bucket_force_create(1)
-- Fail, because replicaset_1 sees not the actual replicaset_2's
-- master UUID.
res, err = vshard.storage.bucket_send(1, replicaset_uuid[2])
res, util.portable_error(err)
test_run:grep_log('bad_uuid_1_a', 'Mismatch server UUID on replica bad_uuid_2_a%(storage%@')
box.space._bucket:select{}
-- Bucket sending fails, but it remains 'sending'. It is because
-- we do not know was request executed or not before connection
-- was lost. Restore it to 'active' manually.
box.space._bucket:replace{1, vshard.consts.BUCKET.ACTIVE}

test_run:cmd('create server bad_uuid_router with script="misc/bad_uuid_router.lua", wait=True, wait_load=True')
test_run:cmd('start server bad_uuid_router')
test_run:switch('bad_uuid_router')
fiber = require('fiber')

-- Router failed to connect because of UUID mismatch.
while test_run:grep_log('bad_uuid_router', 'Mismatch server UUID on replica bad_uuid_2_a') == nil do fiber.sleep(0.1) end

--
-- Repair config and try start again. After successfull start,
-- break already created netbox connections by changing UUID with
-- no changing listened port.
--
test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
REPLICASET_2 = { 'bad_uuid_2_a_repaired', 'bad_uuid_2_b' }
test_run:cmd('create server bad_uuid_2_a_repaired with script="misc/bad_uuid_2_a_repaired.lua", wait=False, wait_load=False')
test_run:cmd('start server bad_uuid_2_a_repaired with wait_load=False, wait=False')
test_run:cmd('create server bad_uuid_2_b with script="misc/bad_uuid_2_b.lua", wait=False, wait_load=False')
test_run:cmd('start server bad_uuid_2_b with wait_load=False, wait=False')
util.wait_master(test_run, REPLICASET_2, 'bad_uuid_2_a_repaired')

test_run:switch('bad_uuid_1_a')
-- Send is ok - now UUID of bad_uuid_2_a is correct.
vshard.storage.bucket_send(1, replicaset_uuid[2])
-- Fill log with garbage to separate two 'Mismatch' messages.
require('log').info(string.rep('a', 1000))

-- Now start another UUID on the same port.
test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)
REPLICASET_2 = { 'bad_uuid_2_a', 'bad_uuid_2_b' }
test_run:cmd('create server bad_uuid_2_a with script="misc/bad_uuid_2_a.lua", wait=False, wait_load=False')
test_run:cmd('start server bad_uuid_2_a with wait=False, wait_load=False')
test_run:cmd('create server bad_uuid_2_b with script="misc/bad_uuid_2_b.lua", wait=False, wait_load=False')
test_run:cmd('start server bad_uuid_2_b with wait=False, wait_load=False')
util.wait_master(test_run, REPLICASET_2, 'bad_uuid_2_a')

test_run:switch('bad_uuid_1_a')
vshard.storage.bucket_force_create(2)
res, err = vshard.storage.bucket_send(2, replicaset_uuid[2])
res, util.portable_error(err)
-- Close existing connection on a first error and log it.
test_run:grep_log('bad_uuid_1_a', 'Mismatch server UUID on replica bad_uuid_2_a') ~= nil

test_run:switch('bad_uuid_router')
-- Can not discovery - UUID of bucket 1 replicaset is incorrect.
vshard.router.static.route_map[1] = nil
vshard.router.route(1)
-- Ok to work with correct replicasets.
vshard.router.route(2).uuid

_ = test_run:cmd("switch default")
test_run:cmd('stop server bad_uuid_router')
test_run:cmd('cleanup server bad_uuid_router')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
