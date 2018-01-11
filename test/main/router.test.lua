test_run = require('test_run').new()
test_run:cmd("push filter '.*/init.lua.*[0-9]+: ' to ''")
netbox = require('net.box')
fiber = require('fiber')

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
test_run:wait_fullmesh(REPLICASET_1)
test_run:wait_fullmesh(REPLICASET_2)
test_run:cmd("create server router_1 with script='main/router_1.lua'")
test_run:cmd("start server router_1")

replicaset1_uuid = test_run:eval('storage_1_a', 'box.info.cluster.uuid')[1]
replicaset2_uuid = test_run:eval('storage_2_a', 'box.info.cluster.uuid')[1]
test_run:cmd("push filter '"..replicaset1_uuid.."' to '<replicaset_1>'")
test_run:cmd("push filter '"..replicaset2_uuid.."' to '<replicaset_2>'")

_ = test_run:cmd("switch router_1")
-- gh-46: Ensure a cfg is not destroyed after router.cfg().
cfg.sharding ~= nil

util = require('util')

-- gh-24: log all connnect/disconnect events.
test_run:grep_log('router_1', 'connected to ')

--
-- Initial distribution
--
util.check_error(vshard.router.call, 1, 'read', 'echo', {123})
replicaset, err = vshard.router.bucket_discovery(1); return err == nil or err
vshard.router.bootstrap()

-- Second one should produce error
vshard.router.bootstrap()

--
-- gh-48: more precise error messages about bucket unavailability.
--
util.check_error(vshard.router.call, vshard.consts.BUCKET_COUNT + 1, 'read', 'echo', {123})
util.check_error(vshard.router.call, -1, 'read', 'echo', {123})
replicaset, err = vshard.router.bucket_discovery(1); return err == nil or err
replicaset, err = vshard.router.bucket_discovery(2); return err == nil or err

test_run:cmd('switch storage_2_a')
box.space._bucket:replace({1, vshard.consts.BUCKET.SENDING})
test_run:cmd('switch storage_1_a')
box.space._bucket:replace({1, vshard.consts.BUCKET.RECEIVING})
test_run:cmd('switch router_1')
-- Ok to read sending bucket.
vshard.router.call(1, 'read', 'echo', {123})
-- Not ok to write sending bucket.
util.check_error(vshard.router.call, 1, 'write', 'echo', {123})

test_run:cmd('switch storage_2_a')
box.space._bucket:replace({1, vshard.consts.BUCKET.ACTIVE})
test_run:cmd('switch storage_1_a')
box.space._bucket:delete({1})
test_run:cmd('switch router_1')

-- Check unavailability of master of a replicaset.
test_run:cmd('stop server storage_2_a')
util.check_error(vshard.router.call, 1, 'read', 'echo', {123})
test_run:cmd('start server storage_2_a')

--
-- gh-26: API to get netbox by bucket identifier.
--
vshard.router.route(vshard.consts.BUCKET_COUNT + 100)
util.check_error(vshard.router.route, 'asdfg')
util.check_error(vshard.router.route)
conn = vshard.router.route(1)
conn.state
-- Test missing master.
rs_uuid = 'ac522f65-aa94-4134-9f64-51ee384f1a54'
rs = vshard.router.internal.replicasets[rs_uuid]
master = rs.master
rs.master = nil
vshard.router.route(1)
rs.master = master
-- Test reconnect on bucker_route().
rs:disconnect()
conn = vshard.router.route(1)
conn:wait_connected()
conn.state

--
-- Function call
--

bucket_id = 1
test_run:cmd("setopt delimiter ';'")

customer = {
    customer_id = 1,
    name = "Customer 1",
    bucket_id = bucket_id,
    accounts = {
        {
            account_id = 10,
            name = "Credit Card",
            balance = 100,
        },
        {
            account_id = 11,
            name = "Debit Card",
            balance = 50,
        },
    }
}
test_run:cmd("setopt delimiter ''");

vshard.router.call(bucket_id, 'write', 'customer_add', {customer})
vshard.router.call(bucket_id, 'read', 'customer_lookup', {1})
vshard.router.call(bucket_id + 1, 'read', 'customer_lookup', {1}) -- nothing

--
-- Test errors from router call.
--
new_bid = vshard.consts.BUCKET_COUNT + 1
space_data = {{1000, {{1}, {2}}}}
-- Insert in a not existing space - it must return box.error.
vshard.router.call(bucket_id, 'write', 'vshard.storage.bucket_recv', {new_bid, 'from_uuid', space_data})

--
-- Monitoring
--

vshard.router.info().replicasets[1].master.state
vshard.router.info().replicasets[2].master.state

--
-- Configuration: inconsistency master=true on storage and routers
--
-- This test case flips masters in replicasets without changing
-- configuration on router and tests NON_MASTER response
--

-- Test the WRITE request
vshard.router.call(1, 'write', 'echo', { 'hello world' })

-- Shuffle masters
util.shuffle_masters(cfg)

-- Reconfigure storages
test_run:cmd("switch storage_1_a")
cfg.sharding = test_run:eval('router_1', 'return cfg.sharding')[1]
vshard.storage.cfg(cfg, names['storage_1_a'])

test_run:cmd("switch storage_1_b")
cfg.sharding = test_run:eval('router_1', 'return cfg.sharding')[1]
vshard.storage.cfg(cfg, names['storage_1_b'])

test_run:cmd("switch storage_2_a")
cfg.sharding = test_run:eval('router_1', 'return cfg.sharding')[1]
vshard.storage.cfg(cfg, names['storage_2_a'])

test_run:cmd("switch storage_2_b")
cfg.sharding = test_run:eval('router_1', 'return cfg.sharding')[1]
vshard.storage.cfg(cfg, names['storage_2_b'])

-- Test that the WRITE request doesn't work
test_run:cmd("switch router_1")
util.check_error(vshard.router.call, 1, 'write', 'echo', { 'hello world' })

-- Reconfigure router and test that the WRITE request does work
vshard.router.cfg(cfg)
vshard.router.call(1, 'write', 'echo', { 'hello world' })


_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET_2)

-- gh-24: log all connnect/disconnect events.
while test_run:grep_log('router_1', 'disconnected from ') == nil do fiber.sleep(0.1) end

test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:cmd('clear filter')
