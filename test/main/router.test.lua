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

--
-- Initial distribution
--
vshard.router.bucket_discovery(1)
vshard.router.bootstrap()
status, replicaset = vshard.router.bucket_discovery(1)
status
status, replicaset = vshard.router.bucket_discovery(2)
status

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
-- Monitoring
--

vshard.router.info().replicasets[1].master.state
vshard.router.info().replicasets[2].master.state

_ = test_run:cmd("switch default")

test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
test_run:cmd('clear filter')
