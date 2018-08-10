test_run = require('test_run').new()

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'router')
test_run:create_cluster(REPLICASET_2, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')
_ = test_run:cmd("create server router_1 with script='router/router_1.lua'")
_ = test_run:cmd("start server router_1")
_ = test_run:switch('router_1')
util = require('util')

rs1 = vshard.router.static.replicasets[util.replicasets[1]]
min_timeout = vshard.consts.CALL_TIMEOUT_MIN

--
-- Try two read requests with exection time = MIN_TIMEOUT + 0.5.
-- It leads to increased network timeout.
--
util.collect_timeouts(rs1)
_ = rs1:callro('sleep', {min_timeout + 0.5}, {timeout = min_timeout})
_ = rs1:callro('sleep', {min_timeout + 0.5}, {timeout = min_timeout})
util.collect_timeouts(rs1)
for i = 1, 9 do rs1:callro('echo') end
util.collect_timeouts(rs1)

--
-- Ensure the luajit errors are not retried.
--
fiber = require('fiber')
start = fiber.time()
_, e = rs1:callro('raise_luajit_error', {}, {timeout = 10})
string.match(e.message, 'assertion')
fiber.time() - start < 1

start = fiber.time()
_, e = rs1:callro('raise_client_error', {}, {timeout = 5})
fiber.time() - start < 1
e.trace = nil
e

_, e = rs1:callro('sleep', {1}, {timeout = 0.0001})
e.trace = nil
e

--
-- Do not send multiple requests during timeout - it brokes long
-- polling requests.
--
_ = rs1:callro('sleep', {4}, {timeout = 100})
_

_ = test_run:switch("default")
_ = test_run:cmd("stop server router_1")
_ = test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
