test_run = require('test_run').new()

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
test_run:cmd("create server router_1 with script='main/router_1.lua'")
test_run:cmd("start server router_1")
test_run:cmd('switch router_1')
util = require('util')

rs1 = vshard.router.internal.replicasets[replicasets[1]]
min_timeout = vshard.consts.CALL_TIMEOUT_MIN

--
-- Try read request with exection time = MIN_TIMEOUT * 4 + 0.3. It
-- must produce two attemts to read: two failed reads with timeout
-- MIN_TIMEOUT and success read with timeout MIN_TIMEOUT * 2.
--
util.collect_timeouts(rs1)
_ = rs1:callro('sleep', {min_timeout + 0.1}, {timeout = min_timeout * 4 + 0.5})
util.collect_timeouts(rs1)
for i = 1, 8 do rs1:callro('echo') end
util.collect_timeouts(rs1)

--
-- Test two timeouts increase per one request.
--
_ = rs1:callro('sleep', {min_timeout * 5}, {timeout = min_timeout * 100})
util.collect_timeouts(rs1)

_ = test_run:cmd("switch default")
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
