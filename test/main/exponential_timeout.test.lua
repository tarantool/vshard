test_run = require('test_run').new()
fiber = require('fiber')

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
test_run:wait_fullmesh(REPLICASET_1)
test_run:wait_fullmesh(REPLICASET_2)
test_run:cmd("create server router_1 with script='main/router_1.lua'")
test_run:cmd("start server router_1")
test_run:cmd('switch router_1')

rs1 = vshard.router.internal.replicasets[replicasets[1]]
rs2 = vshard.router.internal.replicasets[replicasets[2]]
test_run:cmd("setopt delimiter ';'")
function collect_timeouts(rs)
	local timeouts = {}
	for uuid, replica in pairs(rs.replicas) do
		table.insert(timeouts, {ok = replica.net_sequential_ok,
					fail = replica.net_sequential_fail,
					timeout = replica.net_timeout})
	end
	return timeouts
end;
test_run:cmd("setopt delimiter ''");

collect_timeouts(rs1)
collect_timeouts(rs2)

--
-- Test a case, when timeout is already minimal and tries to
-- decrease.
--
for i = 1, 9 do rs1:callrw('echo') end
collect_timeouts(rs1)
_ = rs1:callrw('echo')
collect_timeouts(rs1)

--
-- Test oscillation protection. When a timeout is increased, it
-- must not decrease until 10 success requests.
--
for i = 1, 2 do rs1:callrw('sleep', {vshard.consts.CALL_TIMEOUT_MIN + 0.1}) end
collect_timeouts(rs1)

for i = 1, 9 do rs1:callrw('echo') end
collect_timeouts(rs1)
_ = rs1:callrw('sleep', {vshard.consts.CALL_TIMEOUT_MIN * 2 + 0.1})
collect_timeouts(rs1)
-- Ok, because new timeout is increased twice.
_ = rs1:callrw('sleep', {vshard.consts.CALL_TIMEOUT_MIN * 1.8})
collect_timeouts(rs1)
for i = 1, 9 do rs1:callrw('echo') end
collect_timeouts(rs1)

_ = test_run:cmd("switch default")
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
