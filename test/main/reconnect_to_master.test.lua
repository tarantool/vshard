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

-- Break a connection to a master.
_ = test_run:cmd('stop server storage_1_a')

_ = test_run:switch('router_1')

reps = vshard.router.internal.replicasets
test_run:cmd("setopt delimiter ';'")
function is_disonnected()
	for i, rep in pairs(reps) do
		if rep.master.conn.state ~= 'active' then
			return true
		end
	end
	return false
end
test_run:cmd("setopt delimiter ''");

-- No master in replica set 1.
is_disonnected()

-- Return master.
_ = test_run:cmd('start server storage_1_a')
fiber = require('fiber')
max_iters = 1000
i = 0
while is_disonnected() and i < max_iters do i = i + 1 fiber.sleep(0.1) end

-- Master connection is active again.
is_disonnected()

_ = test_run:cmd("switch default")
test_run:cmd('stop server router_1')
test_run:cmd('cleanup server router_1')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
