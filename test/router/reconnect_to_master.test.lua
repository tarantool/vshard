test_run = require('test_run').new()
test_run:cmd("push filter '.*/init.lua.*[0-9]+: ' to ''")
netbox = require('net.box')
fiber = require('fiber')

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'router')
test_run:create_cluster(REPLICASET_2, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

--
-- gh-51: discovery must work with replicas.
-- Create 10 buckets and replicate them. Then start router and
-- down master. Router discovery fiber must use replica to find
-- buckets.
--
test_run:cmd('switch storage_1_a')
_bucket = box.space._bucket
for i = 1, 10 do _bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end
test_run:cmd('switch storage_1_b')
_bucket = box.space._bucket
fiber = require('fiber')
while _bucket:count() ~= 10 do fiber.sleep(0.1) end

test_run:cmd("create server router_1 with script='router/router_1.lua'")
test_run:cmd("start server router_1")

-- Break a connection to a master.
_ = test_run:cmd('stop server storage_1_a')

_ = test_run:switch('router_1')

reps = vshard.router.static.replicasets
test_run:cmd("setopt delimiter ';'")
function is_disconnected()
    for i, rep in pairs(reps) do
        if rep.master.conn == nil or rep.master.conn.state ~= 'active' then
            return true
        end
    end
    return false
end;
function count_known_buckets()
    local known_buckets = 0
    for _, id in pairs(vshard.router.static.route_map) do
        known_buckets = known_buckets + 1
    end
    return known_buckets
end;
test_run:cmd("setopt delimiter ''");
count_known_buckets()
fiber = require('fiber')
-- Use replica to find buckets.
while count_known_buckets() ~= 10 do vshard.router.discovery_wakeup() fiber.sleep(0.1) end

-- No master in replica set 1.
is_disconnected()

-- Wait until replica is connected to test alerts on unavailable
-- master.
fiber = require('fiber')
while vshard.router.static.replicasets[replicasets[1]].replica == nil do fiber.sleep(0.1) end
vshard.router.info()

-- Return master.
_ = test_run:cmd('start server storage_1_a')
fiber = require('fiber')
max_iters = 1000
i = 0
while is_disconnected() and i < max_iters do i = i + 1 fiber.sleep(0.1) end

-- Master connection is active again.
is_disconnected()

_ = test_run:cmd("switch default")
test_run:cmd('stop server router_1')
test_run:cmd('cleanup server router_1')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
