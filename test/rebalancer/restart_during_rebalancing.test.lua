test_run = require('test_run').new()

REPLICASET_1 = { 'fullbox_1_a', 'fullbox_1_b' }
REPLICASET_2 = { 'fullbox_2_a', 'fullbox_2_b' }
REPLICASET_3 = { 'fullbox_3_a', 'fullbox_3_b' }
REPLICASET_4 = { 'fullbox_4_a', 'fullbox_4_b' }

test_run:create_cluster(REPLICASET_1, 'rebalancer')
test_run:create_cluster(REPLICASET_2, 'rebalancer')
test_run:create_cluster(REPLICASET_3, 'rebalancer')
test_run:create_cluster(REPLICASET_4, 'rebalancer')
util = require('lua_libs.util')
util.wait_master(test_run, REPLICASET_1, 'fullbox_1_a')
util.wait_master(test_run, REPLICASET_2, 'fullbox_2_a')
util.wait_master(test_run, REPLICASET_3, 'fullbox_3_a')
util.wait_master(test_run, REPLICASET_4, 'fullbox_4_a')

test_run:cmd('create server router_1 with script="rebalancer/router_1.lua"')
test_run:cmd('start server router_1')
test_run:switch('router_1')
add_replicaset()
add_second_replicaset()
vshard.router.cfg(cfg)

--
-- Test on the storages restarts during intensive loading and
-- rebalancing.
-- Fullbox_1_a - rebalancer, which holds all buckets at the
-- beginning. Router sends read-write requests on all buckets.
-- Background fiber kills randomly selected servers (including
-- rebalancer). The goal of fullbox_1_a regardless of restarting
-- of all storages is to repair the balance.
--

test_run:switch('fullbox_1_a')
vshard.storage.rebalancer_disable()
log = require('log')
log.info(string.rep('a', 1000))
for i = 1, 200 do box.space._bucket:replace({i, vshard.consts.BUCKET.ACTIVE}) end

test_run:switch('router_1')
for i = 1, 4 do vshard.router.discovery_wakeup() end
util = require('rebalancer_utils')
util.start_loading()
fiber.sleep(2)

test_run:switch('default')
fiber = require('fiber')
log = require('log')
stop_killing = false
is_rebalancer_down = false
is_rebalancer_locked = false
test_run:cmd("setopt delimiter ';'")
function background_killer()
    while not stop_killing do
        local down_servers = {}
        if math.random(10) % 3 == 0 and not is_rebalancer_locked then
            is_rebalancer_down = true
            test_run:cmd('stop server fullbox_1_a')
            table.insert(down_servers, 'fullbox_1_a')
        end
        if math.random(10) % 3 == 0 then
            test_run:cmd('stop server fullbox_2_a')
            table.insert(down_servers, 'fullbox_2_a')
        end
        if math.random(10) % 3 == 0 then
            test_run:cmd('stop server fullbox_3_a')
            table.insert(down_servers, 'fullbox_3_a')
        end
        if math.random(10) % 3 == 0 then
            test_run:cmd('stop server fullbox_4_a')
            table.insert(down_servers, 'fullbox_4_a')
        end
        fiber.sleep(0.8)
        for _, server in pairs(down_servers) do
            test_run:cmd('start server '..server)
        end
        is_rebalancer_down = false
    end
end;
test_run:eval('fullbox_1_a', 'vshard.storage.rebalancer_enable()');
killer = fiber.create(background_killer);

is_balanced = false;
i = 0
repeat
    if not is_rebalancer_down then
        is_rebalancer_locked = true
        is_balanced = test_run:grep_log('fullbox_1_a', 'The cluster is '..
                                        'balanced ok') ~= nil
        if not is_balanced then
            test_run:eval('fullbox_1_a', 'vshard.storage.rebalancer_wakeup()')
        end
        is_rebalancer_locked = false
    end
    i = i + 1
    fiber.sleep(0.3)
until is_balanced;
test_run:cmd("setopt delimiter ''");

stop_killing = true
while killer:status() ~= 'dead' do fiber.sleep(0.1) end

test_run:switch('router_1')
-- Wait until all GC, recovery-discovery finish work.
start = fiber.time()
while vshard.router.info().bucket.available_rw ~= 200 do vshard.router.discovery_wakeup() fiber.sleep(0.1) end
fiber.sleep(10 - (fiber.time() - start))
info = vshard.router.info()
-- Do not show concrete timeouts. They are not stable.
test_run:cmd("setopt delimiter ';'")
for _, rs in pairs(info.replicasets) do
    rs.replica.network_timeout = 'number'
end;
test_run:cmd("setopt delimiter ''");
info
util.stop_loading()
util.check_loading_result()

test_run:switch('fullbox_1_a')
vshard.storage.info().bucket
vshard.storage.internal.buckets_to_recovery
check_consistency()
test_run:switch('fullbox_2_a')
vshard.storage.info().bucket
vshard.storage.internal.buckets_to_recovery
check_consistency()
test_run:switch('fullbox_3_a')
vshard.storage.info().bucket
vshard.storage.internal.buckets_to_recovery
check_consistency()
test_run:switch('fullbox_4_a')
vshard.storage.info().bucket
vshard.storage.internal.buckets_to_recovery
check_consistency()

test_run:switch('default')
test_run:cmd('stop server router_1')
test_run:cmd('cleanup server router_1')
test_run:drop_cluster(REPLICASET_4)
test_run:drop_cluster(REPLICASET_3)
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
