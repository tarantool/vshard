test_run = require('test_run').new()

REPLICASET_1_1 = { 'storage_1_1_a', 'storage_1_1_b' }
REPLICASET_1_2 = { 'storage_1_2_a', 'storage_1_2_b' }
REPLICASET_2_1 = { 'storage_2_1_a', 'storage_2_1_b' }
REPLICASET_2_2 = { 'storage_2_2_a', 'storage_2_2_b' }

test_run:create_cluster(REPLICASET_1_1, 'multiple_routers')
test_run:create_cluster(REPLICASET_1_2, 'multiple_routers')
test_run:create_cluster(REPLICASET_2_1, 'multiple_routers')
test_run:create_cluster(REPLICASET_2_2, 'multiple_routers')
util = require('util')
util.wait_master(test_run, REPLICASET_1_1, 'storage_1_1_a')
util.wait_master(test_run, REPLICASET_1_2, 'storage_1_2_a')
util.wait_master(test_run, REPLICASET_2_1, 'storage_2_1_a')
util.wait_master(test_run, REPLICASET_2_2, 'storage_2_2_a')
util.map_evals(test_run, {REPLICASET_1_1, REPLICASET_1_2, REPLICASET_2_1, REPLICASET_2_2}, 'bootstrap_storage(\'memtx\')')

test_run:cmd("create server router_1 with script='multiple_routers/router_1.lua'")
test_run:cmd("start server router_1")

-- Configure default (static) router.
_ = test_run:cmd("switch router_1")
vshard.router.cfg(configs.cfg_1)
vshard.router.bootstrap()
_ = test_run:cmd("switch storage_1_2_a")
wait_rebalancer_state('The cluster is balanced ok', test_run)
_ = test_run:cmd("switch router_1")

vshard.router.call(1, 'write', 'do_replace', {{1, 1}})
vshard.router.call(1, 'read', 'do_select', {1})

-- Test that static router is just a router object under the hood.
vshard.router.static:route(1) == vshard.router.route(1)

-- Configure extra router.
router_2 = vshard.router.new('router_2', configs.cfg_2)
router_2:bootstrap()
_ = test_run:cmd("switch storage_2_2_a")
wait_rebalancer_state('The cluster is balanced ok', test_run)
_ = test_run:cmd("switch router_1")

router_2:call(1, 'write', 'do_replace', {{2, 2}})
router_2:call(1, 'read', 'do_select', {2})
-- Check that router_2 and static router serves different clusters.
#router_2:call(1, 'read', 'do_select', {1}) == 0

-- Create several routers to the same cluster.
routers = {}
for i = 3, 10 do routers[i] = vshard.router.new('router_' .. i, configs.cfg_2) end
routers[3]:call(1, 'read', 'do_select', {2})
-- Check that they have their own background fibers.
fiber_names = {}
for i = 3, 10 do                                                               \
    for _, rs in pairs(routers[i].replicasets) do                              \
        fiber_names[rs.worker.fiber:name()] = true                             \
    end                                                                        \
end
next(fiber_names) ~= nil
fiber = require('fiber')
for _, xfiber in pairs(fiber.info()) do fiber_names[xfiber.name] = nil end
next(fiber_names) == nil

-- Reconfigure one of routers do not affect the others.
routers[3]:cfg(configs.cfg_1)
routers[3]:call(1, 'read', 'do_select', {1})
#routers[3]:call(1, 'read', 'do_select', {2}) == 0
#routers[4]:call(1, 'read', 'do_select', {1}) == 0
routers[4]:call(1, 'read', 'do_select', {2})
routers[3]:cfg(configs.cfg_2)

-- Try to create router with the same name.
util = require('util')
util.check_error(vshard.router.new, 'router_2', configs.cfg_2)

-- Reload router module.
_, old_rs_1 = next(vshard.router.static.replicasets)
_, old_rs_2 = next(router_2.replicasets)
package.loaded['vshard.router'] = nil
vshard.router = require('vshard.router')
while not old_rs_1.is_outdated do fiber.sleep(0.01) end
while not old_rs_2.is_outdated do fiber.sleep(0.01) end
vshard.router.call(1, 'read', 'do_select', {1})
router_2:call(1, 'read', 'do_select', {2})
routers[5]:call(1, 'read', 'do_select', {2})

-- Self checker.
util.check_error(router_2.info)

_ = test_run:cmd("switch default")
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1_1)
test_run:drop_cluster(REPLICASET_1_2)
test_run:drop_cluster(REPLICASET_2_1)
test_run:drop_cluster(REPLICASET_2_2)
