test_run = require('test_run').new()
test_run:cmd("create server router_1 with script='router/empty_cluster.lua'")
test_run:cmd("start server router_1")
test_run:switch('router_1')

-- Start the database with sharding
vshard = require('vshard')
vshard.router.cfg({sharding = {}})

--
-- Check that failover works ok when a replicaset is set, but has
-- no replicas.
--
fiber = require('fiber')
sharding = { ['cbf06940-0790-498b-948d-042b62cf3d29'] = { replicas = {} } }
vshard.router.cfg({sharding = sharding})
for i = 1, 10 do vshard.router.static.failover_fiber:wakeup() fiber.sleep(0.001) end

test_run:switch('default')
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
