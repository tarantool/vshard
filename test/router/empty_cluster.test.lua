test_run = require('test_run').new()
test_run:cmd("create server router_1 with script='router/empty_cluster.lua'")
test_run:cmd("start server router_1")
test_run:switch('router_1')

-- Start the database with sharding
vshard = require('vshard')
vshard.router.cfg({sharding = {}})

test_run:switch('default')
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
