test_run = require('test_run').new()
REPLICASET_1 = { 'box_1_a', 'box_1_b', 'box_1_c' }
test_run:create_cluster(REPLICASET_1, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.map_evals(test_run, {REPLICASET_1}, 'bootstrap_storage(\'memtx\')')
_ = test_run:cmd("create server router_3 with script='router/router_3.lua'")
_ = test_run:cmd("start server router_3")
_ = test_run:switch("router_3")

cfg.bucket_count = 3000
vshard.router.cfg(cfg)
vshard.router.bootstrap()

--
-- gh-179: negative router.info bucket count, when it is
-- configured improperly.
--
cfg.bucket_count = 1000
r = vshard.router.new('gh-179', cfg)
while type(r:info().bucket.unknown) == 'number' and r:info().bucket.unknown > 0 do r:discovery_wakeup() fiber.sleep(0.1) end
i = r:info()
i.bucket
i.alerts

_ = test_run:switch("default")
_ = test_run:cmd("stop server router_3")
_ = test_run:cmd("cleanup server router_3")
test_run:drop_cluster(REPLICASET_1)
_ = test_run:cmd('clear filter')
