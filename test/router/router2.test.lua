test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'router')
test_run:create_cluster(REPLICASET_2, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')
util.push_rs_filters(test_run)
_ = test_run:cmd("create server router_1 with script='router/router_1.lua'")
_ = test_run:cmd("start server router_1")

_ = test_run:switch("router_1")
util = require('util')

-- gh-210: router should provide API to enable/disable discovery,
-- since it is a too expensive thing in big clusters to be not
-- stoppable/controllable.

f1 = vshard.router.static.discovery_fiber
cfg.discovery_mode = 'off'
vshard.router.cfg(cfg)
vshard.router.static.discovery_fiber
f2 = vshard.router.static.discovery_fiber

cfg.discovery_mode = 'on'
vshard.router.cfg(cfg)
f3 = vshard.router.static.discovery_fiber
vshard.router.static.discovery_fiber:status()

cfg.discovery_mode = nil
vshard.router.cfg(cfg)
f4 = vshard.router.static.discovery_fiber
vshard.router.static.discovery_fiber:status()

vshard.router.discovery_set('off')
f5 = vshard.router.static.discovery_fiber
vshard.router.static.discovery_fiber
vshard.router.discovery_set('on')
f6 = vshard.router.static.discovery_fiber
vshard.router.static.discovery_fiber:status()

cfg.discovery_mode = 'once'
vshard.router.cfg(cfg)
f7 = vshard.router.static.discovery_fiber
vshard.router.static.discovery_fiber:status()

f1:status(), f2, f3:status(), f4:status(), f5, f6:status(), f7:status()

-- Errored discovery continued successfully after errors are gone.
vshard.router.bootstrap()
vshard.router.discovery_set('off')
vshard.router._route_map_clear()

-- Discovery requests 2 and 4 will fail on storages.
util.map_evals(test_run, {{'storage_1_a'}, {'storage_2_a'}},                    \
               'vshard.storage.internal.errinj.ERRINJ_DISCOVERY = 4')

vshard.router.info().bucket.unknown
vshard.router.discovery_set('on')
function continue_discovery()                                                   \
    local res = vshard.router.info().bucket.unknown == 0                        \
    if not res then                                                             \
        vshard.router.discovery_wakeup()                                        \
    end                                                                         \
    return res                                                                  \
end
test_run:wait_cond(continue_discovery)
vshard.router.info().bucket.unknown

-- Discovery injections should be reset meaning they were returned
-- needed number of times.
_ = test_run:switch('storage_1_a')
vshard.storage.internal.errinj.ERRINJ_DISCOVERY
_ = test_run:switch('storage_2_a')
vshard.storage.internal.errinj.ERRINJ_DISCOVERY

-- With 'on' discovery works infinitely.
_ = test_run:switch('router_1')
vshard.router._route_map_clear()
vshard.router.discovery_set('on')
test_run:wait_cond(continue_discovery)
vshard.router.info().bucket.unknown
vshard.router.static.discovery_fiber:status()

-- With 'once' discovery mode the discovery fiber deletes self
-- after full discovery.
vshard.router._route_map_clear()
vshard.router.discovery_set('once')
test_run:wait_cond(continue_discovery)
vshard.router.info().bucket.unknown
vshard.router.static.discovery_fiber
-- Second set won't do anything.
vshard.router.discovery_set('once')
vshard.router.static.discovery_fiber

_ = test_run:switch("default")
_ = test_run:cmd("stop server router_1")
_ = test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
_ = test_run:cmd('clear filter')
