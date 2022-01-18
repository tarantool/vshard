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

--
-- Known bucket count should be updated properly when replicaset
-- is removed from the config.
--
vshard.router.info().bucket
rs1_uuid = util.replicasets[1]
rs1 = cfg.sharding[rs1_uuid]
cfg.sharding[rs1_uuid] = nil
vshard.router.cfg(cfg)
vshard.router.info().bucket
cfg.sharding[rs1_uuid] = rs1
vshard.router.cfg(cfg)
vshard.router.discovery_set('on')
function wait_all_rw()                                                          \
    local total = vshard.router.bucket_count()                                  \
    local res = vshard.router.info().bucket.available_rw == total               \
    if not res then                                                             \
        vshard.router.discovery_wakeup()                                        \
    end                                                                         \
    return res                                                                  \
end
test_run:wait_cond(wait_all_rw)
vshard.router.info().bucket

--
-- gh-298: backoff replicas when the storage raises errors meaning its
-- configuration is not finished or even didn't start. Immediately failover to
-- other instances then.
--
test_run:switch('storage_2_b')
-- Turn off replication so as _func manipulations on the master wouldn't reach
-- the replica.
old_replication = box.cfg.replication
box.cfg{replication = {}}

test_run:switch('storage_2_a')
box.schema.user.revoke('storage', 'execute', 'function', 'vshard.storage.call')

test_run:switch('router_1')
vshard.consts.REPLICA_BACKOFF_INTERVAL = 0.1

-- Indeed fails when called directly via netbox.
conn = vshard.router.route(1).master.conn
ok, err = pcall(conn.call, conn, 'vshard.storage.call',                         \
                {1, 'read', 'echo', {1}})
assert(not ok and err.code == box.error.ACCESS_DENIED)

-- Works when called via vshard - it goes to another replica transparently.
long_timeout = {timeout = 1000000}
res = vshard.router.callro(1, 'echo', {100}, long_timeout)
assert(res == 100)

--
-- When all replicas are in backoff due to lack of access, raise an error.
--
test_run:switch('storage_2_b')
assert(echo_count == 1)
echo_count = 0
-- Restore the replication so the replica gets the _func change from master.
box.cfg{replication = old_replication}
test_run:wait_vclock('storage_2_b', test_run:get_vclock('storage_1_a'))

test_run:switch('router_1')
ok, err = vshard.router.callro(1, 'echo', {100}, long_timeout)
assert(not ok and err.code == vshard.error.code.REPLICASET_IN_BACKOFF)
assert(err.error.code == box.error.ACCESS_DENIED)

test_run:switch('storage_2_a')
assert(echo_count == 0)
box.schema.user.grant('storage', 'execute', 'function', 'vshard.storage.call')
test_run:wait_vclock('storage_2_b', test_run:get_vclock('storage_1_a'))

--
-- No vshard function = backoff.
--
test_run:switch('router_1')
-- Drop all backoffs to check all works fine now.
fiber.sleep(vshard.consts.REPLICA_BACKOFF_INTERVAL)
res = vshard.router.callrw(1, 'echo', {100}, long_timeout)
assert(res == 100)

test_run:switch('storage_2_a')
assert(echo_count == 1)
echo_count = 0
old_storage_call = vshard.storage.call
vshard.storage.call = nil

-- Indeed fails when called directly via netbox.
test_run:switch('router_1')
conn = vshard.router.route(1).master.conn
ok, err = pcall(conn.call, conn, 'vshard.storage.call',                         \
                {1, 'read', 'echo', {1}})
assert(not ok and err.code == box.error.NO_SUCH_PROC)

-- Works when called via vshard - it goes to another replica.
res = vshard.router.callro(1, 'echo', {100}, long_timeout)
assert(res == 100)

--
-- When all replicas are in backoff due to not having the function, raise
-- an error.
--
test_run:switch('storage_2_b')
assert(echo_count == 1)
echo_count = 0
old_storage_call = vshard.storage.call
vshard.storage.call = nil

test_run:switch('router_1')
ok, err = vshard.router.callro(1, 'echo', {100}, long_timeout)
assert(not ok and err.code == vshard.error.code.REPLICASET_IN_BACKOFF)
assert(err.error.code == box.error.NO_SUCH_PROC)

test_run:switch('storage_2_b')
assert(echo_count == 0)
vshard.storage.call = old_storage_call

test_run:switch('storage_2_a')
assert(echo_count == 0)
vshard.storage.call = old_storage_call

--
-- Fails without backoff for other errors.
--
test_run:switch('router_1')
fiber.sleep(vshard.consts.REPLICA_BACKOFF_INTERVAL)
rs = vshard.router.route(1)
ok, err = rs:callro('vshard.storage.call', {1, 'badmode', 'echo', {100}},       \
                    long_timeout)
assert(not ok and err.message:match('Unknown mode') ~= nil)

--
-- Storage is disabled = backoff.
--
test_run:switch('storage_2_a')
vshard.storage.disable()

test_run:switch('router_1')
-- Drop old backoffs.
fiber.sleep(vshard.consts.REPLICA_BACKOFF_INTERVAL)
-- Success, but internally the request was retried.
--
-- n/a: there was a bug when an error code in the router depended
-- on vshard.error being global. Nullify it to ensure it is not
-- the case anymore.
router = vshard.router
vshard = nil
res, err = router.callro(1, 'echo', {100}, long_timeout)
assert(res == 100)
vshard = package.loaded.vshard

-- The best replica entered backoff state.
util = require('util')
storage_2 = vshard.router.static.replicasets[replicasets[2]]
storage_2_a = storage_2.replicas[util.name_to_uuid.storage_2_a]
assert(storage_2_a.backoff_ts ~= nil)

test_run:switch('storage_2_b')
assert(echo_count == 1)
echo_count = 0

test_run:switch('storage_2_a')
assert(echo_count == 0)
vshard.storage.enable()

test_run:switch('router_1')
-- Drop the backoff.
fiber.sleep(vshard.consts.REPLICA_BACKOFF_INTERVAL)
-- Now goes to the best replica - it is enabled again.
res, err = vshard.router.callro(1, 'echo', {100}, long_timeout)
assert(res == 100)

test_run:switch('storage_2_a')
assert(echo_count == 1)

_ = test_run:switch("default")
_ = test_run:cmd("stop server router_1")
_ = test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
_ = test_run:cmd('clear filter')
