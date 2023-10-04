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

--
-- gh-75: automatic master discovery on router.
--

_ = test_run:switch("router_1")
util = require('util')
vshard.router.bootstrap()

for _, rs in pairs(cfg.sharding) do                                             \
    for _, r in pairs(rs.replicas) do                                           \
        r.master = nil                                                          \
    end                                                                         \
end                                                                             \

function enable_auto_masters()                                                  \
    for _, rs in pairs(cfg.sharding) do                                         \
        rs.master = 'auto'                                                      \
    end                                                                         \
    vshard.router.cfg(cfg)                                                      \
end

function disable_auto_masters()                                                 \
    for _, rs in pairs(cfg.sharding) do                                         \
        rs.master = nil                                                         \
    end                                                                         \
    vshard.router.cfg(cfg)                                                      \
end

-- But do not forget the buckets. Otherwise bucket discovery will establish
-- the connections instead of external requests.
function forget_masters()                                                       \
    disable_auto_masters()                                                      \
    enable_auto_masters()                                                       \
end

function check_all_masters_found()                                              \
    for _, rs in pairs(vshard.router.static.replicasets) do                     \
        if not rs.master then                                                   \
            vshard.router.master_search_wakeup()                                \
            return false                                                        \
        end                                                                     \
    end                                                                         \
    return true                                                                 \
end

function check_master_for_replicaset(rs_id, master_name)                        \
    local rs_uuid = util.replicasets[rs_id]                                     \
    local master_uuid = util.name_to_uuid[master_name]                          \
    local master = vshard.router.static.replicasets[rs_uuid].master             \
    if not master or master.uuid ~= master_uuid then                            \
        vshard.router.master_search_wakeup()                                    \
        return false                                                            \
    end                                                                         \
    return true                                                                 \
end

function check_no_master_for_replicaset(rs_id)                                  \
    local rs_uuid = util.replicasets[rs_id]                                     \
    local master = vshard.router.static.replicasets[rs_uuid].master             \
    if not master then                                                          \
        return true                                                             \
    end                                                                         \
    vshard.router.master_search_wakeup()                                        \
    return false                                                                \
end

function check_all_buckets_found()                                              \
    if vshard.router.info().bucket.unknown == 0 then                            \
        return true                                                             \
    end                                                                         \
    vshard.router.discovery_wakeup()                                            \
    return false                                                                \
end

master_search_helper_f = nil
function aggressive_master_search_f()                                           \
    while true do                                                               \
        vshard.router.master_search_wakeup()                                    \
        fiber.sleep(0.001)                                                      \
    end                                                                         \
end

function start_aggressive_master_search()                                       \
    assert(master_search_helper_f == nil)                                       \
    master_search_helper_f = fiber.new(aggressive_master_search_f)              \
    master_search_helper_f:set_joinable(true)                                   \
end

function stop_aggressive_master_search()                                        \
    assert(master_search_helper_f ~= nil)                                       \
    master_search_helper_f:cancel()                                             \
    master_search_helper_f:join()                                               \
    master_search_helper_f = nil                                                \
end

function master_discovery_block()                                               \
    vshard.router.internal.errinj.ERRINJ_MASTER_SEARCH_DELAY = true             \
end

function check_master_discovery_block()                                         \
    if vshard.router.internal.errinj.ERRINJ_MASTER_SEARCH_DELAY == 'in' then    \
        return true                                                             \
    end                                                                         \
    vshard.router.master_search_wakeup()                                        \
    return false                                                                \
end

function master_discovery_unblock()                                             \
    vshard.router.internal.errinj.ERRINJ_MASTER_SEARCH_DELAY = false            \
end

--
-- Simulate the first cfg when no masters are known.
--
forget_masters()
assert(vshard.router.static.master_search_fiber ~= nil)
test_run:wait_cond(check_all_masters_found)
test_run:wait_cond(check_all_buckets_found)

--
-- Change master and see how router finds it again.
--
test_run:switch('storage_1_a')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = false
replicas[util.name_to_uuid.storage_1_b].master = true
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = false
replicas[util.name_to_uuid.storage_1_b].master = true
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('router_1')
big_timeout = 1000000
opts_big_timeout = {timeout = big_timeout}
test_run:wait_cond(function()                                                   \
    return check_master_for_replicaset(1, 'storage_1_b')                        \
end)
vshard.router.callrw(1501, 'echo', {1}, opts_big_timeout)

test_run:switch('storage_1_b')
assert(echo_count == 1)
echo_count = 0

--
-- Revert the master back.
--
test_run:switch('storage_1_a')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = true
replicas[util.name_to_uuid.storage_1_b].master = false
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = true
replicas[util.name_to_uuid.storage_1_b].master = false
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('router_1')
test_run:wait_cond(function()                                                   \
    return check_master_for_replicaset(1, 'storage_1_a')                        \
end)

--
-- Call tries to wait for master if has enough time left.
--
start_aggressive_master_search()
test_run:cmd('stop server storage_1_b')
rs1 = vshard.router.static.replicasets[util.replicasets[1]]
replica = rs1.replicas[util.name_to_uuid.storage_1_b]
-- Ensure the replica is not available. Otherwise RO requests sneak into it
-- instead of waiting for master.
test_run:wait_cond(function() return not replica:is_connected() end)

forget_masters()
vshard.router.callrw(1501, 'echo', {1}, opts_big_timeout)

forget_masters()
vshard.router.callro(1501, 'echo', {1}, opts_big_timeout)

forget_masters()
vshard.router.route(1501):callrw('echo', {1}, opts_big_timeout)

forget_masters()
vshard.router.route(1501):callro('echo', {1}, opts_big_timeout)

stop_aggressive_master_search()
test_run:cmd('start server storage_1_b')

test_run:switch('storage_1_a')
assert(echo_count == 4)
echo_count = 0

--
-- Old replicaset objects stop waiting for master when search is disabled.
--

-- Turn off masters on the first replicaset.
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = false
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = false
vshard.storage.cfg(cfg, instance_uuid)

-- Try to make an RW request but then turn of the auto search.
test_run:switch('router_1')
forget_masters()
f1 = fiber.create(function()                                                    \
    fiber.self():set_joinable(true)                                             \
    return vshard.router.callrw(1501, 'echo', {1}, opts_big_timeout)            \
end)
fiber.sleep(0.01)
disable_auto_masters()
f1:join()

-- Try to make an RO request but then turn of the auto search.
test_run:cmd('stop server storage_1_a')
test_run:cmd('stop server storage_1_b')
forget_masters()
f2 = fiber.create(function()                                                    \
    fiber.self():set_joinable(true)                                             \
    return vshard.router.callro(1501, 'echo', {1}, opts_big_timeout)            \
end)
fiber.sleep(0.01)
disable_auto_masters()
f2:join()
test_run:cmd('start server storage_1_a')
test_run:cmd('start server storage_1_b')

--
-- Multiple masters logging.
--
test_run:switch('storage_1_a')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = true
replicas[util.name_to_uuid.storage_1_b].master = false
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = false
replicas[util.name_to_uuid.storage_1_b].master = true
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('router_1')
-- Ensure both replicas are connected. Otherwise the router can go to only one,
-- find it is master, and won't go to the second one until the first one resigns
-- or dies.
rs1 = vshard.router.static.replicasets[util.replicasets[1]]
replica1 = rs1.replicas[util.name_to_uuid.storage_1_a]
replica2 = rs1.replicas[util.name_to_uuid.storage_1_b]
test_run:wait_cond(function()                                                   \
    return replica1:is_connected() and replica2:is_connected()                  \
end)

forget_masters()
start_aggressive_master_search()
test_run:wait_log('router_1', 'Found more than one master', nil, 10)
stop_aggressive_master_search()

--
-- Async request won't wait for master. Otherwise it would need to wait, which
-- is not async behaviour. The timeout should be ignored.
--
do                                                                              \
    forget_masters()                                                            \
    return vshard.router.callrw(1501, 'echo', {1}, {                            \
        is_async = true, timeout = big_timeout                                  \
    })                                                                          \
end

--
-- Restore the old master back.
--
test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_b].master = false
replicas[util.name_to_uuid.storage_1_a].master = true
vshard.storage.cfg(cfg, instance_uuid)

--
-- RW call uses a hint from the old master about who is the new master.
--
test_run:switch('router_1')
forget_masters()
test_run:wait_cond(check_all_masters_found)
master_discovery_block()
test_run:wait_cond(check_master_discovery_block)

-- Change master while discovery is asleep.
test_run:switch('storage_1_a')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = false
replicas[util.name_to_uuid.storage_1_b].master = true
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = false
replicas[util.name_to_uuid.storage_1_b].master = true
vshard.storage.cfg(cfg, instance_uuid)

-- First request fails and tells where is the master. The second attempt works.
test_run:switch('router_1')
vshard.router.callrw(1501, 'echo', {1}, opts_big_timeout)

test_run:switch('storage_1_b')
assert(echo_count == 1)
echo_count = 0

--
-- A non master error might contain no information about a new master.
--
-- Make the replicaset read-only.
test_run:switch('storage_1_a')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_b].master = false
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_b].master = false
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('router_1')
-- A request should return no info about a new master. The router will wait for
-- a new master discovery.
f = fiber.create(function()                                                     \
    fiber.self():set_joinable(true)                                             \
    return vshard.router.callrw(1501, 'echo', {1}, opts_big_timeout)            \
end)
test_run:wait_cond(function()                                                   \
    return check_no_master_for_replicaset(1)                                    \
end)

test_run:switch('storage_1_a')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = true
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('storage_1_b')
replicas = cfg.sharding[util.replicasets[1]].replicas
replicas[util.name_to_uuid.storage_1_a].master = true
vshard.storage.cfg(cfg, instance_uuid)

test_run:switch('router_1')
master_discovery_unblock()
test_run:wait_cond(check_all_masters_found)
f:join()

test_run:switch('storage_1_a')
assert(echo_count == 1)
echo_count = 0

--
-- Unit tests for master change in a replicaset object. Normally it can only
-- happen in quite complicated cases. Hence the tests prefer to use the internal
-- replicaset object instead.
-- Disable the master search fiber so as it wouldn't interfere.
--
test_run:switch('router_1')
master_discovery_block()
test_run:wait_cond(check_master_discovery_block)
rs = vshard.router.static.replicasets[util.replicasets[1]]
storage_a_uuid = util.name_to_uuid.storage_1_a
storage_b_uuid = util.name_to_uuid.storage_1_b

assert(rs.master.uuid == storage_a_uuid)
rs.master = nil
rs.is_master_auto = false

-- When auto-search is disabled and master is not known, nothing will make it
-- known. It is up to the config.
assert(not rs:update_master(storage_a_uuid, storage_b_uuid))
assert(not rs.master)
-- New master might be not reported.
assert(not rs:update_master(storage_a_uuid))
assert(not rs.master)

-- With auto-search and not known master it is not assigned if a new master is
-- not reported.
rs.is_master_auto = true
-- But update returns true, because it makes sense to try a next request later
-- when the master is found.
assert(rs:update_master(storage_a_uuid))
assert(not rs.master)

-- Report of a not known UUID won't assign the master.
assert(rs:update_master(storage_a_uuid, util.name_to_uuid.storage_2_a))
assert(not rs.master)

-- Report of a known UUID assigns the master.
assert(rs:update_master(storage_a_uuid, storage_b_uuid))
assert(rs.master.uuid == storage_b_uuid)

-- Master could change while the request's error was being received. Then the
-- error should not change anything because it is outdated.
assert(rs:update_master(storage_a_uuid))
assert(rs.master.uuid == storage_b_uuid)
-- It does not depend on auto-search. Still returns true, because if the master
-- was changed since the request was sent, it means it could be retried and
-- might succeed.
rs.is_master_auto = false
assert(rs:update_master(storage_a_uuid))
assert(rs.master.uuid == storage_b_uuid)

-- If the current master is reported as not a master and auto-search is
-- disabled, update should fail. Because makes no sense to retry until a new
-- config is applied externally.
assert(not rs:update_master(storage_b_uuid, storage_a_uuid))
assert(rs.master.uuid == storage_b_uuid)

-- With auto-search, if the node is not a master and no new master is reported,
-- the current master should be reset. Because makes no sense to send more RW
-- requests to him. But update returns true, because the current request could
-- be retried after waiting for a new master discovery.
rs.is_master_auto = true
assert(rs:update_master(storage_b_uuid))
assert(rs.master == nil)

-- When candidate is reported, and is known, it is used. But restore the master
-- first to test its change.
assert(rs:update_master(storage_b_uuid, storage_a_uuid))
assert(rs.master.uuid == storage_a_uuid)
-- Now update.
assert(rs:update_master(storage_a_uuid, storage_b_uuid))
assert(rs.master.uuid == storage_b_uuid)

-- Candidate UUID might be not known in case the topology config is different on
-- the router and on the storage. Then the master is simply reset.
assert(rs:update_master(storage_b_uuid, util.name_to_uuid.storage_2_a))
assert(rs.master == nil)

-- Replica reports self as both master and not - ignore conflicting info.
assert(rs:update_master(storage_b_uuid, storage_b_uuid))
assert(rs.master == nil)

master_discovery_unblock()
test_run:wait_cond(check_all_masters_found)

_ = test_run:switch("default")
_ = test_run:cmd("stop server router_1")
_ = test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
_ = test_run:cmd('clear filter')
