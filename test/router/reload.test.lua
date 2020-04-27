test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage(\'memtx\')')
_ = test_run:cmd("create server router_1 with script='router/router_1.lua'")
_ = test_run:cmd("start server router_1")

_ = test_run:switch('router_1')
fiber = require('fiber')
vshard.router.bootstrap()

while test_run:grep_log('router_1', 'All replicas are ok') == nil do fiber.sleep(0.1) end
while test_run:grep_log('router_1', 'buckets: was 0, became 1000') == nil do fiber.sleep(0.1) vshard.router.discovery_wakeup() end
while test_run:grep_log('router_1', 'buckets: was 1000, became 1500') == nil do fiber.sleep(0.1) vshard.router.discovery_wakeup() end

--
-- Gh-72: allow reload. Test simple reload, error during
-- reloading, ensure the fibers are restarted on reload.
--

assert(rawget(_G, '__module_vshard_router') ~= nil)
vshard.router.module_version()
_ = test_run:cmd("setopt delimiter ';'")
function check_reloaded()
	for k, v in pairs(old_internal) do
		if v == vshard.router.internal[k] then
			return k
		end
	end
end;
function check_not_reloaded()
	for k, v in pairs(old_internal) do
		if v ~= vshard.router.internal[k] then
			return k
		end
	end
end;
function copy_functions(t)
	local ret = {}
	for k, v in pairs(t) do
		if type(v) == 'function' then
			ret[k] = v
		end
	end
	return ret
end;
_ = test_run:cmd("setopt delimiter ''");
--
-- Simple reload. All functions are reloaded and they have
-- another pointers in vshard.router.internal.
--
old_internal = copy_functions(vshard.router.internal)
package.loaded["vshard.router"] = nil
_ = require('vshard.router')
vshard.router.module_version()

check_reloaded()

while test_run:grep_log('router_1', 'failover_f has been started') == nil do fiber.sleep(0.1) end
while test_run:grep_log('router_1', 'discovery_f has been started') == nil do fiber.sleep(0.1) vshard.router.discovery_wakeup() end

check_reloaded()

--
-- Error during reload - in such a case no function can be
-- updated. Reload is atomic.
--
vshard.router.internal.errinj.ERRINJ_RELOAD = true
old_internal = copy_functions(vshard.router.internal)
package.loaded["vshard.router"] = nil
util = require('util')
util.check_error(require, 'vshard.router')
check_not_reloaded()
vshard.router.module_version()

--
-- A next reload is ok, and all functions are updated.
--
vshard.router.internal.errinj.ERRINJ_RELOAD = false
old_internal = copy_functions(vshard.router.internal)
package.loaded["vshard.router"] = nil
_ = require('vshard.router')
vshard.router.module_version()
check_reloaded()

--
-- Outdate old replicaset and replica objects.
--
rs = vshard.router.route(1)
rs:callro('echo', {'some_data'})
package.loaded["vshard.router"] = nil
_ = require('vshard.router')
-- Make sure outdate async task has had cpu time.
while not rs.is_outdated do fiber.sleep(0.001) end
rs.callro(rs, 'echo', {'some_data'})
vshard.router = require('vshard.router')
rs = vshard.router.route(1)
rs:callro('echo', {'some_data'})
-- Test `connection_outdate_delay`.
old_connection_delay = cfg.connection_outdate_delay
cfg.connection_outdate_delay = 0.3
vshard.router.cfg(cfg)
cfg.connection_outdate_delay = old_connection_delay
vshard.router.static.connection_outdate_delay = nil
rs_new = vshard.router.route(1)
rs_old = rs
_, replica_old = next(rs_old.replicas)
rs_new:callro('echo', {'some_data'})
-- Check old objets are still valid.
rs_old:callro('echo', {'some_data'})
replica_old.conn ~= nil
fiber.sleep(0.2)
rs_old:callro('echo', {'some_data'})
replica_old.conn ~= nil
replica_old.is_outdated == nil
fiber.sleep(0.2)
rs_old:callro('echo', {'some_data'})
replica_old.conn == nil
replica_old.is_outdated == true
rs_new:callro('echo', {'some_data'})

--
-- gh-193: code should not rely on global function addresses
-- stability. They change at reload. Because of that, for example,
-- removal of an old trigger becomes impossible by a global
-- function name.
--
-- The call below should not throw an exception.
rs_new.master:detach_conn()

_ = test_run:switch('default')
_ = test_run:cmd('stop server router_1')
_ = test_run:cmd('cleanup server router_1')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
