test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')

test_run:switch('storage_1_a')
vshard.storage.bucket_force_create(1, vshard.consts.DEFAULT_BUCKET_COUNT / 2)
test_run:switch('storage_2_a')
fiber = require('fiber')
vshard.storage.bucket_force_create(vshard.consts.DEFAULT_BUCKET_COUNT / 2 + 1, vshard.consts.DEFAULT_BUCKET_COUNT / 2)

--
-- Gh-72: allow reload. Test simple reload, error during
-- reloading, ensure the fibers are restarted on reload.
--

assert(rawget(_G, '__module_vshard_storage') ~= nil)

vshard.storage.module_version()
while test_run:grep_log('storage_2_a', 'The cluster is balanced ok') == nil do vshard.storage.rebalancer_wakeup() fiber.sleep(0.1) end
test_run:cmd("setopt delimiter ';'")
function check_reloaded()
	for k, v in pairs(old_internal) do
		if v == vshard.storage.internal[k] then
			return k
		end
	end
end;
function check_not_reloaded()
	for k, v in pairs(old_internal) do
		if v ~= vshard.storage.internal[k] then
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
test_run:cmd("setopt delimiter ''");

--
-- Simple reload. All functions are reloaded and they have
-- another pointers in vshard.storage.internal.
--
old_internal = copy_functions(vshard.storage.internal)
package.loaded["vshard.storage"] = nil
_ = require('vshard.storage')
vshard.storage.module_version()

check_reloaded()

while test_run:grep_log('storage_2_a', 'collect_garbage_f has been started') == nil do fiber.sleep(0.1) end
while test_run:grep_log('storage_2_a', 'recovery_f has been started') == nil do fiber.sleep(0.1) vshard.storage.recovery_wakeup() end
while test_run:grep_log('storage_2_a', 'rebalancer_f has been started') == nil do fiber.sleep(0.1) vshard.storage.rebalancer_wakeup() end

check_reloaded()

--
-- Error during reload - in such a case no function can be
-- updated. Reload is atomic.
--
vshard.storage.internal.errinj.ERRINJ_RELOAD = true
old_internal = copy_functions(vshard.storage.internal)
package.loaded["vshard.storage"] = nil
util = require('util')
util.check_error(require, 'vshard.storage')
check_not_reloaded()
vshard.storage.module_version()

--
-- A next reload is ok, and all functions are updated.
--
vshard.storage.internal.errinj.ERRINJ_RELOAD = false
old_internal = copy_functions(vshard.storage.internal)
package.loaded["vshard.storage"] = nil
_ = require('vshard.storage')
vshard.storage.module_version()
check_reloaded()

--
-- Outdate old replicaset and replica objects.
--
_, rs = next(vshard.storage.internal.replicasets)
package.loaded["vshard.storage"] = nil
_ = require('vshard.storage')
rs.callro(rs, 'echo', {'some_data'})
_, rs = next(vshard.storage.internal.replicasets)
rs.callro(rs, 'echo', {'some_data'})

test_run:switch('default')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
test_run:cmd('clear filter')
