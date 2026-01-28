local fiber = require('fiber')
local t = require('luatest')
local vreplicaset = require('vshard.replicaset')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local lversion = require('vshard.version')
local verror = require('vshard.error')

local small_timeout_opts = {timeout = 0.01}
local timeout_opts = {timeout = vtest.wait_timeout}
local sync_opts = {timeout = 1, is_async = false}
local async_opts = {timeout = 1, is_async = true}


local test_group = t.group('vconnect')

local cfg_template = {
    sharding = {
        replicaset = {
            replicas = {
                replica = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 20,
    test_user_grant_range = 'super',
    replication_timeout = 0.1,
    identification_mode = 'name_as_key',
}
local global_cfg

test_group.before_all(function(g)
    t.run_only_if(vutil.feature.persistent_names)
    global_cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
end)

test_group.after_all(function(g)
    g.cluster:stop()
end)

--
-- Test, that conn_vconnect_wait fails, when future is nil.
--
test_group.test_vconnect_no_future = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    g.replica:exec(function()
        rawset(_G, '_call', ivshard.storage._call)
        rawset(_G, 'do_sleep', true)
        -- Future should not appear at all.
        ivshard.storage._call = function()
            while _G.do_sleep do
                ifiber.sleep(0.1)
            end
        end
    end)

    rs.master.conn = nil
    local ret, err = rs:callrw('get_uuid', {}, small_timeout_opts)
    t.assert(verror.is_timeout(err))
    t.assert_equals(ret, nil)
    t.assert_not_equals(rs.master.conn.state, 'closed')

    g.replica:exec(function()
        _G.do_sleep = false
        ivshard.storage._call = _G._call
    end)
end

--
-- Test, that conn_vconnect_check fails, when future's result is nil.
--
test_group.test_vconnect_check_no_future = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    g.replica:exec(function()
        rawset(_G, '_call', ivshard.storage._call)
        ivshard.storage._call = function()
            return require('vshard.error').make('Critical error')
        end
    end)

    rs.master.conn = nil
    local opts = table.deepcopy(timeout_opts)
    opts.is_async = true
    t.helpers.retrying({}, function()
        -- It may be VHANDSHAKE_NOT_COMPLETE error, when future
        -- is not ready. But at the end it must be the actual error.
        local ret, err = rs:callrw('get_uuid', {}, opts)
        t.assert_str_contains(err.message, "PROC_LUA")
        t.assert_equals(ret, nil)
        t.assert_equals(rs.master.conn.state, 'closed')
    end)

    g.replica:exec(function()
        ivshard.storage._call = _G._call
    end)
end

--
-- 1. Change name and stop replica.
-- 2. Wait for error_reconnect timeout.
-- 3. Assert, that on reconnect name change is noticed.
--
test_group.test_vconnect_on_reconnect = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    t.assert_not_equals(rs:connect_master(), nil)
    -- Configuration to use after restart.
    local new_cfg = table.deepcopy(global_cfg)
    local cfg_rs = new_cfg.sharding.replicaset
    cfg_rs.replicas.bad = cfg_rs.replicas.replica
    cfg_rs.replicas.replica = nil

    g.replica:exec(function()
        box.cfg{instance_name = 'bad', force_recovery = true}
    end)
    g.replica:stop()
    t.helpers.retrying(timeout_opts, function()
        t.assert_equals(rs.master.conn.state, 'error_reconnect')
    end)

    -- Replica cannot be started with incorrect name, change box.cfg.
    g.replica.box_cfg.instance_name = 'bad'
    g.replica:start()
    vtest.cluster_cfg(g, new_cfg)

    t.helpers.retrying(timeout_opts, function()
        local ret, err = rs:callrw('get_uuid', {}, timeout_opts)
        t.assert_equals(err.type, 'ShardingError')
        t.assert_equals(err.name, 'INSTANCE_NAME_MISMATCH')
        t.assert_equals(ret, nil)
        t.assert_equals(rs.master.conn.state, 'closed')
    end)

    g.replica:exec(function()
        box.cfg{instance_name = 'replica', force_recovery = true}
    end)
    vtest.cluster_cfg(g, global_cfg)
end

--
-- Test, that async call doesn't yield and immediately fails.
--
test_group.test_async_no_yield = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    g.replica:exec(function()
        rawset(_G, '_call', ivshard.storage._call)
        rawset(_G, 'do_sleep', true)
        -- Future should not appear at all.
        ivshard.storage._call = function()
            while _G.do_sleep do
                ifiber.sleep(0.1)
            end
        end
    end)

    local opts = table.deepcopy(timeout_opts)
    opts.is_async = true
    local csw1 = fiber.self():csw()
    local ret, err = rs:callrw('get_uuid', {}, opts)
    local csw2 = fiber.self():csw()
    local tarantool211_version = lversion.new(2, 11, 3, 'entrypoint', 0, 71)
    local tarantool30_version = lversion.new(3, 0, 1, nil, 0, 50)
    if (vutil.core_version >= tarantool211_version and
       vutil.core_version < lversion.new(3, 0, 0, nil, 0, 0)) or
       vutil.core_version >= tarantool30_version then
        t.assert_equals(csw2, csw1)
    else
        -- Due to tarantool/tarantool#9489 bug.
        t.assert_equals(csw2, csw1 + 1)
    end
    t.assert_str_contains(err.name, 'VHANDSHAKE_NOT_COMPLETE')
    t.assert_equals(ret, nil)
    t.assert_not_equals(rs.master.conn.state, 'closed')

    g.replica:exec(function()
        _G.do_sleep = false
        ivshard.storage._call = _G._call
    end)
end

--
-- Test, that during master search name is validated.
--
test_group.test_locate_master = function()
    -- Replace name with the bad one.
    local new_cfg = table.deepcopy(global_cfg)
    local cfg_rs = new_cfg.sharding.replicaset
    cfg_rs.replicas.bad = cfg_rs.replicas.replica
    cfg_rs.replicas.replica = nil
    local _, rs = next(vreplicaset.buildall(new_cfg))

    -- Avoid noop in locate_master.
    rs.master = nil
    rs.is_master_auto = true
    -- Retry, until the connection is established and
    -- name mismach error is encountered.
    local ok, is_nop, last_err
    t.helpers.retrying(timeout_opts, function()
        ok, is_nop, last_err = rs:locate_master()
        t.assert_not_equals(last_err, nil)
    end)

    t.assert_equals(last_err.name, 'INSTANCE_NAME_MISMATCH')
    t.assert_equals(is_nop, false)
    t.assert_equals(ok, false)
end

--
-- gh-517: net.box connection leaks on detach, when vconnect hangs.
--
test_group.test_conn_not_leaks_on_detach = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    rs:wait_connected(vtest.wait_timeout)

    g.replica:exec(function()
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = function(service_name, ...)
            if service_name == 'info' then
                ifiber.sleep(ivconst.TIMEOUT_INFINITY)
            end
            return _G.old_call(service_name, ...)
        end
    end)

    -- Connection is detached new one is created, it hangs.
    rs.master:detach_conn()
    rs:connect_master()
    t.helpers.retrying({}, function()
        t.assert_not_equals(rs.master.conn.vconnect, nil)
        t.assert_not_equals(rs.master.conn.vconnect.future, nil)
    end)

    -- Hanged connection must be garbage collected.
    local tmp = setmetatable({conn = rs.master.conn}, {__mode = 'v'})
    rs.master:detach_conn()

    t.helpers.retrying({timeout = vtest.wait_timeout}, function()
        -- jit.flush is required in order to drop all traces, since
        -- otherwise object may be referenced on a trace via the function
        -- prototype of the t.helpers.retrying.
        jit.flush()
        collectgarbage()
        t.assert_equals(tmp.conn, nil)
    end)

    g.replica:exec(function()
        ivshard.storage._call = _G.old_call
    end)
    rs:wait_connected(vtest.wait_timeout)
end

--
-- gh-517: net.box connection leaks on rebind, when vconnect hangs.
--
test_group.test_conn_not_leaks_on_rebind = function(g)
    local rss = vreplicaset.buildall(global_cfg)
    local _, rs = next(rss)
    rs:wait_connected(vtest.wait_timeout)

    g.replica:exec(function()
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = function(service_name, ...)
            if service_name == 'info' then
                ifiber.sleep(ivconst.TIMEOUT_INFINITY)
            end
            return _G.old_call(service_name, ...)
        end
    end)

    -- Create hanged connection.
    rs.master:detach_conn()
    rs:connect_master()
    t.helpers.retrying({}, function()
        t.assert_not_equals(rs.master.conn.vconnect, nil)
        t.assert_not_equals(rs.master.conn.vconnect.future, nil)
    end)

    local tmp = setmetatable({conn = rs.master.conn}, {__mode = 'v'})
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding.replicaset.replicas.replica = nil
    new_cfg_template.sharding.replicaset.replicas.new_replica = {master = true}
    local new_global_cfg = vtest.config_new(new_cfg_template)
    local new_rss = vreplicaset.buildall(new_global_cfg)
    vreplicaset.rebind_replicasets(new_rss, rss)

    -- Remove strong reference to the old replicasets and wait for GC.
    -- luacheck: ignore 311/rss
    rss = nil
    t.helpers.retrying({timeout = vtest.wait_timeout}, function()
        -- See test_conn_not_leaks_on_detach, why jit.flush is needed.
        jit.flush()
        collectgarbage()
        t.assert_equals(tmp.conn, nil)
    end)

    g.replica:exec(function()
        ivshard.storage._call = _G.old_call
    end)
end

--
-- gh-632: Connection is closed during name check on initial connection,
-- when retryable error happens.
--
local function test_conn_with_retryable_error_template(g, opts, err_msg,
                                                       err_func, recovery_func)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    t.assert_not_equals(rs:connect_master(), nil)
    t.assert_equals(rs.master.conn.state, 'initial')
    g.replica:exec(err_func)
    t.helpers.retrying({}, function()
        local net_status, res, err = rs.replicas.replica:call('echo', {123},
                                                              opts)
        t.assert_not(net_status)
        t.assert_not(res)
        t.assert_str_contains(err.message, err_msg)
        t.assert_equals(rs.master.conn.state, 'active')
    end)
    g.replica:exec(recovery_func)
end


test_group.test_conn_not_closed_during_disabled_storage = function(g)
    local disable_replica = function() ivshard.storage.disable() end
    local enable_replica = function() ivshard.storage.enable() end
    local err_msg = 'Storage is disabled'

    test_conn_with_retryable_error_template(g, sync_opts, err_msg,
                                            disable_replica, enable_replica)
    test_conn_with_retryable_error_template(g, async_opts, err_msg,
                                            disable_replica, enable_replica)
end

test_group.test_conn_not_closed_during_undefined_storage_func = function(g)
    local nullify_func = function()
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = nil
    end
    local restore_func = function() ivshard.storage._call = _G.old_call end
    local err_msg = 'Procedure \'vshard.storage._call\' is not defined'

    test_conn_with_retryable_error_template(g, sync_opts, err_msg,
                                            nullify_func, restore_func)
    test_conn_with_retryable_error_template(g, async_opts, err_msg,
                                            nullify_func, restore_func)
end

test_group.test_conn_not_closed_during_denial_of_access = function(g)
    local revoke_perms = function()
        box.session.su('admin')
        box.schema.user.revoke('storage', 'super')
        box.schema.user.revoke('storage', 'execute', 'function',
                            'vshard.storage._call')
        box.session.su('guest')
    end
    local grant_perms = function()
        box.session.su('admin')
        box.schema.user.grant('storage', 'super')
        box.schema.user.grant('storage', 'execute', 'function',
                            'vshard.storage._call')
        box.session.su('guest')
    end
    local err_msg = 'Execute access to function \'vshard.storage._call\' ' ..
                    'is denied'
    test_conn_with_retryable_error_template(g, sync_opts, err_msg,
                                            revoke_perms, grant_perms)
    test_conn_with_retryable_error_template(g, async_opts, err_msg,
                                            revoke_perms, grant_perms)
end
