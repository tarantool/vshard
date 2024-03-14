local fiber = require('fiber')
local t = require('luatest')
local vreplicaset = require('vshard.replicaset')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local lversion = require('vshard.version')
local verror = require('vshard.error')

local small_timeout_opts = {timeout = 0.01}
local timeout_opts = {timeout = vtest.wait_timeout}

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
-- Test, that conn_vconnect_wait fails to get correct
-- result. Connection should be closed.
--
test_group.test_vconnect_no_result = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    g.replica:exec(function()
        rawset(_G, '_call', ivshard.storage._call)
        ivshard.storage._call = nil
    end)

    -- Drop connection in order to make replicaset to recreate it.
    rs.master.conn = nil
    local ret, err = rs:callrw('get_uuid', {}, timeout_opts)
    t.assert_str_contains(err.message, "_call' is not defined")
    t.assert_equals(ret, nil)
    -- Critical error, connection should be closed.
    t.assert_equals(rs.master.conn.state, 'closed')

    g.replica:exec(function()
        ivshard.storage._call = _G._call
    end)
end

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
        ivshard.storage._call = nil
    end)

    rs.master.conn = nil
    local opts = table.deepcopy(timeout_opts)
    opts.is_async = true
    t.helpers.retrying({}, function()
        -- It may be VHANDSHAKE_NOT_COMPLETE error, when future
        -- is not ready. But at the end it must be the actual error.
        local ret, err = rs:callrw('get_uuid', {}, opts)
        t.assert_str_contains(err.message, "_call' is not defined")
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
    t.helpers.retrying({}, function()
        t.assert_equals(rs.master.conn.state, 'error_reconnect')
    end)

    -- Replica cannot be started with incorrect name, change box.cfg.
    g.replica.box_cfg.instance_name = 'bad'
    g.replica:start()
    vtest.cluster_cfg(g, new_cfg)
    local ret, err = rs:callrw('get_uuid', {}, timeout_opts)
    t.assert_equals(err.name, 'INSTANCE_NAME_MISMATCH')
    t.assert_equals(ret, nil)
    t.assert_equals(rs.master.conn.state, 'closed')

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
