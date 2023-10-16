local fiber = require('fiber')
local t = require('luatest')
local vreplicaset = require('vshard.replicaset')
local vtest = require('test.luatest_helpers.vtest')

local small_timeout_opts = {timeout = 0.05}
local timeout_opts = {timeout = vtest.wait_timeout}

local test_group = t.group('replicaset')

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
                replica_1_c = {},
            },
        },
    },
    bucket_count = 20
}
local global_cfg

test_group.before_all(function(g)
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_wait_connected_all = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    local uuid_a = g.replica_1_a:instance_uuid()
    local uuid_b = g.replica_1_b:instance_uuid()
    --
    -- Basic.
    --
    local timeout, err, err_uuid = rs:wait_connected_all(timeout_opts)
    t.assert_equals(err, nil)
    t.assert_equals(err_uuid, nil)
    t.assert_not_equals(timeout, nil)
    t.assert_le(timeout, vtest.wait_timeout)
    --
    -- One is dead.
    --
    g.replica_1_a:exec(function()
        rawset(_G, "test_listen", box.cfg.listen)
        box.cfg{listen = box.NULL}
    end)
    rs.replicas[uuid_a].conn:close()
    rs.replicas[uuid_a].conn = nil
    timeout, err, err_uuid = rs:wait_connected_all(small_timeout_opts)
    t.assert_not_equals(err, nil)
    t.assert_equals(err_uuid, uuid_a)
    t.assert_equals(timeout, nil)
    --
    -- Exception.
    --
    timeout, err, err_uuid = rs:wait_connected_all({
        timeout = vtest.wait_timeout,
        except = uuid_a,
    })
    t.assert_equals(err, nil)
    t.assert_equals(err_uuid, nil)
    t.assert_not_equals(timeout, nil)
    t.assert_le(timeout, vtest.wait_timeout)
    --
    -- Restore the connection - normal wait works again.
    --
    g.replica_1_a:exec(function()
        box.cfg{listen = _G.test_listen}
        _G.test_listen = nil
    end)
    timeout = rs:wait_connected_all(timeout_opts)
    t.assert_not_equals(timeout, nil)
    --
    -- Another is dead.
    --
    g.replica_1_b:exec(function()
        rawset(_G, "test_listen", box.cfg.listen)
        box.cfg{listen = box.NULL}
    end)
    rs.replicas[uuid_b].conn:close()
    rs.replicas[uuid_b].conn = nil
    timeout, err, err_uuid = rs:wait_connected_all(small_timeout_opts)
    t.assert_not_equals(err, nil)
    t.assert_equals(err_uuid, uuid_b)
    t.assert_equals(timeout, nil)
    --
    -- Connection established during the waiting is fine.
    --
    local f = fiber.new(function()
        return rs:wait_connected_all(timeout_opts)
    end)
    f:set_joinable(true)
    fiber.sleep(0.01)
    t.assert_equals(f:status(), 'suspended')
    g.replica_1_b:exec(function()
        box.cfg{listen = _G.test_listen}
        _G.test_listen = nil
    end)
    local ok
    ok, timeout, err, err_uuid = f:join()
    t.assert(ok)
    t.assert_equals(err, nil)
    t.assert_equals(err_uuid, nil)
    t.assert_not_equals(timeout, nil)
    t.assert_lt(timeout, vtest.wait_timeout)
end

test_group.test_map_call = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    local uuid_a = g.replica_1_a:instance_uuid()
    local uuid_b = g.replica_1_b:instance_uuid()
    local uuid_c = g.replica_1_c:instance_uuid()
    --
    -- Basic.
    --
    local res, err, err_uuid = rs:map_call('get_uuid', {}, timeout_opts)
    t.assert_equals(err, nil)
    t.assert_equals(err_uuid, nil)
    t.assert_equals(res, {
        [uuid_a] = {uuid_a},
        [uuid_b] = {uuid_b},
        [uuid_c] = {uuid_c},
    })
    --
    -- One instance is down.
    --
    vtest.storage_stop(g.replica_1_b)
    res, err, err_uuid = rs:map_call('get_uuid', {}, small_timeout_opts)
    t.assert_not_equals(err, nil)
    t.assert_equals(err_uuid, uuid_b)
    t.assert_equals(res, nil)
    vtest.storage_start(g.replica_1_b, global_cfg)
    --
    -- Raise an error.
    --
    vtest.cluster_exec_each(g, function(uuid_a)
        rawset(_G, 'test_err_a', function()
            if box.info.uuid == uuid_a then
                error('uuid_a')
            end
            return true
        end)
    end, {uuid_a})
    res, err, err_uuid = rs:map_call('test_err_a', {}, timeout_opts)
    t.assert_not_equals(err, nil)
    t.assert_str_contains(err.message, 'uuid_a')
    t.assert_equals(err_uuid, uuid_a)
    t.assert_equals(res, nil)
    --
    -- Too long request.
    --
    vtest.cluster_exec_each(g, function(uuid_c)
        rawset(_G, 'test_sleep_do', true)
        rawset(_G, 'test_sleep_c', function()
            rawset(_G, 'test_sleep_is_called', true)
            if box.info.uuid == uuid_c then
                while _G.test_sleep_do do
                    ifiber.sleep(ivtest.busy_step)
                end
            end
            return true
        end)
    end, {uuid_c})
    res, err = rs:map_call('test_sleep_c', {}, small_timeout_opts)
    t.assert_not_equals(err, nil)
    t.assert(vtest.error_is_timeout(err))
    t.assert_equals(res, nil)
    res = vtest.cluster_exec_each(g, function()
        _G.test_sleep_do = false
        return _G.test_sleep_is_called
    end)
    t.assert_equals(res, {
        replica_1_a = true,
        replica_1_b = true,
        replica_1_c = true,
    })
    --
    -- Option 'except'.
    --
    res, err = rs:map_call('test_err_a', {}, {
        timeout = vtest.wait_timeout,
        except = uuid_a,
    })
    t.assert_equals(err, nil)
    t.assert_equals(res, {
        [uuid_b] = {true},
        [uuid_c] = {true},
    })
    --
    -- Cleanup.
    --
    vtest.cluster_exec_each(g, function()
        _G.test_err_a = nil
        _G.test_sleep_do = nil
        _G.test_sleep_c = nil
        _G.test_sleep_is_called = nil
    end)
end

test_group.test_locate_master_when_no_conn_object = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs_cfg = new_cfg_template.sharding[1]
    rs_cfg.master = 'auto'
    rs_cfg.replicas.replica_1_a.master = nil
    local new_global_cfg = vtest.config_new(new_cfg_template)
    local replicasets = vreplicaset.buildall(new_global_cfg)
    local _, rs = next(replicasets)
    t.assert_equals(rs.master, nil)
    for _, r in pairs(rs.replicas) do
        t.assert_equals(r.conn, nil)
    end
    t.assert(rs.is_master_auto)
    --
    -- First attempt to locate the masters only creates the connections, but
    -- doesn't wait for their establishment. The call is supposed to be retried
    -- later.
    --
    local is_all_done, is_all_nop, last_err =
        vreplicaset.locate_masters(replicasets)
    t.assert_equals(last_err, nil)
    t.assert(not is_all_done)
    t.assert(not is_all_nop)
    for _, r in pairs(rs.replicas) do
        t.assert_not_equals(r.conn, nil)
        r.conn:wait_connected(vtest.wait_timeout)
    end
    is_all_done, is_all_nop, last_err =
        vreplicaset.locate_masters(replicasets)
    t.assert_equals(last_err, nil)
    t.assert(is_all_done)
    t.assert(not is_all_nop)
    t.assert_equals(rs.master, rs.replicas[g.replica_1_a:instance_uuid()])
end
