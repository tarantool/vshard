local fiber = require('fiber')
local t = require('luatest')
local vreplicaset = require('vshard.replicaset')
local vtest = require('test.luatest_helpers.vtest')
local verror = require('vshard.error')
local vutil = require('vshard.util')

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
    bucket_count = 20,
    test_user_grant_range = 'super',
    replication_timeout = 0.1,
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
    t.assert(verror.is_timeout(err))
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

test_group.test_named_replicaset = function(g)
    t.run_only_if(vutil.feature.persistent_names)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.identification_mode = 'name_as_key'
    new_cfg_template.sharding['replicaset'] = new_cfg_template.sharding[1]
    new_cfg_template.sharding[1] = nil

    local new_global_cfg = vtest.config_new(new_cfg_template)
    local replicasets = vreplicaset.buildall(new_global_cfg)

    -- Assert, that no UUID identification in replicaset is used.
    local rs = replicasets.replicaset
    local replica_1_a = rs.replicas.replica_1_a
    t.assert_not_equals(rs, nil)
    t.assert_not_equals(replica_1_a, nil)
    t.assert_equals(rs.uuid, nil)
    t.assert_equals(replica_1_a.uuid, nil)
    t.assert_equals(rs.name, 'replicaset')
    t.assert_equals(replica_1_a.name, 'replica_1_a')
    t.assert_equals(rs.id, rs.name)
    t.assert_equals(replica_1_a.id, replica_1_a.name)

    -- Name is not set, uuid is not set, name mismatch error.
    local ret, err = rs:callrw('get_uuid', {}, {timeout = 5})
    t.assert_equals(err.name, 'INSTANCE_NAME_MISMATCH')
    t.assert_equals(ret, nil)

    local uuid_a = g.replica_1_a:instance_uuid()
    -- Test, that NAME_MISMATCH error is skipped, when uuid is specified.
    -- Before the name configuration, as a name cannot be dropped. New
    -- replicaset in order not to rebuild it for the name configuration.
    new_global_cfg.sharding['replicaset'].replicas['replica_1_a'].uuid =
        g.replica_1_a:instance_uuid()
    local rs_2 = vreplicaset.buildall(new_global_cfg).replicaset
    ret, err = rs_2:callrw('get_uuid', {}, timeout_opts)
    t.assert_equals(err, nil)
    t.assert_equals(ret, uuid_a)

    -- Set name, everything works from now on.
    g.replica_1_a:exec(function() box.cfg{instance_name = 'replica_1_a'} end)
    ret, err = rs:callrw('get_uuid', {}, timeout_opts)
    t.assert_equals(err, nil)
    t.assert_equals(ret, uuid_a)

    -- Test, that name identification works.
    vtest.storage_stop(g.replica_1_b)
    local err_id
    ret, err, err_id = rs:map_call('get_uuid', {}, small_timeout_opts)
    t.assert_not_equals(err, nil)
    t.assert_equals(err_id, g.replica_1_b.alias)
    t.assert_equals(ret, nil)
    vtest.storage_start(g.replica_1_b, global_cfg)
end

test_group.test_ipv6_uri = function(g)
    local new_cfg = table.deepcopy(global_cfg)
    local rs_uuid = g.replica_1_a:replicaset_uuid()
    local uuid = g.replica_1_a:instance_uuid()
    new_cfg.sharding[rs_uuid].replicas[uuid].uri = 'storage:storage@[::1]:3301'
    local _, rs = next(vreplicaset.buildall(new_cfg))
    local replica_string = 'replica_1_a(storage@[::1]:3301)'
    t.assert_equals(tostring(rs.master), replica_string)
end
