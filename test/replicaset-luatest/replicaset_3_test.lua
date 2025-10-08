local fiber = require('fiber')
local t = require('luatest')
local vreplicaset = require('vshard.replicaset')
local vtest = require('test.luatest_helpers.vtest')
local verror = require('vshard.error')
local vconsts = require('vshard.consts')
local vutil = require('vshard.util')
local vconst = require('vshard.consts')
local server = require('test.luatest_helpers.server')
local vcfg = require('vshard.cfg')

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
    global_cfg = vcfg.check(vtest.config_new(cfg_template))

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

local function get_auto_master_global_cfg()
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs_cfg = new_cfg_template.sharding[1]
    rs_cfg.master = 'auto'
    rs_cfg.replicas.replica_1_a.master = nil
    return vcfg.check(vtest.config_new(new_cfg_template))
end

test_group.test_locate_master_when_no_conn_object = function(g)
    local new_global_cfg = get_auto_master_global_cfg()
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

--
-- gh-ee-4: when a known master is disconnected, the master locator would keep
-- trying to hit the old instance. It should instead try to locate a new master
-- when the old one is disconnected.
--
test_group.test_locate_master_after_disconnect = function(g)
    local new_global_cfg = get_auto_master_global_cfg()
    new_global_cfg.read_only = true
    vtest.cluster_cfg(g, new_global_cfg)
    g.replica_1_a:update_box_cfg{read_only = false}

    local replicasets = vreplicaset.buildall(new_global_cfg)
    local _, rs = next(replicasets)
    local is_all_done = vreplicaset.locate_masters(replicasets)
    t.assert(not is_all_done)
    for _, r in pairs(rs.replicas) do
        r.conn:wait_connected(vtest.wait_timeout)
    end
    is_all_done = vreplicaset.locate_masters(replicasets)
    t.assert(is_all_done)
    t.assert_equals(rs.master, rs.replicas[g.replica_1_a:instance_uuid()])
    --
    -- Master dies and is switched.
    --
    g.replica_1_a:stop()
    g.replica_1_b:update_box_cfg{read_only = false}
    local ok, err = rs:callrw('get_uuid', {}, {timeout = 0.01})
    t.assert_not_equals(err, nil)
    t.assert(not ok)
    --
    -- Discovery won't be stuck on just the broken master.
    --
    is_all_done = vreplicaset.locate_masters(replicasets)
    t.assert(is_all_done)
    t.assert_equals(rs.master, rs.replicas[g.replica_1_b:instance_uuid()])

    -- Restore.
    g.replica_1_b:update_box_cfg{read_only = true}
    g.replica_1_a:start()
    vtest.cluster_cfg(g, global_cfg)
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

    g.replica_1_a:exec(function(uuid_a)
        box.cfg{force_recovery = true}
        box.space._cluster:replace({box.info.id, uuid_a})
        box.cfg{force_recovery = false}
    end, {uuid_a})
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

local function hang_get_uuid(instance)
    instance:exec(function()
        rawset(_G, 'sleep_num', 0)
        rawset(_G, 'sleep_cond', ifiber.cond())
        rawset(_G, 'old_get_uuid', _G.get_uuid)
        _G.get_uuid = function()
            _G.sleep_num = _G.sleep_num + 1
            _G.sleep_cond:wait()
        end
    end)
end

local function reset_sleep(instance)
    instance:exec(function()
        _G.sleep_cond:broadcast()
        _G.sleep_num = 0
    end)
end

local function reset_get_uuid(instance)
    instance:exec(function()
        _G.sleep_cond:broadcast()
        _G.get_uuid = _G.old_get_uuid
        _G.old_get_uuid = nil
        _G.sleep_cond = nil
        _G.sleep_num = nil
    end)
end

local function prepare_stateless_balancing_rs()
    local new_cfg_template = table.deepcopy(cfg_template)
    -- replica_1_b > replica_1_a (master) > replica_1_c
    new_cfg_template.sharding[1].replicas.replica_1_b.zone = 2
    new_cfg_template.sharding[1].replicas.replica_1_a.zone = 3
    new_cfg_template.sharding[1].replicas.replica_1_c.zone = 4
    new_cfg_template.zone = 1
    new_cfg_template.weights = {
        [1] = {
            [2] = 1,
            [3] = 2,
            [4] = 3,
        }
    }

    local new_global_cfg = vtest.config_new(new_cfg_template)
    new_global_cfg = vcfg.check(new_global_cfg)
    local _, rs = next(vreplicaset.buildall(new_global_cfg))
    rs:wait_connected_all(timeout_opts)
    return rs
end

test_group.test_stateless_balancing_callro = function(g)
    local rs = prepare_stateless_balancing_rs()
    -- replica_1_b is the prioritized one.
    local uuid = rs:callro('get_uuid', {}, timeout_opts)
    t.assert_equals(uuid, g.replica_1_b:instance_uuid())

    --
    -- callro fallback to lowest prioritized replica , if request to other
    -- instances fail.
    --
    hang_get_uuid(g.replica_1_a)
    hang_get_uuid(g.replica_1_b)
    local request_timeout_opts = {request_timeout = 1, timeout = 3}
    uuid = rs:callro('get_uuid', {}, request_timeout_opts)
    t.assert_equals(uuid, g.replica_1_c:instance_uuid())
    t.assert_equals(g.replica_1_a:eval('return _G.sleep_num'), 1)
    t.assert_equals(g.replica_1_b:eval('return _G.sleep_num'), 1)
    reset_sleep(g.replica_1_a)
    reset_sleep(g.replica_1_b)

    --
    -- If all instances are unresponsive, there's nothing we can do.
    -- Test, that when timeout > request_timeout * #rs, then we make
    -- several requests to some replicas.
    --
    local err
    hang_get_uuid(g.replica_1_c)
    request_timeout_opts.timeout = 3.5
    uuid, err = rs:callro('get_uuid', {}, request_timeout_opts)
    t.assert_equals(uuid, nil)
    -- Either 'timed out' or 'Timeout exceeded' message. Depends on version.
    t.assert(verror.is_timeout(err))
    t.assert_equals(g.replica_1_b:eval('return _G.sleep_num'), 2)
    t.assert_equals(g.replica_1_c:eval('return _G.sleep_num'), 1)
    t.assert_equals(g.replica_1_a:eval('return _G.sleep_num'), 1)

    reset_get_uuid(g.replica_1_a)
    reset_get_uuid(g.replica_1_b)
    reset_get_uuid(g.replica_1_c)
end

test_group.test_stateless_balancing_callre = function(g)
    local rs = prepare_stateless_balancing_rs()
    -- replica_1_b is the prioritized one.
    local uuid = rs:callre('get_uuid', {}, timeout_opts)
    t.assert_equals(uuid, g.replica_1_b:instance_uuid())

    --
    -- callre fallback to another replica, if request fail.
    --
    hang_get_uuid(g.replica_1_b)
    local request_timeout_opts = {request_timeout = 1, timeout = 3}
    uuid = rs:callre('get_uuid', {}, request_timeout_opts)
    t.assert_equals(uuid, g.replica_1_c:instance_uuid())
    t.assert_equals(g.replica_1_b:eval('return _G.sleep_num'), 1)
    reset_sleep(g.replica_1_b)

    --
    -- If all replicas are unresponsive, fallback to the master.
    --
    hang_get_uuid(g.replica_1_c)
    uuid = rs:callre('get_uuid', {}, request_timeout_opts)
    t.assert_equals(uuid, g.replica_1_a:instance_uuid())
    t.assert_equals(g.replica_1_b:eval('return _G.sleep_num'), 1)
    t.assert_equals(g.replica_1_c:eval('return _G.sleep_num'), 1)
    reset_sleep(g.replica_1_b)
    reset_sleep(g.replica_1_c)

    --
    -- If it's not enough time to make requests to all replicas and then
    -- to master, we won't fallback to master and will fail earlier.
    --
    local err
    request_timeout_opts.timeout = 1.5
    uuid, err = rs:callre('get_uuid', {}, request_timeout_opts)
    t.assert_equals(uuid, nil)
    -- Either 'timed out' or 'Timeout exceeded' message. Depends on version.
    t.assert(verror.is_timeout(err))
    t.assert_equals(g.replica_1_b:eval('return _G.sleep_num'), 1)
    t.assert_equals(g.replica_1_c:eval('return _G.sleep_num'), 1)

    reset_get_uuid(g.replica_1_b)
    reset_get_uuid(g.replica_1_c)
end

--
-- gh-518: connection was recreated on every call, when
-- instance is down.
--
test_group.test_conn_is_not_recreated = function(g)
    local _, rs = next(vreplicaset.buildall(global_cfg))
    rs:wait_connected(vtest.wait_timeout)

    -- Kill master, the connection should not be recreated.
    vtest.storage_stop(g.replica_1_a)
    local conn = rs.master.conn
    rs:callrw('echo', {'hello'})
    t.assert_equals(conn, rs.master.conn)

    vtest.storage_start(g.replica_1_a, global_cfg)
    rs:wait_connected(vtest.wait_timeout)
end

--
-- gh-537: router was unable to find a new master if old one hangs.
--
test_group.test_locate_master_when_old_master_hangs = function(g)
    -- freeze() and thaw() are available only since Tarantool 2.4.1.
    t.run_only_if(vutil.version_is_at_least(2, 4, 1, nil, 0, 0))

    local new_global_cfg = get_auto_master_global_cfg()
    local _, rs = next(vreplicaset.buildall(new_global_cfg))
    rs:wait_connected_all(timeout_opts)

    -- Find master.
    t.assert(rs:locate_master())
    t.assert_equals(rs.master.uuid, g.replica_1_a:instance_uuid())

    -- Change master in the replicaset.
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs_cfg = new_cfg_template.sharding[1]
    rs_cfg.replicas.replica_1_a.master = nil
    rs_cfg.replicas.replica_1_b.master = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    -- Make the test faster.
    local old_timeout = vconst.MASTER_SEARCH_TIMEOUT
    vconst.MASTER_SEARCH_TIMEOUT = 0.5

    -- Old master hangs.
    g.replica_1_a:freeze()

    -- Replicaset is able to locate a new one.
    local start_ts = fiber.clock()
    local is_done, is_nop = rs:locate_master()
    t.assert_lt(fiber.clock() - start_ts, 2 * vconst.MASTER_SEARCH_TIMEOUT)
    t.assert(is_done)
    t.assert_not(is_nop)
    t.assert_equals(rs.master.uuid, g.replica_1_b:instance_uuid())

    g.replica_1_a:thaw()
    vconst.MASTER_SEARCH_TIMEOUT = old_timeout
    vtest.cluster_cfg(g, global_cfg)
end

--
-- gh-537: test, that locate_master is NoOp, when master doesn't respond
-- but there's no another master in replicaset.
--
test_group.test_locate_master_noop_when_no_master_with_hanged_one = function(g)
    -- freeze() and thaw() are available only since Tarantool 2.4.1.
    t.run_only_if(vutil.version_is_at_least(2, 4, 1, nil, 0, 0))

    local new_global_cfg = get_auto_master_global_cfg()
    local _, rs = next(vreplicaset.buildall(new_global_cfg))
    rs:wait_connected_all(timeout_opts)

    -- Find master.
    t.assert(rs:locate_master())
    t.assert_equals(rs.master.uuid, g.replica_1_a:instance_uuid())

    -- Make the test faster.
    local old_timeout = vconst.MASTER_SEARCH_TIMEOUT
    vconst.MASTER_SEARCH_TIMEOUT = 0.5

    -- Old master hangs.
    g.replica_1_a:freeze()

    -- Locate master doesn't change the master, since new one is not found.
    local is_done, is_nop = rs:locate_master()
    t.assert_not(is_done)
    t.assert_not(is_nop)
    t.assert_equals(rs.master.uuid, g.replica_1_a:instance_uuid())

    g.replica_1_a:thaw()
    vconst.MASTER_SEARCH_TIMEOUT = old_timeout
    vtest.cluster_cfg(g, global_cfg)
end

--
-- gh-513: vshard.storage.info() should not fail due to replica.upstream
-- being nil.
--
test_group.test_storage_info_fail_during_replica_disconnection = function(g)
    g.replica_1_b:exec(function()
        local old_replication = box.cfg.replication
        -- replica_1_b disconnects from the master (replica_1_a)
        box.cfg{replication = {}}
        t.assert_equals(ivshard.storage.info().replication, {})
        t.assert_equals(ivshard.storage.info().alerts[1][1],
                        'UNREACHABLE_MASTER')
        box.cfg{replication = old_replication}
    end)
end

test_group.test_storage_info_fail_while_replica_has_master_name = function(g)
    t.run_only_if(vutil.feature.persistent_names)

    local replica = server:new({
        alias = 'non_config_replica_with_master_name',
        box_cfg = {
            replication = {
                g.replica_1_a.net_box_uri,
                g.replica_1_b.net_box_uri,
                g.replica_1_c.net_box_uri,
            },
            instance_name = 'replica_1_a'
        }
    })

    replica:start()
    replica:wait_for_vclock_of(g.replica_1_a)
    local id = replica:instance_id()

    g.replica_1_b:exec(function()
        t.assert_not_equals(ivshard.storage.info().replication, {})
    end)

    replica:stop()
    replica:drop()
    g.replica_1_a:exec(function(id)
        box.space._cluster:delete(id)
    end, {id})
end

-- gh-408: connection doesn't fetch schema.
--
-- The behavior of the `fetch_schema` connection option is tested inside
-- of Tarantool, so let's not copy-paste this test here, simply check the
-- `opts.fetch_schema` flag inside the connection.
--
test_group.test_conn_does_not_fetch_schema = function()
    -- Default `connection_fetch_schema` value
    local new_cfg = vcfg.check(global_cfg)
    local _, rs = next(vreplicaset.buildall(new_cfg))
    rs:wait_connected(vtest.wait_timeout)
    t.assert(rs.master.conn.opts.fetch_schema)

    -- A config with manually disabled `connection_fetch_schema`
    new_cfg.connection_fetch_schema = false
    _, rs = next(vreplicaset.buildall(new_cfg))
    rs:wait_connected(vtest.wait_timeout)
    t.assert_not(rs.master.conn.opts.fetch_schema)
end

test_group.test_rebinding_fetch_schema = function()
    local new_cfg = table.deepcopy(global_cfg)

    for _, old_fetch_schema in pairs({ true, false }) do
        for _, fetch_schema in pairs({ true, false }) do
            new_cfg.connection_fetch_schema = old_fetch_schema
            local old_replicasets = vreplicaset.buildall(new_cfg)
            local _, rs = next(old_replicasets)
            rs:wait_connected(vtest.wait_timeout)
            t.assert_equals(rs.master.conn.opts.fetch_schema, old_fetch_schema)

            new_cfg.connection_fetch_schema = fetch_schema
            local replicasets = vreplicaset.buildall(new_cfg)
            _, rs = next(replicasets)
            vreplicaset.rebind_replicasets(replicasets, old_replicasets)

            if old_fetch_schema == fetch_schema then
                t.assert_not_equals(rs.master.conn, nil)
                t.assert_equals(rs.master.conn.opts.fetch_schema, fetch_schema)
            else
                t.assert_equals(rs.master.conn, nil)
            end
        end
    end

end

local function worker_register_service(service_name, func)
    vreplicaset._worker_service_to_func[service_name] = func
end

local function worker_unregister_service(service_name)
    vreplicaset._worker_service_to_func[service_name] = nil
end

test_group.test_worker_basic = function()
    local replicasets = vreplicaset.buildall(global_cfg)
    local _, rs = next(replicasets)

    -- Workers are created manually.
    t.assert_equals(rs.worker, nil)
    for _, replica in pairs(rs.replicas) do
        t.assert_equals(replica.worker, nil)
    end
    vreplicaset.create_workers(replicasets)
    t.assert_not_equals(rs.worker, nil)
    t.assert_not_equals(rs.worker.fiber:status(), 'dead')
    for _, replica in pairs(rs.replicas) do
        t.assert_not_equals(replica, nil)
        t.assert_not_equals(replica.worker.fiber:status(), 'dead')
    end

    local function step_func(_, data)
        data.test = 'world'
        -- Step function returns timeout for worker until the next step.
        return vconsts.TIMEOUT_INFINITY
    end

    -- Test basic service work.
    local service_name = 'basic_step'
    worker_register_service(service_name, step_func)
    rs.worker:add_service(service_name)
    for _, r in pairs(rs.replicas) do
        r.worker:add_service(service_name)
    end

    local function assert_service_data_equals(rs, service, expected)
        t.assert_not_equals(rs.worker.services[service], nil)
        t.assert_equals(rs.worker.services[service].data, expected)
        for _, r in pairs(rs.replicas) do
            t.assert_equals(r.worker.services[service].data, expected)
        end
    end
    assert_service_data_equals(rs, service_name, {})
    fiber.yield()
    assert_service_data_equals(rs, service_name, {test = 'world'})

    -- On outdate the fiber is killed.
    vreplicaset.outdate_replicasets(replicasets)
    fiber.yield()
    t.assert_equals(rs.worker.fiber:status(), 'dead')
    for _, replica in pairs(rs.replicas) do
        t.assert_equals(replica.worker.fiber:status(), 'dead')
    end

    -- Cleanup.
    worker_unregister_service(service_name)
end

test_group.test_worker_several_services = function()
    local replicasets = vreplicaset.buildall(global_cfg)
    vreplicaset.create_workers(replicasets)
    local _, rs = next(replicasets)

    local function step_template(timeout)
        return function(_, data)
            if not data.counter then
                data.counter = 1
                return timeout
            end
            data.counter = data.counter + 1
            return timeout
        end
    end

    local service_name_1, service_name_2 = 'one', 'two'
    -- Runs on every fiber.yield
    worker_register_service(service_name_1, step_template(0))
    -- Runs once, the next execution is after 500 years.
    worker_register_service(service_name_2,
                            step_template(vconsts.TIMEOUT_INFINITY))
    rs.worker:add_service(service_name_1)
    rs.worker:add_service(service_name_2)
    t.assert_equals(rs.worker.services[service_name_1].data, {})
    t.assert_equals(rs.worker.services[service_name_2].data, {})
    fiber.yield()
    t.assert_equals(rs.worker.services[service_name_1].data, {counter = 1})
    t.assert_equals(rs.worker.services[service_name_2].data, {counter = 1})
    fiber.yield()
    t.assert_equals(rs.worker.services[service_name_1].data, {counter = 2})
    t.assert_equals(rs.worker.services[service_name_2].data, {counter = 1})

    -- Cleanup.
    vreplicaset.outdate_replicasets(replicasets)
    worker_unregister_service(service_name_1)
    worker_unregister_service(service_name_2)
end

test_group.test_worker_service_info = function()
    local rss = vreplicaset.buildall(global_cfg)
    vreplicaset.create_workers(rss)
    local _, rs = next(rss)

    local service_name = 'worker_info'
    local function step_func(_, data)
        if not data.info then
            data.info = require('vshard.service_info').new(service_name)
        end
        return vconst.TIMEOUT_INFINITY
    end

    local function check_new_info(info)
        t.assert_equals(info.name, service_name)
        t.assert_equals(info.status, 'ok')
        t.assert_equals(info.status_idx, 0)
        t.assert_equals(info.activity, 'unknown')
        t.assert_equals(info.error, '')
    end

    -- Check info of the replicaset only.
    local rs_service_name = 'replicaset_' .. service_name
    local r_service_name = 'replica_' .. service_name
    worker_register_service(rs_service_name, step_func)
    worker_register_service(r_service_name, step_func)
    rs.worker:add_service(rs_service_name)
    fiber.yield()
    local info = rs:service_info(service_name)
    check_new_info(info)
    t.assert_equals(info.replicas, nil)

    -- Check info of the replicaset and replica services.
    for _, replica in pairs(rs.replicas) do
        replica.worker:add_service(r_service_name)
    end
    fiber.yield()
    info = rs:service_info(service_name)
    check_new_info(info)
    t.assert_not_equals(info.replicas, nil)
    for _, i in pairs(info.replicas) do
        check_new_info(i)
    end

    -- Check info of replicas only.
    rs.worker:remove_service(rs_service_name)
    info = rs:service_info(service_name)
    t.assert_equals(info.name, nil)
    t.assert_not_equals(info.replicas, nil)
    for _, i in pairs(info.replicas) do
        check_new_info(i)
    end

    worker_register_service('replicaset_' .. service_name)
    worker_register_service('replica_' .. service_name)
end

test_group.test_failover_interval = function(g)
    -- Set huge timeout.
    local new_global_cfg = table.deepcopy(global_cfg)
    new_global_cfg = vcfg.check(new_global_cfg)
    new_global_cfg.failover_interval = vconst.TIMEOUT_INFINITY
    new_global_cfg.failover_ping_timeout = 0.5
    local replicasets = vreplicaset.buildall(new_global_cfg)
    vreplicaset.create_workers(replicasets)
    local _, rs = next(replicasets)
    rs:wait_connected_all(timeout_opts)

    local uuid_a = g.replica_1_a:instance_uuid()
    vtest.storage_stop(g.replica_1_a)
    t.helpers.retrying(timeout_opts, function()
        t.assert_equals(rs.replicas[uuid_a].replication_status,
                        vconst.STATUS.YELLOW)
        rs.replicas[uuid_a].worker:wakeup_service('replica_failover')
    end)

    vtest.storage_start(g.replica_1_a, global_cfg)
    -- The value can be reconfigured. Simulate reconfiguration.
    new_global_cfg.failover_interval = 0.1
    local new_replicasets = vreplicaset.buildall(new_global_cfg)
    vreplicaset.rebind_replicasets(new_replicasets, replicasets)
    -- Skip connections, this test is not about them.
    vreplicaset.outdate_replicasets(replicasets)
    vreplicaset.create_workers(new_replicasets)
    for _, replicaset in pairs(new_replicasets) do
        replicaset.worker:add_service('replicaset_failover')
        for _, replica in pairs(replicaset.replicas) do
            replica.worker:add_service('replica_failover')
        end
    end

    local _, new_rs = next(new_replicasets)
    t.helpers.retrying(timeout_opts, function()
        t.assert_equals(new_rs.replicas[uuid_a].replication_status,
                        vconst.STATUS.GREEN)
    end)
end

test_group.test_locate_master_not_hangs_on_futures = function(g)
    -- freeze() and thaw() are available only since Tarantool 2.4.1.
    t.run_only_if(vutil.version_is_at_least(2, 4, 1, nil, 0, 0))

    local new_global_cfg = get_auto_master_global_cfg()
    local _, rs = next(vreplicaset.buildall(new_global_cfg))
    rs:wait_connected_all(timeout_opts)

    -- Find the master.
    t.assert(rs:locate_master())
    t.assert_equals(rs.master.uuid, g.replica_1_a:instance_uuid())

    -- Change the master to replica_1_c.
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs_cfg = new_cfg_template.sharding[1]
    rs_cfg.replicas.replica_1_a.master = nil
    rs_cfg.replicas.replica_1_c.master = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    -- Freeze the node, so that one of the futures hangs.
    g.replica_1_b:freeze()

    local start_time = fiber.clock()
    local is_done, is_nop = rs:locate_master()
    t.assert_lt(fiber.clock() - start_time, vconst.MASTER_SEARCH_TIMEOUT)
    t.assert(is_done)
    t.assert_not(is_nop)
    t.assert_equals(rs.master.uuid, g.replica_1_c:instance_uuid())

    g.replica_1_b:thaw()
    vtest.cluster_cfg(g, global_cfg)
end

local function assert_rs_tostring_correctness(rs, is_master_missed)
    local rs_tostring_template = 'replicaset(id=\"%s\", master=%s)'
    local master_str = is_master_missed and 'missing' or rs.master
    local rs_str = string.format(rs_tostring_template, rs.id, master_str)
    t.assert_equals(tostring(rs), rs_str)
end

local function assert_replica_tostring_correctness(replica)
    local replica_tostring_template = '%s(%s)'
    local replica_id = replica.name or replica.uuid
    local replica_str = string.format(replica_tostring_template, replica_id,
                                      vreplicaset.replica_safe_uri(replica))
    t.assert_equals(tostring(replica), replica_str)
end

local function assert_full_rs_tostring_correctness(replicasets,
                                                   is_master_missed)
    for _, rs in pairs(replicasets) do
        assert_rs_tostring_correctness(rs, is_master_missed)
        for _, replica in pairs(rs.replicas) do
            assert_replica_tostring_correctness(replica)
        end
    end
end

--
-- gh-597: outdating the replicasets leads to broken replicas in logs.
--
test_group.test_tostring_of_outdated_replicasets_and_replicas = function()
    local replicasets = vreplicaset.buildall(global_cfg)
    assert_full_rs_tostring_correctness(replicasets, false)
    vreplicaset.outdate_replicasets(replicasets)
    assert_full_rs_tostring_correctness(replicasets, false)

    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].replicas.replica_1_a.master = false
    global_cfg = vcfg.check(vtest.config_new(new_cfg_template))
    local replicasets_with_no_master = vreplicaset.buildall(global_cfg)
    assert_full_rs_tostring_correctness(replicasets_with_no_master, true)
    vreplicaset.outdate_replicasets(replicasets_with_no_master)
    assert_full_rs_tostring_correctness(replicasets_with_no_master, true)
end
