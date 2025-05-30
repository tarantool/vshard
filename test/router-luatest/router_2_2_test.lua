local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local vconsts = require('vshard.consts')
local vserver = require('test.luatest_helpers.server')

local g = t.group('router')
local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
                replica_2_b = {},
            },
        },
    },
    bucket_count = 100,
    test_user_grant_range = 'super',
}
local global_cfg = vtest.config_new(cfg_template)

local function callrw_get_uuid(bid, timeout)
    timeout = timeout ~= nil and timeout or iwait_timeout
    return ivshard.router.callrw(bid, 'get_uuid', {}, {timeout = timeout})
end

local function callrw_session_get(bid, key)
    return ivshard.router.callrw(bid, 'session_get', {key},
                                 {timeout = iwait_timeout})
end

local function callrw_session_set(bid, key, value)
    return ivshard.router.callrw(bid, 'session_set', {key, value},
                                 {timeout = iwait_timeout})
end

-- Check whether errors are the same and their messages contain str
local function assert_errors_equals(err1, err2, str)
    t.assert_equals(err1, err2)
    t.assert_not_equals(err1, nil)
    t.assert_str_contains(err1.message, str)
end

g.before_all(function()
    vtest.cluster_new(g, global_cfg)

    t.assert_equals(g.replica_1_a:exec(function()
        return #ivshard.storage.info().alerts
    end), 0, 'no alerts after boot')

    local router = vtest.router_new(g, 'router', global_cfg)
    g.router = router
    local res, err = router:exec(function()
        return ivshard.router.bootstrap({timeout = iwait_timeout})
    end)
    t.assert(res and not err, 'bootstrap buckets')
end)

g.after_all(function()
    g.cluster:drop()
end)

g.test_basic = function(g)
    local router = g.router
    local res, err = router:exec(function()
        return ivshard.router.callrw(1, 'echo', {1}, {timeout = iwait_timeout})
    end)
    t.assert(not err, 'no error')
    t.assert_equals(res, 1, 'good result')
end

g.test_msgpack_args = function(g)
    t.run_only_if(vutil.feature.msgpack_object)
    --
    -- Normal call ro.
    --
    local router = g.router
    local res, err = router:exec(function()
        local args = imsgpack.object({100})
        return ivshard.router.callrw(1, 'echo', args, {timeout = iwait_timeout})
    end)
    t.assert(not err, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Normal call rw.
    --
    res, err = router:exec(function()
        local args = imsgpack.object({100})
        return ivshard.router.callro(1, 'echo', args, {timeout = iwait_timeout})
    end)
    t.assert(not err, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Direct call ro.
    --
    res, err = router:exec(function()
        local args = imsgpack.object({100})
        local route = ivshard.router.route(1)
        return route:callro('echo', args, {timeout = iwait_timeout})
    end)
    t.assert(err == nil, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Direct call rw.
    --
    res, err = router:exec(function()
        local args = imsgpack.object({100})
        local route = ivshard.router.route(1)
        return route:callrw('echo', args, {timeout = iwait_timeout})
    end)
    t.assert(err == nil, 'no error')
    t.assert_equals(res, 100, 'good result')
end

local function test_return_raw_template(g, mode)
    --
    -- Normal call.
    --
    -- luacheck: ignore 113/add_details
    local router = g.router
    local res = router:exec(function(mode)
        return add_details(ivshard.router[mode](1, 'echo', {1, 2, 3},
                           {timeout = iwait_timeout, return_raw = true}))
    end, {mode})
    t.assert_equals(res.val, {1, 2, 3}, 'value value')
    t.assert_equals(res.val_type, 'userdata', 'value type')
    t.assert(not res.err, 'no error')

    --
    -- Route call.
    --
    res = router:exec(function(mode)
        local route = ivshard.router.route(1)
        return add_details(route[mode](route, 'echo', {1, 2, 3},
                           {timeout = iwait_timeout, return_raw = true}))
    end, {mode})
    t.assert_equals(res.val, {1, 2, 3}, 'value value')
    t.assert_equals(res.val_type, 'userdata', 'value type')
    t.assert(not res.err, 'no error')

    --
    -- Empty result set.
    --
    res = router:exec(function(mode)
        return add_details(ivshard.router[mode](1, 'echo', {},
                           {timeout = iwait_timeout, return_raw = true}))
    end, {mode})
    t.assert(not res.val, 'no value')
    t.assert(not res.err, 'no error')

    --
    -- Error.
    --
    res = router:exec(function(mode)
        return add_details(ivshard.router[mode](1, 'box_error', {1, 2, 3},
                           {timeout = iwait_timeout}))
    end, {mode})
    t.assert(not res.val, 'no value')
    t.assert_equals(res.err_type, 'table', 'error type')
    t.assert_covers(res.err, {type = 'ClientError', code = box.error.PROC_LUA},
                    'error value')

    --
    -- Route error.
    --
    res = router:exec(function(mode)
        local route = ivshard.router.route(1)
        return add_details(route[mode](route, 'box_error', {1, 2, 3},
                           {timeout = iwait_timeout}))
    end, {mode})
    t.assert(not res.val, 'no value')
    t.assert_equals(res.err_type, 'table', 'error type')
    t.assert_covers(res.err, {type = 'ClientError', code = box.error.PROC_LUA},
                    'error value')
end

g.test_return_raw = function(g)
    t.run_only_if(vutil.feature.netbox_return_raw)

    g.router:exec(function()
        rawset(_G, 'add_details', function(val, err)
            -- Direct return would turn nils into box.NULLs. The tests want to
            -- ensure it doesn't happen. Table wrap makes the actual nils
            -- eliminate themselves.
            return {
                val = val,
                val_type = type(val),
                err = err,
                err_type = type(err),
            }
        end)
    end)
    test_return_raw_template(g, 'callrw')
    test_return_raw_template(g, 'callro')
    g.router:exec(function()
        _G.add_details = nil
    end)
end

g.test_group_buckets = function(g)
    local bids = vtest.storage_get_n_buckets(g.replica_1_a, 2)
    local val, err = g.router:exec(function(bid1, bid2)
        return ivshard.router._buckets_group({bid2, bid1, bid1, bid2},
                                             ivtest.wait_timeout)
    end, {bids[1], bids[2]})
    assert(err == nil)
    local rs1_uuid = g.replica_1_a:replicaset_uuid()
    local expected = {
        [rs1_uuid] = {bids[2], bids[1]},
    }
    t.assert_equals(val, expected)
end

g.test_uri_compare_and_reuse = function(g)
    -- Multilisten itself is not used, but URI-table is supported since the same
    -- version.
    t.run_only_if(vutil.feature.multilisten)

    local rs1_uuid = g.replica_1_a:replicaset_uuid()
    local rep_1_a_uuid = g.replica_1_a:instance_uuid()
    local bid = vtest.storage_first_bucket(g.replica_1_a)
    local res, err

    local new_cfg = vtest.config_new(cfg_template)
    local rs_1_cfg = new_cfg.sharding[rs1_uuid]
    local rep_1_a_cfg = rs_1_cfg.replicas[rep_1_a_uuid]
    t.assert_equals(type(rep_1_a_cfg.uri), 'string', 'URI is a string')

    -- Set a key in the session to check later for a reconnect.
    res, err = g.router:exec(callrw_session_set, {bid, 1, 10})
    t.assert_equals(err, nil, 'no error')
    t.assert(res, 'set session key')

    -- Make the URI a table but it is still the same.
    rep_1_a_cfg.uri = {rep_1_a_cfg.uri}
    vtest.router_cfg(g.router, new_cfg)

    -- The connection is still the same - session key remains.
    res, err = g.router:exec(callrw_session_get, {bid, 1})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, 10, 'get session key')

    -- Restore the globals back.
    vtest.router_cfg(g.router, global_cfg)
end

g.test_multilisten = function(g)
    t.run_only_if(vutil.feature.multilisten)

    local bid = vtest.storage_first_bucket(g.replica_1_a)

    -- Set 2 listen ports on the master.
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs_1_templ = new_cfg_template.sharding[1]
    local rep_1_a_templ = rs_1_templ.replicas.replica_1_a
    rep_1_a_templ.port_count = 2
    -- Clients should use the first port.
    rep_1_a_templ.port_uri = 1
    local new_cluster_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_cluster_cfg)

    -- Router connects to the first port.
    local new_router_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_router_cfg)

    local rep_1_a_uuid = g.replica_1_a:instance_uuid()
    local res, err = g.router:exec(callrw_get_uuid, {bid})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a')

    -- Save a key in the session to check later for a reconnect.
    res, err = g.router:exec(callrw_session_set, {bid, 1, 10})
    t.assert_equals(err, nil, 'no error')
    t.assert(res, 'set session key')

    -- The key is actually saved.
    res, err = g.router:exec(callrw_session_get, {bid, 1})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, 10, 'get session key')

    -- Router connects to the second port. The storage's cfg is intentionally
    -- unchanged.
    rep_1_a_templ.port_uri = 2
    new_router_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_router_cfg)

    res, err = g.router:exec(callrw_get_uuid, {bid})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a again')

    -- There was a reconnect - the session is new.
    res, err = g.router:exec(callrw_session_get, {bid, 1})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, nil, 'no session key')

    -- To confirm that the router uses the second port, shut it down on the
    -- storage. The router won't be able to reconnect.
    rep_1_a_templ.port_count = 1
    rep_1_a_templ.port_uri = 1
    new_cluster_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_cluster_cfg)
    -- Force router reconnect. Otherwise the router would use the old still
    -- alive connection even though the original listening socket is closed
    -- above.
    vtest.router_disconnect(g.router)

    res, err = g.router:exec(callrw_get_uuid, {bid, 0.05})
    t.assert_equals(res, nil, 'rw failed when second port was shut down')
    -- Code can be anything really. Can't check it reliably not depending on OS.
    t.assert_covers(err, {type = 'ClientError'}, 'got error')

    -- Make the router connect to the first port while it still thinks there
    -- are two ports.
    rep_1_a_templ.port_count = 2
    rep_1_a_templ.port_uri = 1
    new_router_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_router_cfg)
    res, err = g.router:exec(callrw_get_uuid, {bid})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a again')

    -- Restore everything back.
    vtest.cluster_cfg(g, global_cfg)
    vtest.router_cfg(g.router, global_cfg)
end

g.test_ssl = function(g)
    t.run_only_if(vutil.feature.ssl)

    -- So as not to assume where buckets are located, find first bucket of the
    -- first replicaset.
    local bid1 = vtest.storage_first_bucket(g.replica_1_a)
    local bid2 = vtest.storage_first_bucket(g.replica_2_a)

    -- Enable SSL everywhere.
    local new_cfg_template = table.deepcopy(cfg_template)
    local sharding_templ = new_cfg_template.sharding
    local rs_1_templ = sharding_templ[1]
    local rs_2_templ = sharding_templ[2]
    rs_1_templ.is_ssl = true
    rs_2_templ.is_ssl = true

    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    vtest.router_cfg(g.router, new_global_cfg)

    local rep_1_a_uuid = g.replica_1_a:instance_uuid()
    local res, err = g.router:exec(callrw_get_uuid, {bid1})
    t.assert_equals(err, nil)
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a')

    local rep_2_a_uuid = g.replica_2_a:instance_uuid()
    res, err = g.router:exec(callrw_get_uuid, {bid2})
    t.assert_equals(err, nil)
    t.assert_equals(res, rep_2_a_uuid, 'went to 2_a')

    -- Ensure that non-encrypted connection won't work.
    rs_2_templ.is_ssl = nil
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_global_cfg)

    res, err = g.router:exec(callrw_get_uuid, {bid2, 0.01})
    t.assert_equals(res, nil, 'rw failed on non-encrypted connection')
    t.assert_covers(err, {code = box.error.NO_CONNECTION}, 'got error')

    -- Works again when the replicaset also disables SSL.
    vtest.cluster_cfg(g, new_global_cfg)

    -- Force a reconnect right now instead of waiting until it happens
    -- automatically.
    vtest.router_disconnect(g.router)
    res, err = g.router:exec(callrw_get_uuid, {bid2})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_2_a_uuid, 'went to 2_a')

    -- Restore everything back.
    vtest.cluster_cfg(g, global_cfg)
    vtest.router_cfg(g.router, global_cfg)
    vtest.cluster_wait_fullsync(g)
end

g.test_enable_disable = function(g)
    --
    -- gh-291: router enable/disable
    --
    local router = vtest.router_new(g, 'router_1')
    router:exec(function(cfg)
        ivtest.clear_test_cfg_options(cfg)
        -- Do not allow router's configuration to complete.
        _G.ivshard.router.internal.errinj.ERRINJ_CFG_DELAY = true
        rawset(_G, 'fiber_static', ifiber.create(ivshard.router.cfg, cfg))
        rawset(_G, 'fiber_new', ifiber.create(ivshard.router.new,
                                              'new_router', cfg))
        _G.fiber_static:set_joinable(true)
        _G.fiber_new:set_joinable(true)
        local routers = ivshard.router.internal.routers
        rawset(_G, 'static_router', routers._static_router)
        rawset(_G, 'new_router', routers.new_router)
    end, {global_cfg})

    local echo_func = function()
        return router:exec(function(timeout)
            local echo = function(router)
                return pcall(router.callrw, router, 1, 'echo', {1},
                             {timeout = timeout})
            end
            local _, ret_val_1 = echo(_G.static_router)
            local _, ret_val_2 = echo(_G.new_router)
            return ret_val_1, ret_val_2
        end, {vtest.wait_timeout})
    end

    local err1, err2 = echo_func()
    assert_errors_equals(err1, err2, 'router is not configured')

    -- unblock router's configuration and wait until it's finished
    router:exec(function()
        _G.ivshard.router.internal.errinj.ERRINJ_CFG_DELAY = false
        _G.fiber_static:join()
        _G.fiber_new:join()
    end)

    -- finally a success
    local ret1, ret2 = echo_func()
    t.assert_equals(ret1, ret2)
    t.assert_equals(ret1, 1)

    -- manual api disabling and enabling
    router:exec(function()
        _G.static_router:disable()
        _G.new_router:disable()
    end)
    err1, err2 = echo_func()
    assert_errors_equals(err1, err2, 'router is disabled explicitly')

    router:exec(function()
        _G.static_router:enable()
        _G.new_router:enable()
    end)
    ret1, ret2 = echo_func()
    t.assert_equals(ret1, ret2)
    t.assert_equals(ret1, 1)

    -- we don't want this server to interfere with subsequent tests
    vtest.drop_instance(g, router)
end

g.test_explicit_fiber_kill = function(g)
    local bids = vtest.cluster_exec_each_master(g, function()
        return _G.get_first_bucket()
    end)

    -- Kill worker fibers of connections to masters
    g.router:exec(function()
        for _, rs in ipairs(ivshard.router.static.replicasets) do
            -- Explicitly kill the connection's worker fiber.
            -- Callback for pushes is executed inside this fiber.
            pcall(function()
                rs.master.conn:eval([[
                    box.session.push(nil)
                ]], {}, {on_push = function()
                    ifiber.self():cancel()
                end})
            end)
        end
    end)

    -- Check that the replicaset is still accessible
    g.router:exec(function(bids)
        for _, bid in ipairs(bids) do
            local res, err = ivshard.router.callrw(bid, 'echo', {1},
                                                   {timeout = iwait_timeout})
            ilt.assert(res, 1)
            ilt.assert(err, nil)
        end
    end, {bids})
end

g.test_simultaneous_cfg = function()
    local router = vtest.router_new(g, 'router_1')

    router:exec(function(cfg)
        ivshard.router.internal.errinj.ERRINJ_CFG_DELAY = true
        ivtest.clear_test_cfg_options(cfg)
        rawset(_G, 'fiber_cfg_static', ifiber.new(ivshard.router.cfg, cfg))
        rawset(_G, 'fiber_cfg_new', ifiber.new(ivshard.router.new,
                                               'new_router', cfg))
        _G.fiber_cfg_static:set_joinable(true)
        _G.fiber_cfg_new:set_joinable(true)
    end, {global_cfg})

    local function routers_cfg()
        return router:exec(function(cfg)
            ivtest.clear_test_cfg_options(cfg)
            local static_router = ivshard.router.internal.routers._static_router
            local new_router = ivshard.router.internal.routers.new_router
            local _, err1 = pcall(ivshard.router.cfg, cfg)
            local _, err2 = pcall(static_router.cfg, static_router, cfg)
            local _, err3 = pcall(new_router.cfg, new_router, cfg)
            return err1, err2, err3
        end, {global_cfg})
    end

    local err1, err2, err3 = routers_cfg()
    assert_errors_equals(err1, err2, '_static_router is in progress')
    t.assert_str_contains(err3.message, 'new_router is in progress')

    router:exec(function()
        ivshard.router.internal.errinj.ERRINJ_CFG_DELAY = false
        _G.fiber_cfg_static:join()
        _G.fiber_cfg_new:join()
    end)

    -- As soon as configuration is done router's reconfiguration is allowed
    err1, err2, err3 = routers_cfg()
    t.assert_equals(err1, err2)
    t.assert_equals(err2, err3)
    t.assert_equals(err1, nil)

    vtest.drop_instance(g, router)
end

g.test_router_service_info = function(g)
    -- Enable master search fiber and logging of the background fibers
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.discovery_mode = 'on'
    for _, rs in pairs(new_cfg_template.sharding) do
        rs.master = 'auto'
        for _, r in pairs(rs.replicas) do
            r.master = nil
        end
    end

    local new_cluster_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_cluster_cfg)

    -- Test that all services save states
    g.router:exec(function()
        ivshard.router.discovery_wakeup()
        local info = ivshard.router.info({with_services = true})
        for _, rs in pairs(info.replicasets) do
            ilt.assert_not_equals(rs.services, nil)
            ilt.assert_not_equals(rs.services.failover, nil)
        end
        ilt.assert_not_equals(info.services, nil)
        ilt.assert_not_equals(info.services.discovery, nil)
        ilt.assert_not_equals(info.services.master_search, nil)
    end)

    -- Restore everything back.
    vtest.router_cfg(g.router, global_cfg)
end

g.test_router_box_cfg_mode = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.box_cfg_mode = 'manual'
    local new_cluster_cfg = vtest.config_new(new_cfg_template)

    -- Basic test.
    g.router:exec(function(cfg)
        -- Unconfigured box doesn't affect router.
        local box_cfg = box.cfg
        box.cfg = function() end
        ivshard.router.cfg(cfg)
        local opts = {timeout = iwait_timeout}
        local res, err = ivshard.router.callrw(1, 'echo', {1}, opts)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, 1)
        box.cfg = box_cfg
    end, {new_cluster_cfg})
    t.assert(g.router:grep_log('Box configuration was skipped'))

    vtest.router_cfg(g.router, global_cfg)
    t.assert(g.router:grep_log('Calling box.cfg()'))
end

local function router_named_cfg_template()
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.identification_mode = 'name_as_key'
    new_cfg_template.sharding['replicaset_1'] = new_cfg_template.sharding[1]
    new_cfg_template.sharding['replicaset_2'] = new_cfg_template.sharding[2]
    new_cfg_template.sharding[1] = nil
    new_cfg_template.sharding[2] = nil
    return new_cfg_template
end

g.test_named_config_identification = function(g)
    t.run_only_if(vutil.feature.persistent_names)
    local new_global_cfg = vtest.config_new(router_named_cfg_template())

    -- Set names, as they should be verified on connection.
    g.replica_1_a:exec(function()
        box.ctl.wait_rw()
        box.cfg{instance_name = 'replica_1_a', replicaset_name = 'replicaset_1'}
    end)
    g.replica_1_b:exec(function()
        box.cfg{instance_name = 'replica_1_b'}
    end)
    g.replica_2_a:exec(function()
        box.ctl.wait_rw()
        box.cfg{instance_name = 'replica_2_a', replicaset_name = 'replicaset_2'}
    end)
    g.replica_2_b:exec(function()
        box.cfg{instance_name = 'replica_2_b'}
    end)
    g.replica_2_a:wait_vclock_of(g.replica_1_a)
    g.replica_2_b:wait_vclock_of(g.replica_2_a)
    vtest.cluster_cfg(g, new_global_cfg)
    vtest.router_cfg(g.router, new_global_cfg)

    local router = g.router
    local res, err = router:exec(function()
        -- buckets_info shows name, when name identification is used.
        local _, err = ivshard.router.route(1)
        ilt.assert_equals(err, nil)
        ilt.assert_not_equals(ivshard.router.buckets_info()[1].name, nil)

        -- vshard.router.info() also shows names.
        local info = ivshard.router.info()
        local rs = info.replicasets['replicaset_1']
        ilt.assert_equals(rs.name, 'replicaset_1')
        ilt.assert_equals(rs.master.name, 'replica_1_a')
        ilt.assert_equals(info.identification_mode, 'name_as_key')

        -- Just a basic test.
        return ivshard.router.callrw(1, 'echo', {1}, {timeout = iwait_timeout})
    end)
    t.assert(not err, 'no error')
    t.assert_equals(res, 1, 'good result')

    -- Restore everything back.
    vtest.cluster_cfg(g, global_cfg)
    vtest.router_cfg(g.router, global_cfg)
end

--
-- gh-ee-4: when a known master is disconnected, the router wouldn't try to find
-- the new one and would keep trying to hit the old instance. It should instead
-- try to locate a new master when the old one is disconnected.
--
g.test_master_discovery_on_disconnect = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    -- Auto-master simplifies master switch which is very needed in this test.
    for _, rs in pairs(new_cfg_template.sharding) do
        rs.master = 'auto'
        for _, r in pairs(rs.replicas) do
            r.master = nil
        end
    end
    local new_cluster_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_cluster_cfg)
    g.replica_1_a:update_box_cfg{read_only = false}
    g.replica_1_b:update_box_cfg{read_only = true}
    g.router:exec(function()
        -- gh-ee-9: old master search service might be still not finished.
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            local router = ivshard.router.internal.static_router
            if router.master_search_service ~= nil then
                error('Old master search still is not canceled')
            end
        end)
    end)
    vtest.router_cfg(g.router, new_cluster_cfg)

    g.router:exec(function()
        local consts = require('vshard.consts')
        rawset(_G, 'test_old_idle_interval', consts.MASTER_SEARCH_IDLE_INTERVAL)
        -- Make sure the router wouldn't accidentally try to re-discover the
        -- master on its own and would only react to an explicit request.
        consts.MASTER_SEARCH_IDLE_INTERVAL = iwait_timeout + 10

        -- Ensure the service enters the long sleep.
        local router = ivshard.router.internal.static_router
        ivtest.service_wait_for_new_ok(router.master_search_service,
            {on_yield = ivshard.router.master_search_wakeup})
    end)

    local bid = vtest.storage_first_bucket(g.replica_1_a)
    local function check_master(uuid)
        local real_uuid = g.router:exec(function(bid)
            local res, err = ivshard.router.callrw(bid, 'get_uuid', {},
                                                   {timeout = iwait_timeout})
            ilt.assert_equals(err, nil)
            return res
        end, {bid})
        t.assert_equals(real_uuid, uuid)
    end
    local function actively_try_to_access_master(storage, opts)
        g.router:exec(function(bid, rs_uuid, instance_uuid, opts)
            local router = ivshard.router.internal.static_router
            local rs = router.replicasets[rs_uuid]
            local new_master = rs.replicas[instance_uuid]
            ilt.helpers.retrying({timeout = iwait_timeout}, function()
                if rs.master == new_master then
                    return
                end
                -- RW calls will notice that the old master is disconnected and
                -- will trigger the router to try and find a new master.
                ivshard.router.callrw(bid, 'echo', {1}, opts)
                error('Wait for master')
            end)
        end, {bid, storage:replicaset_uuid(), storage:instance_uuid(), opts})
    end
    --
    -- Discover the master first time.
    --
    check_master(g.replica_1_a:instance_uuid())
    --
    -- Blocking call while the known master is disconnected.
    --
    g.replica_1_a:stop()
    g.replica_1_b:update_box_cfg{read_only = false}
    actively_try_to_access_master(g.replica_1_b, {timeout = 0.01})
    check_master(g.replica_1_b:instance_uuid())
    --
    -- Async call while the known master is disconnected.
    --
    g.replica_1_a:start()
    vtest.cluster_cfg(g, new_cluster_cfg)
    g.replica_1_b:stop()
    actively_try_to_access_master(g.replica_1_a, {is_async = true})
    check_master(g.replica_1_a:instance_uuid())

    -- Restore everything back.
    g.router:exec(function()
        local consts = require('vshard.consts')
        consts.MASTER_SEARCH_IDLE_INTERVAL = _G.test_old_idle_interval
        _G.test_old_idle_interval = nil
    end)
    g.replica_1_b:start()
    vtest.router_cfg(g.router, global_cfg)
    vtest.cluster_cfg(g, global_cfg)
end

g.test_request_timeout = function(g)
    local bid = vtest.storage_first_bucket(g.replica_1_a)
    --
    -- Test request_timeout API.
    --
    g.router:exec(function(bid)
        local opts = {}
        -- request_timeout must be <= timeout.
        local err_msg = 'request_timeout must be <= timeout'
        opts.request_timeout = ivconst.CALL_TIMEOUT_MIN * 2
        t.assert_error_msg_contains(err_msg, function()
            ivshard.router.callro(bid, 'echo', {1}, opts)
        end)
        -- request_timeout must be a number.
        err_msg = 'Usage: call'
        opts.request_timeout = 'string'
        t.assert_error_msg_contains(err_msg, function()
            ivshard.router.callro(bid, 'echo', {1}, opts)
        end)
        -- request_timeout <= 0 leads to the TimeOut error.
        err_msg = 'Timeout'
        for _, timeout in ipairs({-1, 0}) do
            opts.request_timeout = timeout
            local ok, err = ivshard.router.callro(bid, 'echo', {1}, opts)
            t.assert_not(ok)
            t.assert_str_contains(err.message, err_msg)
        end
    end, {bid})

    --
    -- Test, that router makes the desired number of calls, when
    -- request_timeout is not a divisor of the timeout.
    --
    g.replica_1_a:exec(function()
        rawset(_G, 'sleep_num', 0)
        rawset(_G, 'sleep_cond', ifiber.cond())
        rawset(_G, 'old_get_uuid', _G.get_uuid)
        _G.get_uuid = function()
            _G.sleep_num = _G.sleep_num + 1
            _G.sleep_cond:wait()
        end
    end)
    g.router:exec(function(bid, uuid)
        local res = ivshard.router.callro(bid, 'get_uuid', {}, {
            request_timeout = 1,
            timeout = 1.5,
        })
        t.assert_equals(res, uuid)
    end, {bid, g.replica_1_b:instance_uuid()})

    -- Timeout errors are retried. Master's priority is higher, when weights
    -- are equal, currently they're not specified at all. Request to master
    -- fails and we fallback to the replica.
    t.assert_equals(g.replica_1_a:eval('return _G.sleep_num'), 1)
    g.replica_1_a:exec(function()
        _G.sleep_cond:broadcast()
        _G.get_uuid = _G.old_get_uuid
        _G.old_get_uuid = nil
        _G.sleep_cond = nil
        _G.sleep_num = nil
    end)
end

local function prepare_affect_priority_rs(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].replicas.replica_1_a.zone = 3
    new_cfg_template.sharding[1].replicas.replica_1_b.zone = 2
    new_cfg_template.zone = 1
    new_cfg_template.weights = {
        [1] = {
            [1] = 0,
            [2] = 1,
            [3] = 2,
        },
    }
    -- So that ping timeout is always > replica.net_timeout.
    -- net_timeout starts with CALL_TIMEOUT_MIN and is mutiplied by 2 if number
    -- of failed requests is >= 2.
    new_cfg_template.failover_ping_timeout = vconsts.CALL_TIMEOUT_MIN * 4
    local new_cluster_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_cluster_cfg)
end

local function affect_priority_clear_net_timeout(g)
    g.router:exec(function()
        -- Reset net_timeout, so that it doesn't affect the test. This is
        -- needed as we use the absolute minimum failover_ping_timeout for
        -- FAILOVER_DOWN_SEQUENTIAL_FAIL = 3. 10 successful calls are needed
        -- to restore it to CALL_TIMEOUT_MIN wthout reset.
        local router = ivshard.router.internal.static_router
        for _, rs in pairs(router.replicasets) do
            for _, r in pairs(rs.replicas) do
                r.net_timeout = ivconst.CALL_TIMEOUT_MIN
            end
        end
    end)
end

--
-- gh-483: failover ping temporary lower replica's priority, when it cannot be
-- reached several times in a row:
--
-- 1. replica_1_b is the prioritized one. replica_1_a is the second one.
-- 2. router establishes connection to all instances, failover sets prioritized
--    replica_1_b.
-- 3. Node breaks and stops to respond.
-- 4. Failover retries ping FAILOVER_DOWN_SEQUENTIAL_FAIL times and changes
--    prioritized replica to the lower one. Note, that connection is recreated
--    on every failed ping.
-- 5. Every FAILOVER_UP_TIMEOUT failover checks, if any replica with higher
--    priority can be reached and changes the prioritized replica if it's so.
--
g.test_failover_ping_affects_priority = function()
    prepare_affect_priority_rs(g)

    -- Find prioritized replica and disable failover for now.
    g.router:exec(function(rs_uuid, replica_uuid)
        local router = ivshard.router.internal.static_router
        local rs = router.replicasets[rs_uuid]
        local opts = {timeout = iwait_timeout}
        rs:wait_connected_all(opts)
        t.helpers.retrying(opts, function()
            _G.failover_wakeup()
            t.assert_equals(rs.replica.uuid, replica_uuid,
                            'Prioritized replica have not been set yet')
        end)
        _G.failover_pause()
    end, {g.replica_1_b:replicaset_uuid(), g.replica_1_b:instance_uuid()})

    -- Break 'info' request on replica so that it fails with TimedOut error.
    g.replica_1_b:exec(function()
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = function(service_name, ...)
            if service_name == 'info' then
                ifiber.sleep(ivconst.CALL_TIMEOUT_MIN * 5)
            end
            return _G.old_call(service_name, ...)
        end
    end)

    affect_priority_clear_net_timeout(g)
    g.router:exec(function(rs_uuid, master_uuid)
        local router = ivshard.router.internal.static_router
        local rs = router.replicasets[rs_uuid]

        -- And we change the prioritized replica.
        _G.failover_continue()
        t.helpers.retrying({timeout = iwait_timeout}, function()
            _G.failover_wakeup()
            t.assert_equals(rs.replica.uuid, master_uuid)
        end)

        -- Check, that prioritized replica is not changed, as it's still broken.
        rawset(_G, 'old_up_timeout', ivconst.FAILOVER_UP_TIMEOUT)
        ivconst.FAILOVER_UP_TIMEOUT = 0.01
        local worker = router.replicasets[rs_uuid].worker
        local info = worker.services['replicaset_failover'].data.info
        ivtest.service_wait_for_new_ok(info, {on_yield = _G.failover_wakeup()})
        t.assert_equals(rs.replica.uuid, master_uuid)
    end, {g.replica_1_b:replicaset_uuid(), g.replica_1_a:instance_uuid()})

    -- Restore 'info' request.
    g.replica_1_b:exec(function()
        ivshard.storage._call = _G.old_call
        _G.old_call = nil
    end)

    -- As replica_1_b has higher priority, it should be restored automatically.
    g.router:exec(function(rs_uuid, replica_uuid)
        local router = ivshard.router.internal.static_router
        local rs = router.replicasets[rs_uuid]
        t.assert_equals(rs.priority_list[1].uuid, replica_uuid)
        t.helpers.retrying({timeout = iwait_timeout}, function()
            _G.failover_wakeup()
            t.assert_equals(rs.replica.uuid, replica_uuid,
                            'Prioritized replica is not up yet')
        end)
    end, {g.replica_1_b:replicaset_uuid(), g.replica_1_b:instance_uuid()})

    vtest.router_cfg(g.router, global_cfg)
    g.router:exec(function()
        ivconst.FAILOVER_UP_TIMEOUT = _G.old_up_timeout
        _G.old_up_timeout = nil
    end)
end

--
-- gh-483: user calls also affects priority. If several sequential requests
-- fail, then the same logic as in the previous test happens.
--
g.test_failed_calls_affect_priority = function()
    prepare_affect_priority_rs(g)
    local timeout = vconsts.CALL_TIMEOUT_MIN * 4

    -- Find prioritized replica and disable failover for now.
    g.router:exec(function(rs_uuid, replica_uuid)
        local router = ivshard.router.internal.static_router
        local rs = router.replicasets[rs_uuid]
        local opts = {timeout = iwait_timeout}
        rs:wait_connected_all(opts)

        t.helpers.retrying(opts, function()
            _G.failover_wakeup()
            t.assert_equals(rs.replica.uuid, replica_uuid,
                            'Prioritized replica have not been set yet')
        end)

        _G.failover_pause()
        -- Discovery is disabled, as it may affect `net_sequential_fail`
        -- and leads to flakiness of the test.
        local errinj = ivshard.router.internal.errinj
        errinj.ERRINJ_LONG_DISCOVERY = true
        t.helpers.retrying(opts, function()
            router.discovery_fiber:wakeup()
            t.assert_equals(errinj.ERRINJ_LONG_DISCOVERY, 'waiting',
                'Discovery have not been stopped yet')
        end)
    end, {g.replica_1_b:replicaset_uuid(), g.replica_1_b:instance_uuid()})

    -- Break 'info' request on replica so that it fails with TimedOut error.
    -- No other request can be broken, as only failover changes priority and
    -- as soon as it wakes up it succeeds with `_call` and sets
    -- `net_sequential_fail` to 0.
    g.replica_1_b:exec(function()
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = function(service_name, ...)
            if service_name == 'info' then
                ifiber.sleep(ivconst.CALL_TIMEOUT_MIN * 5)
            end
            return _G.old_call(service_name, ...)
        end
    end)

    affect_priority_clear_net_timeout(g)
    local bid = vtest.storage_first_bucket(g.replica_1_a)
    g.router:exec(function(bid, timeout, rs_uuid, replica_uuid)
        local router = ivshard.router.internal.static_router
        local replica = router.replicasets[rs_uuid].replica
        t.assert_equals(replica.uuid, replica_uuid)

        local fails = replica.net_sequential_fail
        for _ = 1, ivconst.FAILOVER_DOWN_SEQUENTIAL_FAIL do
            local ok, err = ivshard.router.callro(bid, 'vshard.storage._call',
                                                  {'info'}, {timeout = timeout})
            t.assert_not(ok)
            t.assert(iverror.is_timeout(err))
        end

        t.assert_equals(replica.net_sequential_fail,
                        fails + ivconst.FAILOVER_DOWN_SEQUENTIAL_FAIL)

        -- Priority is changed only by failover. So, the prioritized replica
        -- is still the failing one.
        t.assert_equals(router.replicasets[rs_uuid].replica.uuid, replica_uuid)
    end, {bid, timeout, g.replica_1_b:replicaset_uuid(),
          g.replica_1_b:instance_uuid()})

    -- Enable failover, which changes priority of the replica.
    g.router:exec(function(rs_uuid, master_uuid)
        local router = ivshard.router.internal.static_router
        _G.failover_continue()
        t.helpers.retrying({timeout = iwait_timeout}, function()
            _G.failover_wakeup()
            t.assert_equals(router.replicasets[rs_uuid].replica.uuid,
                            master_uuid, 'Master is not prioritized yet')
        end)
    end, {g.replica_1_b:replicaset_uuid(), g.replica_1_a:instance_uuid()})

    -- Restore 'info' request.
    g.replica_1_b:exec(function()
        ivshard.storage._call = _G.old_call
        _G.old_call = nil
    end)

    -- As replica_1_b has higher priority, it should be restored automatically.
    g.router:exec(function(rs_uuid, replica_uuid)
        local old_up_timeout = ivconst.FAILOVER_UP_TIMEOUT
        ivconst.FAILOVER_UP_TIMEOUT = 1
        local router = ivshard.router.internal.static_router
        local rs = router.replicasets[rs_uuid]
        t.assert_equals(rs.priority_list[1].uuid, replica_uuid)
        t.helpers.retrying({timeout = iwait_timeout}, function()
            _G.failover_wakeup()
            t.assert_equals(rs.replica.uuid, replica_uuid,
                            'Prioritized replica is not up yet')
        end)
        ivconst.FAILOVER_UP_TIMEOUT = old_up_timeout
    end, {g.replica_1_b:replicaset_uuid(), g.replica_1_b:instance_uuid()})

    vtest.router_cfg(g.router, global_cfg)
end

--
-- gh-474: error during alert construction
--
g.test_info_with_named_identification = function()
    t.run_only_if(vutil.feature.persistent_names)
    --
    -- Missing master alert.
    --
    local template = router_named_cfg_template(g)
    -- Intentionally skip master in order to get info warning.
    template.sharding['replicaset_1'].replicas.replica_1_a.master = nil
    vtest.router_cfg(g.router, vtest.config_new(template))
    local alerts = g.router:exec(function()
        local ok, result = pcall(ivshard.router.info)
        ilt.assert(ok, 'no error')
        return result.alerts
    end)
    t.assert(vtest.info_assert_alert(alerts, 'MISSING_MASTER'),
             'MISSING_MASTER alert is constructed')

    --
    -- Unreachable master alert.
    --
    template.sharding['replicaset_1'].replicas.replica_1_a.master = true
    local new_global_cfg = vtest.config_new(template)
    new_global_cfg.sharding['replicaset_1'].replicas.replica_1_a.uri =
        vserver.build_listen_uri('nonexistent')
    vtest.router_cfg(g.router, new_global_cfg)
    alerts = g.router:exec(function()
        local ok, result = pcall(ivshard.router.info)
        ilt.assert(ok, 'no error')
        return result.alerts
    end)
    local alert = vtest.info_assert_alert(alerts, 'UNREACHABLE_MASTER')
    t.assert(alert, 'UNREACHABLE_MASTER alert is constructed')
    t.assert_not_str_contains(alert[2], 'replicaset nil',
                              'alert contains valid replicaset id')
    vtest.router_cfg(g.router, global_cfg)
end

--
-- gh-548: `TRANSFER_IS_IN_PROGRESS` error should be retryable.
--
g.test_retryable_transfer_in_progress = function(g)
    -- Resolve bucket on router.
    local bid = g.replica_2_a:exec(function()
       return _G.get_first_bucket()
    end)
    g.router:exec(function(bid, uuid)
        local rs = ivshard.router.route(bid)
        ilt.assert_equals(rs.uuid, uuid)
    end, {bid, g.replica_2_a:replicaset_uuid()})

    -- Block bucket receive.
    g.replica_1_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = true
    end)

    -- Start bucket send.
    g.replica_2_a:exec(function(bid, uuid)
        rawset(_G, 'bucket_send_fiber', ifiber.create(function()
            return ivshard.storage.bucket_send(
                bid, uuid, {timeout = iwait_timeout})
        end))
        _G.bucket_send_fiber:set_joinable(true)
        -- Drop ref, otherwise BUCKET_IS_LOCKED error will be thrown.
        ivshard.storage.internal.bucket_refs[bid] = nil
        return bid
    end, {bid, g.replica_1_a:replicaset_uuid()})

    -- Make rw request on router.
    g.router:exec(function(bid)
        rawset(_G, 'callrw_fiber', ifiber.create(function()
            return ivshard.router.callrw(
                bid, 'echo', {123}, {timeout = iwait_timeout})
        end))
        _G.callrw_fiber:set_joinable(true)
    end, {bid})

    -- End bucket send.
    g.replica_1_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = false
    end)
    g.replica_2_a:exec(function()
        local ok, res = _G.bucket_send_fiber:join()
        ilt.assert(ok and res)
        _G.bucket_send_fiber = nil
        _G.bucket_gc_wait()
    end)

    -- Request should end successfully.
    g.router:exec(function()
        local ok, res, err = _G.callrw_fiber:join()
        ilt.assert(ok)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, 123)
        _G.callrw_fiber = nil
    end)

    -- Return the bucket back.
    g.replica_1_a:exec(function(bid, uuid)
        local _, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        _G.bucket_gc_wait()
    end, {bid, g.replica_2_a:replicaset_uuid()})
end
