local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local wait_timeout = 120

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
    bucket_count = 100
}
local cluster_cfg = vtest.config_new(cfg_template)

local function callrw_get_uuid(bid, timeout)
    return ivshard.router.callrw(bid, 'get_uuid', {}, {timeout = timeout})
end

local function callrw_session_get(bid, key, timeout)
    return ivshard.router.callrw(bid, 'session_get', {key},
                                 {timeout = timeout})
end

local function callrw_session_set(bid, key, value, timeout)
    return ivshard.router.callrw(bid, 'session_set', {key, value},
                                 {timeout = timeout})
end

g.before_all(function()
    vtest.storage_new(g, cluster_cfg)

    t.assert_equals(g.replica_1_a:exec(function()
        return #ivshard.storage.info().alerts
    end), 0, 'no alerts after boot')

    local router = vtest.router_new(g, 'router', cluster_cfg)
    g.router = router
    local res, err = router:exec(function(timeout)
        return ivshard.router.bootstrap({timeout = timeout})
    end, {wait_timeout})
    t.assert(res and not err, 'bootstrap buckets')
end)

g.after_all(function()
    g.cluster:drop()
end)

g.test_basic = function(g)
    local router = g.router
    local res, err = router:exec(function(timeout)
        return ivshard.router.callrw(1, 'echo', {1}, {timeout = timeout})
    end, {wait_timeout})
    t.assert(not err, 'no error')
    t.assert_equals(res, 1, 'good result')
end

g.test_msgpack_args = function(g)
    t.run_only_if(vutil.feature.msgpack_object)
    --
    -- Normal call ro.
    --
    local router = g.router
    local res, err = router:exec(function(timeout)
        local args = imsgpack.object({100})
        return ivshard.router.callrw(1, 'echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(not err, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Normal call rw.
    --
    res, err = router:exec(function(timeout)
        local args = imsgpack.object({100})
        return ivshard.router.callro(1, 'echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(not err, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Direct call ro.
    --
    res, err = router:exec(function(timeout)
        local args = imsgpack.object({100})
        local route = ivshard.router.route(1)
        return route:callro('echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(err == nil, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Direct call rw.
    --
    res, err = router:exec(function(timeout)
        local args = imsgpack.object({100})
        local route = ivshard.router.route(1)
        return route:callrw('echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(err == nil, 'no error')
    t.assert_equals(res, 100, 'good result')
end

local function test_return_raw_template(g, mode)
    --
    -- Normal call.
    --
    -- luacheck: ignore 113/add_details
    local router = g.router
    local res = router:exec(function(timeout, mode)
        return add_details(ivshard.router[mode](1, 'echo', {1, 2, 3},
                           {timeout = timeout, return_raw = true}))
    end, {wait_timeout, mode})
    t.assert_equals(res.val, {1, 2, 3}, 'value value')
    t.assert_equals(res.val_type, 'userdata', 'value type')
    t.assert(not res.err, 'no error')

    --
    -- Route call.
    --
    res = router:exec(function(timeout, mode)
        local route = ivshard.router.route(1)
        return add_details(route[mode](route, 'echo', {1, 2, 3},
                           {timeout = timeout, return_raw = true}))
    end, {wait_timeout, mode})
    t.assert_equals(res.val, {1, 2, 3}, 'value value')
    t.assert_equals(res.val_type, 'userdata', 'value type')
    t.assert(not res.err, 'no error')

    --
    -- Empty result set.
    --
    res = router:exec(function(timeout, mode)
        return add_details(ivshard.router[mode](1, 'echo', {},
                           {timeout = timeout, return_raw = true}))
    end, {wait_timeout, mode})
    t.assert(not res.val, 'no value')
    t.assert(not res.err, 'no error')

    --
    -- Error.
    --
    res = router:exec(function(timeout, mode)
        return add_details(ivshard.router[mode](1, 'box_error', {1, 2, 3},
                           {timeout = timeout}))
    end, {wait_timeout, mode})
    t.assert(not res.val, 'no value')
    t.assert_equals(res.err_type, 'table', 'error type')
    t.assert_covers(res.err, {type = 'ClientError', code = box.error.PROC_LUA},
                    'error value')

    --
    -- Route error.
    --
    res = router:exec(function(timeout, mode)
        local route = ivshard.router.route(1)
        return add_details(route[mode](route, 'box_error', {1, 2, 3},
                           {timeout = timeout}))
    end, {wait_timeout, mode})
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

g.test_map_callrw_raw = function(g)
    t.run_only_if(vutil.feature.netbox_return_raw)

    local create_map_func_f = function(res1)
        rawset(_G, 'do_map', function(res2)
            return {res1, res2}
        end)
    end
    g.replica_1_a:exec(create_map_func_f, {1})
    g.replica_2_a:exec(create_map_func_f, {2})
    --
    -- Successful map.
    --
    local res = g.router:exec(function(timeout)
        local val, err = ivshard.router.map_callrw(
            'do_map', imsgpack.object({3}), {timeout = timeout,
            return_raw = true})
        local _, one_map = next(val)
        return {
            val = val,
            map_type = type(one_map),
            err = err,
        }
    end, {wait_timeout})
    local rs1_uuid = g.replica_1_a:replicaset_uuid()
    local rs2_uuid = g.replica_2_a:replicaset_uuid()
    local expected = {
        [rs1_uuid] = {{1, 3}},
        [rs2_uuid] = {{2, 3}},
    }
    t.assert_equals(res.val, expected, 'map callrw success')
    t.assert_equals(res.map_type, 'userdata', 'values are msgpacks')
    t.assert(not res.err, 'no error')
    --
    -- Successful map, but one of the storages returns nothing.
    --
    g.replica_2_a:exec(function()
        _G.do_map = function()
            return
        end
    end)
    res = g.router:exec(function(timeout)
        return ivshard.router.map_callrw('do_map', {}, {timeout = timeout,
                                         return_raw = true})
    end, {wait_timeout})
    expected = {
        [rs1_uuid] = {{1}},
    }
    t.assert_equals(res, expected, 'map callrw without one value success')
    --
    -- Error at map stage.
    --
    g.replica_2_a:exec(function()
        _G.do_map = function()
            return box.error(box.error.PROC_LUA, "map_err")
        end
    end)
    local err, err_uuid
    res, err, err_uuid = g.router:exec(function(timeout)
        return ivshard.router.map_callrw('do_map', {}, {timeout = timeout,
                                         return_raw = true})
    end, {wait_timeout})
    t.assert(res == nil, 'no result')
    t.assert_covers(err, {
        code = box.error.PROC_LUA,
        type = 'ClientError',
        message = 'map_err'
    }, 'error object')
    t.assert_equals(err_uuid, rs2_uuid, 'error uuid')
    --
    -- Cleanup.
    --
    g.replica_1_a:exec(function()
        _G.do_map = nil
    end)
    g.replica_2_a:exec(function()
        _G.do_map = nil
    end)
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
    res, err = g.router:exec(callrw_session_set, {bid, 1, 10, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert(res, 'set session key')

    -- Make the URI a table but it is still the same.
    rep_1_a_cfg.uri = {rep_1_a_cfg.uri}
    vtest.router_cfg(g.router, new_cfg)

    -- The connection is still the same - session key remains.
    res, err = g.router:exec(callrw_session_get, {bid, 1, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, 10, 'get session key')

    -- Restore the globals back.
    vtest.router_cfg(g.router, cluster_cfg)
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
    local new_storage_cfg = vtest.config_new(new_cfg_template)
    vtest.storage_cfg(g, new_storage_cfg)

    -- Router connects to the first port.
    local new_router_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_router_cfg)

    local rep_1_a_uuid = g.replica_1_a:instance_uuid()
    local res, err = g.router:exec(callrw_get_uuid, {bid, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a')

    -- Save a key in the session to check later for a reconnect.
    res, err = g.router:exec(callrw_session_set, {bid, 1, 10, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert(res, 'set session key')

    -- The key is actually saved.
    res, err = g.router:exec(callrw_session_get, {bid, 1, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, 10, 'get session key')

    -- Router connects to the second port. The storage's cfg is intentionally
    -- unchanged.
    rep_1_a_templ.port_uri = 2
    new_router_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_router_cfg)

    res, err = g.router:exec(callrw_get_uuid, {bid, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a again')

    -- There was a reconnect - the session is new.
    res, err = g.router:exec(callrw_session_get, {bid, 1, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, nil, 'no session key')

    -- To confirm that the router uses the second port, shut it down on the
    -- storage. The router won't be able to reconnect.
    rep_1_a_templ.port_count = 1
    rep_1_a_templ.port_uri = 1
    new_storage_cfg = vtest.config_new(new_cfg_template)
    vtest.storage_cfg(g, new_storage_cfg)
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
    res, err = g.router:exec(callrw_get_uuid, {bid, wait_timeout})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a again')

    -- Restore everything back.
    vtest.storage_cfg(g, cluster_cfg)
    vtest.router_cfg(g.router, cluster_cfg)
end