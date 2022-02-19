local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local wait_timeout = 120

local g = t.group('router')
local cluster_cfg, cfg_meta = vtest.config_new({
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
})

g.before_all(function()
    vtest.storage_new(g, cluster_cfg)

    t.assert_equals(g.replica_1_a:exec(function()
        return #vshard.storage.info().alerts
    end), 0, 'no alerts after boot')

    local router = vtest.router_new(g, 'router', cluster_cfg)
    g.router = router
    local res, err = router:exec(function(timeout)
        return vshard.router.bootstrap({timeout = timeout})
    end, {wait_timeout})
    t.assert(res and not err, 'bootstrap buckets')
end)

g.after_all(function()
    g.cluster:drop()
end)

g.test_basic = function(g)
    local router = g.router
    local res, err = router:exec(function(timeout)
        return vshard.router.callrw(1, 'echo', {1}, {timeout = timeout})
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
        local args = msgpack.object({100})
        return vshard.router.callrw(1, 'echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(not err, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Normal call rw.
    --
    res, err = router:exec(function(timeout)
        local args = msgpack.object({100})
        return vshard.router.callro(1, 'echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(not err, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Direct call ro.
    --
    res, err = router:exec(function(timeout)
        local args = msgpack.object({100})
        local route = vshard.router.route(1)
        return route:callro('echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(err == nil, 'no error')
    t.assert_equals(res, 100, 'good result')
    --
    -- Direct call rw.
    --
    res, err = router:exec(function(timeout)
        local args = msgpack.object({100})
        local route = vshard.router.route(1)
        return route:callrw('echo', args, {timeout = timeout})
    end, {wait_timeout})
    t.assert(err == nil, 'no error')
    t.assert_equals(res, 100, 'good result')
end

local function test_return_raw_template(g, mode)
    --
    -- Normal call.
    --
    local router = g.router
    local res = router:exec(function(timeout, mode)
        return add_details(vshard.router[mode](1, 'echo', {1, 2, 3},
                           {timeout = timeout, return_raw = true}))
    end, {wait_timeout, mode})
    t.assert_equals(res.val, {1, 2, 3}, 'value value')
    t.assert_equals(res.val_type, 'userdata', 'value type')
    t.assert(not res.err, 'no error')

    --
    -- Route call.
    --
    res = router:exec(function(timeout, mode)
        local route = vshard.router.route(1)
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
        return add_details(vshard.router[mode](1, 'echo', {},
                           {timeout = timeout, return_raw = true}))
    end, {wait_timeout, mode})
    t.assert(not res.val, 'no value')
    t.assert(not res.err, 'no error')

    --
    -- Error.
    --
    res = router:exec(function(timeout, mode)
        return add_details(vshard.router[mode](1, 'box_error', {1, 2, 3},
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
        local route = vshard.router.route(1)
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
        _G.do_map = function(res2)
            return {res1, res2}
        end
    end
    g.replica_1_a:exec(create_map_func_f, {1})
    g.replica_2_a:exec(create_map_func_f, {2})
    --
    -- Successful map.
    --
    local res = g.router:exec(function(timeout)
        local val, err = vshard.router.map_callrw('do_map', msgpack.object({3}),
                                                  {timeout = timeout,
                                                   return_raw = true})
        local _, one_map = next(val)
        return {
            val = val,
            map_type = type(one_map),
            err = err,
        }
    end, {wait_timeout})
    local expected = {
        [cfg_meta.replicasets[1].uuid] = {{1, 3}},
        [cfg_meta.replicasets[2].uuid] = {{2, 3}},
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
        return vshard.router.map_callrw('do_map', {}, {timeout = timeout,
                                        return_raw = true})
    end, {wait_timeout})
    expected = {
        [cfg_meta.replicasets[1].uuid] = {{1}},
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
        return vshard.router.map_callrw('do_map', {}, {timeout = timeout,
                                        return_raw = true})
    end, {wait_timeout})
    t.assert(res == nil, 'no result')
    t.assert_covers(err, {
        code = box.error.PROC_LUA,
        type = 'ClientError',
        message = 'map_err'
    }, 'error object')
    t.assert_equals(err_uuid, cfg_meta.replicasets[2].uuid, 'error uuid')
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
