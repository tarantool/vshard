local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local wait_timeout = 120

local g = t.group('router')
local cluster_cfg = vtest.config_new({
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
