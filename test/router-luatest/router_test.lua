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
