local t = require('luatest')
local verror = require('vshard.error')
local vtest = require('test.luatest_helpers.vtest')

local g = t.group('bootstrap')

g.before_all(function(g)
    local cfg = vtest.config_new({
        sharding = {
            {replicas = {a = {master = true}}},
            {replicas = {b = {master = true}}},
        },
        bucket_count = 2,
    })
    vtest.cluster_new(g, cfg)
    vtest.router_new(g, 'router', cfg)
end)

g.after_all(function(g)
    g.cluster:drop()
end)

g.test_bootstrap_preserves_count_error = function(g)
    -- Fail the first count; the other replicaset is empty.
    local first = g.router:exec(function()
        return (next(ivshard.router.routeall()))
    end)
    local storage = first == g.a:replicaset_uuid() and g.a or g.b
    storage:exec(function() ivshard.storage.disable() end)
    local ok, res, err = pcall(g.router.exec, g.router, function()
        return ivshard.router.bootstrap({if_not_bootstrapped = true,
                                        timeout = iwait_timeout})
    end)
    storage:exec(function() ivshard.storage.enable() end)
    t.assert(ok, res)
    t.assert_equals(res, nil)
    t.assert_equals(err and err.code, verror.code.STORAGE_IS_DISABLED)
end
