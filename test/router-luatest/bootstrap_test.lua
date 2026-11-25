local t = require('luatest')
local verror = require('vshard.error')
local vtest = require('test.luatest_helpers.vtest')

local g = t.group('bootstrap')

g.before_each(function(g)
    g.cfg = vtest.config_new({
        sharding = {
            {replicas = {a = {master = true}, a2 = {}}},
            {replicas = {b = {master = true}}},
        },
        bucket_count = 2,
        rebalancer_mode = 'off',
    })
    vtest.cluster_new(g, g.cfg)
    vtest.router_new(g, 'router', g.cfg)
    vtest.storage_wait_bucket_sync(g.a)
    vtest.storage_wait_bucket_sync(g.b)
end)

g.after_each(function(g)
    g.cluster:drop()
    g.cluster = nil
end)

g.test_bootstrap_preserves_count_error = function(g)
    -- Fail the first count; the other replicaset is empty.
    local first = g.router:exec(function()
        return (next(ivshard.router.routeall()))
    end)
    local storage = first == g.a:replicaset_uuid() and g.a or g.b
    storage:exec(function() ivshard.storage.disable() end)
    local res, err = g.router:exec(function()
        return ivshard.router.bootstrap({if_not_bootstrapped = true,
                                        timeout = iwait_timeout})
    end)
    t.assert_equals(res, nil)
    t.assert_equals(err and err.code, verror.code.STORAGE_IS_DISABLED)
end

g.test_bootstrap_unsynced_master = function(g)
    -- The promoted replica must not see the bucket until replication resumes.
    g.a2:wait_vclock_of(g.a)
    g.a2:update_box_cfg({replication = {}})
    g.a:exec(function() ilt.assert(ivshard.storage.bucket_create(1)) end)
    local replicas = g.cfg.sharding[g.a:replicaset_uuid()].replicas
    replicas[g.a:instance_uuid()].master = nil
    replicas[g.a2:instance_uuid()].master = true
    local isolated_cfg = table.copy(g.cfg)
    isolated_cfg.replication = {}
    vtest.storage_cfg(g.a2, isolated_cfg)
    vtest.router_cfg(g.router, g.cfg)
    t.assert_equals(g.a2:eval('return box.space._bucket:count()'), 0)

    local function bootstrap()
        return ivshard.router.bootstrap({timeout = 1})
    end
    local res, err = g.router:exec(bootstrap)
    t.assert_equals(res, nil)
    t.assert_equals(err.code, verror.code.MASTER_NOT_SYNCED)
    t.assert_equals(g.b:eval('return box.space._bucket:count()'), 0)

    vtest.storage_cfg(g.a2, g.cfg)
    vtest.storage_cfg(g.a, g.cfg)
    vtest.storage_wait_bucket_sync(g.a2)
    res, err = g.router:exec(bootstrap)
    t.assert_equals(res, nil)
    t.assert_equals(err.code, verror.code.NON_EMPTY)
end

g.test_bootstrap_if_not_bootstrapped_with_unavailable_master = function(g)
    g.a:exec(function() ilt.assert(ivshard.storage.bucket_create(1)) end)
    g.b:stop()
    g.router:exec(function()
        local res, err = ivshard.router.bootstrap({timeout = 1})
        ilt.assert_equals(res, nil)
        ilt.assert_not_equals(err, nil)
        ilt.assert_equals(ivshard.router.bootstrap({
            if_not_bootstrapped = true, timeout = 1,
        }), true)
    end)
end
