local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local table_keys = vutil.table_keys

local BACKEND = 'roles.recovery-point-manager.backends.vshard-router'

local g = t.group('router_recovery_point')

local cfg_template = {
    sharding = {
        replicaset_1 = {
            replicas = {
                replica_1_a = {
                    master = true,
                },
            },
        },
        replicaset_2 = {
            replicas = {
                replica_2_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 10,
    replication_timeout = 0.1,
    identification_mode = 'name_as_key',
}
local global_cfg

g.before_all(function(cg)
    t.run_only_if(vutil.feature.recovery_point, 'no recovery_point API')
    global_cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(cg, global_cfg)
    vtest.cluster_bootstrap(cg, global_cfg)
    cg.router = vtest.router_new(cg, 'router', global_cfg)
    cg.router:exec(function()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.router.discovery_wakeup()
            ilt.assert_equals(ivshard.router.info().bucket.unknown, 0)
        end)
    end)
end)

g.after_all(function(cg)
    cg.cluster:drop()
end)

local function count_recovery_points(storage)
    return storage:exec(function()
        return box.space._recovery_point:count()
    end)
end

--------------------------------------------------------------------------------
-- Router's create_cluster_recovery_point
--------------------------------------------------------------------------------

g.test_create_cluster_point = function(cg)
    local before_1 = count_recovery_points(cg.replica_1_a)
    local before_2 = count_recovery_points(cg.replica_2_a)

    local res, err = cg.router:exec(function()
        return ivshard.router.create_cluster_recovery_point('point-1',
            {timeout = iwait_timeout})
    end)
    t.assert_equals(err, nil)
    t.assert_not_equals(res, nil)

    t.assert_items_equals(table_keys(res), {'replicaset_1', 'replicaset_2'})
    for _, rs_res in pairs(res) do
        local point = rs_res[1]
        t.assert_equals(point.label, 'point-1')
        t.assert_type(point.timestamp, 'number')
        t.assert_type(point.lsn, 'number')
        t.assert_type(point.replica_id, 'number')
    end
    t.assert_equals(count_recovery_points(cg.replica_1_a), before_1 + 1)
    t.assert_equals(count_recovery_points(cg.replica_2_a), before_2 + 1)

    -- A non-string point name is a usage error.
    cg.router:exec(function()
        local ok, uerr = pcall(ivshard.router.create_cluster_recovery_point, 42)
        ilt.assert_not(ok)
        ilt.assert_str_contains(tostring(uerr), 'Usage: router.create_cluster')
    end)
end

--------------------------------------------------------------------------------
-- vshard-router backend module
--------------------------------------------------------------------------------

g.test_backend_basic = function(cg)
    cg.router:exec(function(backend_name)
        local backend = require(backend_name)
        -- new() resolves the router eagerly and raises when it is missing.
        local res, err = pcall(backend.new, {router_name = 'nope'})
        ilt.assert_not(res)
        ilt.assert_str_contains(tostring(err), 'router "nope" not found')

        -- Good backend instance.
        local inst = backend.new({})
        ilt.assert_not_equals(inst.router, nil)
        res, err = inst:create_point(
            {label = 'unit-1', timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert_not_equals(res, nil)
        local count = 0
        for _, rs_res in pairs(res) do
            ilt.assert_equals(rs_res[1].label, 'unit-1')
            count = count + 1
        end
        ilt.assert_equals(count, 2)
    end, {BACKEND})
end

g.test_backend_info = function(cg)
    cg.router:exec(function(backend_name)
        local backend = require(backend_name)
        local inst = backend.new({})

        -- Wait for a fully discovered, healthy cluster: the real info is
        -- tagged, the router status is dropped and there are no alerts.
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.router.discovery_wakeup()
            ilt.assert_equals(ivshard.router.info().bucket.unknown, 0)
        end)
        local info = inst:info()
        ilt.assert_equals(info.backend_type, 'vshard-router')
        ilt.assert_equals(info.status, nil)
        ilt.assert_equals(info.alerts, {})

        -- Produce a real, whitelisted router warning: with discovery off, clear
        -- the route map so all buckets become unknown.
        ivshard.router.discovery_set('off')
        ivshard.router._route_map_clear()

        local warned = inst:info()
        ilt.assert_equals(warned.backend_type, 'vshard-router')
        ilt.assert_equals(warned.status, nil)
        -- Every kept alert is converted to the {message = ...} shape.
        local found = false
        for _, alert in ipairs(warned.alerts) do
            ilt.assert_type(alert.message, 'string')
            if alert.message:find('UNKNOWN_BUCKETS') then
                found = true
            end
        end
        ilt.assert(found)

        -- Restore discovery and wait until all buckets are known again.
        ivshard.router.discovery_set('on')
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.router.discovery_wakeup()
            ilt.assert_equals(ivshard.router.info().bucket.unknown, 0)
        end)
    end, {BACKEND})
end

--------------------------------------------------------------------------------
-- Imperative recovery point manager
--------------------------------------------------------------------------------

g.test_core_manager = function(cg)
    local before = count_recovery_points(cg.replica_1_a)
    cg.router:exec(function(backend_name)
        local m = box.backup.recovery_point.manager_create('test_mgr', {
            backend = require(backend_name),
            backend_cfg = {},
            create_interval = 0.1,
            timeout = iwait_timeout,
        })
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_not_equals(m:info().last_point, nil)
        end)
        local info = box.backup.recovery_point.managers['test_mgr']:info()
        ilt.assert_equals(info.backend.backend_type, 'vshard-router')
        box.backup.recovery_point.manager_drop('test_mgr')
    end, {BACKEND})
    -- The point actually landed on the storage master.
    t.assert_gt(count_recovery_points(cg.replica_1_a), before)
end
