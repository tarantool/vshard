local t = require('luatest')
local fiber = require('fiber')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local vconst = require('vshard.consts')
local verror = require('vshard.error')
local vreplicaset = require('vshard.replicaset')

local timeout_opts = {timeout = vtest.wait_timeout}
local small_timeout_opts = {timeout = 0.05}

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
    bucket_count = 20
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

test_group.test_replicaset_map_call = function(g)
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
                    ifiber.sleep(0.001)
                end
            end
            return true
        end)
    end, {uuid_c})
    res, err = rs:map_call('test_sleep_c', {}, small_timeout_opts)
    t.assert_not_equals(err, nil)
    t.assert(vtest.error_is_timeout(err))
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
    -- Cleanup.
    --
    vtest.cluster_exec_each(g, function()
        _G.test_err_a = nil
        _G.test_sleep_do = nil
        _G.test_sleep_c = nil
        _G.test_sleep_is_called = nil
    end)
end
