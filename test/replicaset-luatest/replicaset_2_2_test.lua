local t = require('luatest')
local vreplicaset = require('vshard.replicaset')
local vtest = require('test.luatest_helpers.vtest')
local test_group = t.group('replicaset-2-2')
local vcfg = require('vshard.cfg')
local verror = require('vshard.error')

local timeout_opts = {timeout = vtest.wait_timeout}

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
        }
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

test_group.test_wait_masters_connect = function()
    local replicasets = vreplicaset.buildall(global_cfg)
    for _, rs in pairs(replicasets) do
        for _, r in pairs(rs.replicas) do
            t.assert_not(r:is_connected())
        end
    end
    vreplicaset.wait_masters_connect(replicasets, timeout_opts.timeout)
    for _, rs in pairs(replicasets) do
        for _, r in pairs(rs.replicas) do
            if rs.master == r then
                t.assert(r:is_connected())
            else
                t.assert_not(r:is_connected())
            end
        end
    end
end

test_group.test_masters_map_call = function()
    local replicasets = vreplicaset.buildall(global_cfg)
    local res = vreplicaset.masters_map_call(
        replicasets, 'get_uuid', nil, timeout_opts)
    for _, rs in pairs(replicasets) do
        t.assert(rs.master:is_connected())
    end
    local count = 0
    for rs_id, uuid in pairs(res) do
        t.assert(replicasets[rs_id].master.id, uuid)
        count = count + 1
    end
    t.assert_equals(count, 2)
end

test_group.test_masters_map_call_reply_timeout = function()
    local id, rs = next(vreplicaset.buildall(global_cfg))
    local future = {
        wait_result = function() return nil, verror.timeout() end,
        is_ready = function() return false end,
        discard = function() end,
    }
    rs.master.conn = {
        state = 'active',
        is_connected = function() return true end,
        call = function() return future end,
    }
    local timeout = rs.master.net_timeout
    for _ = 1, 2 do
        local _, err = vreplicaset.masters_map_call(
            {[id] = rs}, 'get_uuid', nil, {timeout = timeout})
        t.assert(err and verror.is_timeout(err))
    end
    t.assert_equals(rs.master.net_timeout, timeout * 2)
end

test_group.test_masters_map_call_missing_master = function()
    local id, rs = next(vreplicaset.buildall(global_cfg))
    rs.wait_connected = function(self, timeout)
        self.master = nil
        return timeout
    end
    local res, err, err_id = vreplicaset.masters_map_call(
        {[id] = rs}, 'get_uuid', nil, timeout_opts)
    t.assert_equals(res, nil)
    t.assert_equals(err.code, verror.code.MISSING_MASTER)
    t.assert_equals(err_id, id)
end
