local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local asserts = require('test.luatest_helpers.asserts')

local test_group = t.group('storage')

local cfg_template = {
    sharding = {
        repliacset_1 = {
            replicas = {
                replica_1_a = {
                    master = true
                },
                replica_1_b = {},
            },
        },
    },
    bucket_count = 20,
    identification_mode = 'name_as_key'
}

local global_cfg

test_group.before_all(function(g)
    t.run_only_if(vutil.feature.persistent_names)
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_named_replicaset_alerts_when_replica_disconnects = function(g)
    g.replica_1_b:stop()
    local alerts = g.replica_1_a:exec(function()
        return ivshard.storage.info().alerts
    end)
    asserts:info_assert_alert(alerts, 'UNREACHABLE_REPLICA')
    asserts:info_assert_alert(alerts, 'UNREACHABLE_REPLICASET')
    g.replica_1_b:start()
end
