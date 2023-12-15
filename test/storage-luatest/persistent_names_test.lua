local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local test_group = t.group('storage')

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

test_group.before_all(function(g)
    -- Even cluster bootstrap can be done only on Tarantool >= 3.0.0.
    -- No need to add run_only_if for every test. This skips them all.
    t.run_only_if(vutil.feature.persistent_names)
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Test, that all names are properly set after cluster bootstrap.
--
test_group.test_basic = function(g)
    local rs_name = g.replica_1_a:replicaset_name()
    local name = g.replica_1_a:instance_name()
    t.assert_equals(rs_name, 'replicaset_1')
    t.assert_equals(name, 'replica_1_a')
    rs_name = g.replica_2_a:replicaset_name()
    name = g.replica_2_a:instance_name()
    t.assert_equals(rs_name, 'replicaset_2')
    t.assert_equals(name, 'replica_2_a')
end

--
-- Test named config to UUID config switch.
--
test_group.test_switch_to_uuid = function(g)
    -- Make UUID indexed config.
    local new_cfg = table.deepcopy(global_cfg)
    local uuid_rs_1 = g.replica_1_a:replicaset_uuid()
    local uuid_1 = g.replica_1_a:instance_uuid()
    local uuid_rs_2 = g.replica_2_a:replicaset_uuid()
    local uuid_2 = g.replica_2_a:instance_uuid()
    new_cfg.identification_mode = 'uuid_as_key'
    local sharding = new_cfg.sharding
    sharding[uuid_rs_1] = sharding.replicaset_1
    sharding[uuid_rs_2] = sharding.replicaset_2
    sharding.replicaset_1 = nil
    sharding.replicaset_2 = nil
    sharding[uuid_rs_1].replicas[uuid_1] =
        sharding[uuid_rs_1].replicas.replica_1_a
    sharding[uuid_rs_2].replicas[uuid_2] =
        sharding[uuid_rs_2].replicas.replica_2_a
    sharding[uuid_rs_1].replicas.replica_1_a = nil
    sharding[uuid_rs_2].replicas.replica_2_a = nil

    -- Send bucket on UUID identification.
    vtest.cluster_cfg(g, new_cfg)
    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        return bid
    end, {uuid_rs_2})

    -- Send it back on name identification.
    vtest.cluster_cfg(g, global_cfg)
    g.replica_2_a:exec(function(bid, name)
        local ok, err = ivshard.storage.bucket_send(
            bid, name, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_name()})
end
