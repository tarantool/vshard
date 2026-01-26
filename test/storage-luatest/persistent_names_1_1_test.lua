local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local server = require('test.luatest_helpers.server')
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

local function persistent_names_remove(g)
    return vtest.cluster_exec_each_master(g, function()
        local name = box.info.name
        box.cfg{force_recovery = true}
        box.space._cluster:update(box.info.id, {{'=', 3, box.NULL}})
        box.cfg{force_recovery = false}
        return name
    end)
end

local function persistent_names_restore(g, names)
    for vtest_name, persistent_name in pairs(names) do
        g[vtest_name]:exec(function(name)
            box.cfg{force_recovery = true}
            box.space._cluster:update(box.info.id, {{'=', 3, name}})
            box.cfg{force_recovery = false}
        end, {persistent_name})
    end
end

--
-- vshard throwed unrelevant UNREACHABLE_REPLICA warning, when names are
-- not set and `name_as_key` identification_mode is used.
--
test_group.test_no_unreachable_replica_alert = function(g)
    local names = persistent_names_remove(g)
    server:assert_no_alerts(g.replica_1_a)
    server:assert_no_alerts(g.replica_2_a)
    persistent_names_restore(g, names)
end

--
-- gh-493: vshard should not show alerts for replicas, which are not in the
-- vshard's config.
--
test_group.test_alerts_for_named_replica = function(g)
    t.run_only_if(vutil.feature.persistent_names)

    local named_replica = server:new({
        alias = 'named_replica',
        box_cfg = {
            replication = g.replica_1_a.net_box_uri,
            instance_name = 'named_replica'
        }
    })

    named_replica:start()
    named_replica:wait_for_vclock_of(g.replica_1_a)
    server:assert_no_alerts(g.replica_1_a)
    local instance_id = named_replica:instance_id()

    named_replica:stop()
    server:assert_no_alerts(g.replica_1_a)

    named_replica:drop()
    g.replica_1_a:exec(function(id)
        box.space._cluster:delete(id)
    end, {instance_id})
end
