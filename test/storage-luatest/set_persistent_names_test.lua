local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{engine = 'memtx'}, {engine = 'vinyl'}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {
        engine = 'memtx', memtx_use_mvcc_engine = true
    })
    table.insert(group_config, {
        engine = 'vinyl', memtx_use_mvcc_engine = true
    })
end

local test_group = t.group('storage', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 10,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    t.run_only_if(vutil.feature.persistent_names)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_named_config_identification = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.identification_mode = 'name_as_key'
    new_cfg_template.sharding['replicaset_1'] = new_cfg_template.sharding[1]
    new_cfg_template.sharding['replicaset_2'] = new_cfg_template.sharding[2]
    new_cfg_template.sharding[1] = nil
    new_cfg_template.sharding[2] = nil
    local new_global_cfg = vtest.config_new(new_cfg_template)

    -- Attempt to configure with named config without name set causes error.
    g.replica_1_a:exec(function(cfg)
        -- Name is not shown in info, when uuid identification is used.
        local info = ivshard.storage.info()
        local rs = info.replicasets[ivutil.replicaset_uuid()]
        ilt.assert_equals(rs.name, nil)
        ilt.assert_equals(rs.master.name, nil)
        ilt.assert_equals(info.identification_mode, 'uuid_as_key')
        ilt.assert_error_msg_contains('Instance name mismatch',
                                      ivshard.storage.cfg, cfg, 'replica_1_a')
    end, {new_global_cfg})

    -- Set names on all replicas.
    g.replica_1_a:exec(function()
        box.cfg{instance_name = 'replica_1_a', replicaset_name = 'replicaset_1'}
    end)
    g.replica_2_a:exec(function()
        box.cfg{instance_name = 'replica_2_a', replicaset_name = 'replicaset_2'}
    end)

    -- Check, that UUIDs are validated, when named config is used.
    local rs_1_cfg = new_global_cfg.sharding.replicaset_1
    local replica_1_a_cfg = rs_1_cfg.replicas.replica_1_a
    replica_1_a_cfg.uuid = g.replica_2_a:instance_uuid()
    g.replica_1_a:exec(function(cfg)
        ilt.assert_error_msg_contains('Instance UUID mismatch',
                                      ivshard.storage.cfg, cfg, 'replica_1_a')
    end, {new_global_cfg})
    -- The correct UUID should be OK.
    replica_1_a_cfg.uuid = g.replica_1_a:instance_uuid()

    -- Now the config can be finally applied.
    vtest.cluster_cfg(g, new_global_cfg)

    -- Test, that sending by name works properly
    local rs_name_2 = g.replica_2_a:replicaset_name()
    t.assert_equals(rs_name_2, 'replicaset_2')
    g.replica_1_a:exec(function(name)
        -- Name is shown, when name identification is used.
        local info = ivshard.storage.info()
        local rs = info.replicasets[box.info.replicaset.name]
        ilt.assert_equals(rs.name, box.info.replicaset.name)
        ilt.assert_equals(rs.uuid, nil)
        ilt.assert_equals(rs.master.name, box.info.name)
        ilt.assert_equals(info.identification_mode, 'name_as_key')

        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(
            bid, name, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
    end, {rs_name_2})

    -- Test, that rebalancer is also ok.
    vtest.cluster_rebalancer_enable(g)
    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        ivtest.service_wait_for_new_ok(internal.rebalancer_service,
            {on_yield = ivshard.storage.rebalancer_wakeup})

        -- Cleanup
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        ilt.assert_equals(box.space._bucket:count(), 5)
    end)

    g.replica_2_a:exec(function()
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        ilt.assert_equals(box.space._bucket:count(), 5)
    end)

    vtest.cluster_rebalancer_disable(g)
    -- Back to UUID identification.
    vtest.cluster_cfg(g, global_cfg)
end
