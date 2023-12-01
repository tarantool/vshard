local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{mode = 'auto'}, {mode = 'manual_access'}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {
        mode = 'auto', memtx_use_mvcc_engine = true
    })
    table.insert(group_config, {
        mode = 'manual_access', memtx_use_mvcc_engine = true
    })
end

local test_group = t.group('storage_schema_management_mode', group_config)

local cfg_template = {
    sharding = {
        {
            master = 'auto',
            replicas = {
                replica_1_a = {read_only = false},
                replica_1_b = {read_only = true},
            },
        },
        {
            master = 'auto',
            replicas = {
                replica_2_a = {read_only = false},
            },
        },
    },
    bucket_count = 10,
}

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    cfg_template.schema_management_mode = g.params.mode
    local cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, cfg)
    vtest.cluster_bootstrap(g, cfg)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
    g.cluster = nil
end)

test_group.test_boot_with_mode_manual_access = function(g)
    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    --
    -- Switch to another mode. Still works fine.
    --
    local test_template = table.deepcopy(cfg_template)
    if test_template.schema_management_mode == 'auto' then
        test_template.schema_management_mode = 'manual_access'
    else
        test_template.schema_management_mode = 'auto'
    end
    vtest.cluster_cfg(g, vtest.config_new(test_template))
    g.replica_2_a:exec(function(bid, uuid)
        local ok, err = ivshard.storage.bucket_send(bid, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})
end
