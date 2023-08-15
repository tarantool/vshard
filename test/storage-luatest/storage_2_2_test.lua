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
        },
    },
    bucket_count = 16,
    rebalancer_mode = 'off',
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

local function find_rebalancer_name(g)
    local map, err = vtest.cluster_exec_each(g, function()
        return ivshard.storage.internal.rebalancer_fiber ~= nil
    end)
    t.assert_equals(err, nil)
    local rebalancer_name
    for name, has_rebalancer in pairs(map) do
        if has_rebalancer then
            t.assert_equals(rebalancer_name, nil)
            rebalancer_name = name
        end
    end
    return rebalancer_name
end

test_group.test_rebalancer_mode_off_no_marks = function(g)
    local _, err = vtest.cluster_exec_each(g, function()
        ilt.assert(not ivshard.storage.internal.rebalancer_fiber)
    end)
    t.assert_equals(err, nil)
end

test_group.test_rebalancer_mode_off_ignore_marks = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].replicas.replica_1_a.rebalancer = true
    local new_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_cfg)
    local _, err = vtest.cluster_exec_each(g, function()
        ilt.assert(not ivshard.storage.internal.rebalancer_fiber)
    end)
    t.assert_equals(err, nil)
    -- Revert.
    vtest.cluster_cfg(g, global_cfg)
end

test_group.test_rebalancer_mode_auto_ignore_marks = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.rebalancer_mode = 'auto'
    local new_cfg = vtest.config_new(new_cfg_template)
    local map, err = vtest.cluster_exec_each(g, function(cfg)
        -- Tell each instance it should be the rebalancer. But it must be
        -- ignored when mode = 'auto'.
        local info = box.info
        cfg.sharding[ivutil.replicaset_uuid(info)]
            .replicas[info.uuid].rebalancer = true
        local _, err = ivshard.storage.cfg(cfg, box.info.uuid)
        ilt.assert_equals(err, nil)
        return {
            has_rebalancer = ivshard.storage.internal.rebalancer_fiber ~= nil,
            uuid = box.info.uuid,
        }
    end, {new_cfg})
    t.assert_equals(err, nil)
    local min_uuid
    local rebalancer_uuid
    for _, res in pairs(map) do
        if not min_uuid or min_uuid > res.uuid then
            min_uuid = res.uuid
        end
        if res.has_rebalancer then
            t.assert_equals(rebalancer_uuid, nil)
            rebalancer_uuid = res.uuid
        end
    end
    t.assert_equals(min_uuid, rebalancer_uuid)
    -- Revert.
    vtest.cluster_cfg(g, global_cfg)
end

test_group.test_rebalancer_mode_manual_no_marks = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.rebalancer_mode = 'manual'
    local new_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_cfg)
    local _, err = vtest.cluster_exec_each(g, function()
        ilt.assert(not ivshard.storage.internal.rebalancer_fiber)
    end)
    t.assert_equals(err, nil)
    -- Revert.
    vtest.cluster_cfg(g, global_cfg)
end

test_group.test_rebalancer_mode_manual_with_mark = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.rebalancer_mode = 'manual'

    -- Try a master.
    new_cfg_template.sharding[1].replicas.replica_1_a.rebalancer = true
    local new_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_cfg)
    t.assert_equals(find_rebalancer_name(g), 'replica_1_a')

    -- Try a replica.
    new_cfg_template.sharding[1].replicas.replica_1_a.rebalancer = nil
    new_cfg_template.sharding[2].replicas.replica_2_b.rebalancer = true
    new_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_cfg)
    t.assert_equals(find_rebalancer_name(g), 'replica_2_b')

    -- Ensure it works even on the replica.
    vtest.cluster_rebalancer_disable(g)
    g.replica_1_a:exec(function(rs2_uuid)
        while true do
            local bid = _G.get_first_bucket()
            if not bid then
                break
            end
            local _, err = ivshard.storage.bucket_send(
                bid, rs2_uuid, {timeout = iwait_timeout})
            ilt.assert_equals(err, nil)
        end
    end, {g.replica_2_a:replicaset_uuid()})
    vtest.cluster_rebalancer_enable(g)

    g.replica_2_b:exec(function()
        ivtest.service_wait_for_new_ok(
            ivshard.storage.internal.rebalancer_service, {
                on_yield = ivshard.storage.rebalancer_wakeup
            })
    end)
    local _, err = vtest.cluster_exec_each_master(g, function(bucket_count)
        _G.bucket_gc_wait()
        ilt.assert_equals(ivshard.storage.buckets_count(), bucket_count)
    end, {new_cfg_template.bucket_count / #new_cfg_template.sharding})
    t.assert_equals(err, nil)

    -- Revert.
    vtest.cluster_cfg(g, global_cfg)
end
