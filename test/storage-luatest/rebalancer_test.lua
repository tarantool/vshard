local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local test_group = t.group('storage')

local cfg_template = {
    sharding = {
        {
            master = 'auto',
            replicas = {
                replica_1_a = {
                    read_only = false,
                },
                replica_1_b = {
                    read_only = true,
                },
            },
        },
        {
            master = 'auto',
            replicas = {
                replica_2_a = {
                    read_only = false,
                },
                replica_2_b = {
                    read_only = true,
                },
            },
        },
        {
            master = 'auto',
            replicas = {
                replica_3_a = {
                    read_only = false,
                },
                replica_3_b = {
                    read_only = true,
                },
            },
        },
    },
    bucket_count = 30
}
local global_cfg

--
-- After reconfig the rebalancer might appear not instantly. For example,
-- box.cfg might decide to re-establish all replica connections and the instance
-- becomes orphan until the connections are back online.
--
local function wait_rebalancer_on_instance(g, instance_name)
    t.helpers.retrying({timeout = vtest.wait_timeout}, function()
        t.assert_equals(vtest.cluster_rebalancer_find(g), instance_name)
    end)
end

test_group.before_all(function(g)
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_exec_each(g, function()
        ivconst.MASTER_SEARCH_WORK_INTERVAL = ivtest.busy_step
    end)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_rebalancer_in_work = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].weight = 0
    new_cfg_template.sharding[2].weight = 0
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    vtest.cluster_rebalancer_enable(g)
    local function wait_n_buckets(storage, count)
        t.helpers.retrying({timeout = vtest.wait_timeout}, storage.exec,
                           storage, function(count)
            local _status = box.space._bucket.index.status
            if _status:count({ivconst.BUCKET.ACTIVE}) ~= count then
                error('Wrong bucket count')
            end
        end, {count})
    end
    wait_n_buckets(g.replica_1_a, 0)
    wait_n_buckets(g.replica_2_a, 0)
    wait_n_buckets(g.replica_3_a, cfg_template.bucket_count)
    vtest.cluster_exec_each_master(g, function()
        _G.bucket_gc_wait()
    end)
    vtest.cluster_cfg(g, global_cfg)
    wait_n_buckets(g.replica_1_a, cfg_template.bucket_count / 3)
    wait_n_buckets(g.replica_2_a, cfg_template.bucket_count / 3)
    wait_n_buckets(g.replica_3_a, cfg_template.bucket_count / 3)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_exec_each_master(g, function()
        _G.bucket_gc_wait()
    end)
end

test_group.test_rebalancer_location = function(g)
    t.assert_equals(vtest.cluster_rebalancer_find(g), 'replica_1_a')

    local new_cfg_template = table.deepcopy(cfg_template)
    --
    -- Move the rebalancer to a different auto-master inside the min replicaset
    -- UUID.
    --
    new_cfg_template.sharding[1].replicas.replica_1_a.read_only = true
    new_cfg_template.sharding[1].replicas.replica_1_b.read_only = false
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_1_b')
    --
    -- Min replicaset has auto master enabled, but no instance has the master
    -- role. Hence no rebalancer at all.
    --
    new_cfg_template.sharding[1].replicas.replica_1_b.read_only = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, nil)
    --
    -- Min replicaset has manual master and it is not specified anywhere. Hence
    -- the rebalancer appears on the min replicaset among the ones having any
    -- master at all. In this case, only auto-masters are left.
    --
    new_cfg_template.sharding[1].master = nil
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_a')
    --
    -- Manual master is specified on the biggest replicaset UUID. It is
    -- prioritized over all auto-masters, regardless of their UUIDs.
    --
    new_cfg_template.sharding[1].master = 'auto'
    new_cfg_template.sharding[1].replicas.replica_1_a.read_only = false
    new_cfg_template.sharding[1].replicas.replica_1_b.read_only = true
    new_cfg_template.sharding[3].master = nil
    new_cfg_template.sharding[3].replicas.replica_3_a.master = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_3_a')
    --
    -- Back to auto-master on the minimal replicaset UUID.
    --
    new_cfg_template.sharding[3].master = 'auto'
    new_cfg_template.sharding[3].replicas.replica_3_a.master = nil
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_1_a')
    --
    -- Cleanup.
    --
    vtest.cluster_cfg(g, global_cfg)
    wait_rebalancer_on_instance(g, 'replica_1_a')
end
