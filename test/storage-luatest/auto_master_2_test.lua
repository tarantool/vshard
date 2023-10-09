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
    },
    bucket_count = 20
}
local global_cfg

test_group.before_all(function(g)
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_bootstrap = function(g)
    g.replica_1_a:exec(function()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
    g.replica_1_b:exec(function()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
end

test_group.test_change = function(g)
    g.replica_1_a:exec(function()
        box.cfg{read_only = true}
        ifiber.yield()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
    g.replica_1_b:exec(function()
        box.cfg{read_only = false}
        ifiber.yield()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
end

test_group.test_turn_off_and_on = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs_cfg = new_cfg_template.sharding[1]
    rs_cfg.master = nil
    rs_cfg.replicas.replica_1_a.read_only = nil
    rs_cfg.replicas.replica_1_b.read_only = nil
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    local function check_master_is_fully_off()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(not ivshard.storage.internal.this_replicaset.is_master_auto)
        box.cfg{read_only = false}
        ilt.assert(not box.info.ro)
        ilt.assert(not ivshard.storage.internal.is_master)
        box.cfg{read_only = true}
    end
    g.replica_1_a:exec(check_master_is_fully_off)
    g.replica_1_b:exec(check_master_is_fully_off)

    rs_cfg.replicas.replica_1_a.master = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    g.replica_1_a:exec(function()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(not ivshard.storage.internal.this_replicaset.is_master_auto)
        box.cfg{read_only = true}
        ilt.assert(box.info.ro)
        ilt.assert(ivshard.storage.internal.is_master)
        box.cfg{read_only = false}
    end)
    g.replica_1_b:exec(check_master_is_fully_off)

    vtest.cluster_cfg(g, global_cfg)
    g.replica_1_a:exec(function()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
    g.replica_1_b:exec(function()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
end
