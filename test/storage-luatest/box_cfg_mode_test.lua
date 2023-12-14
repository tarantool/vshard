local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local test_group = t.group()

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {master = true},
            },
        },
        {
            replicas = {
                replica_2_a = {master = true},
            },
        },
    },
    bucket_count = 20,
    box_cfg_mode = 'manual',
    replication_timeout = 0.1,
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

test_group.test_storage_disabled_error = function(g)
    g.replica_1_a:exec(function(cfg)
        local old_box = box.cfg
        box.cfg = function() end
        ilt.assert_error_msg_contains('Box must be configured', function()
            ivshard.storage.cfg(cfg, box.info.uuid)
        end)
        box.cfg = old_box
    end, {global_cfg})
end

--
-- vtest.cluster_new does exactly, what user should do,
-- when using 'manual' box_cfg_mode: call box.cfg prior
-- to executing vshard.storage.cfg. So, just some basic test.
--
test_group.test_storage_basic = function(g)
    -- noreset in order to not depend on reconfiguration in previous tests.
    t.assert(g.replica_1_a:grep_log('Box configuration was skipped',
                                    65536, {noreset = true}))
    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    -- Restore balance.
    g.replica_2_a:exec(function(bid, uuid)
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})
end
