local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local test_group = t.group('log_verbosity')

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
    },
    bucket_count = 30,
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

--
-- gh-588: services should not spam the same errors on every iteration.
--
test_group.test_services_do_not_spam_same_errors = function(g)
    --
    -- Recovery service.
    --
    local bids = vtest.storage_get_n_buckets(g.replica_2_a, 3)
    g.replica_1_a:exec(function(bids)
        for _, bid in ipairs(bids) do
            box.space._bucket:replace{bid, ivconst.BUCKET.RECEIVING}
        end
    end, {bids})
    local msg = "Can not find for bucket"
    g.replica_1_a:assert_log_exactly_once(msg, {timeout = 1,
        on_yield = function() ivshard.storage.recovery_wakeup() end})

    --
    -- GC service.
    --
    vtest.storage_stop(g.replica_1_b)
    g.replica_1_a:exec(function(bids)
        rawset(_G, 'old_wait_lsn_timeout', ivconst.GC_WAIT_LSN_TIMEOUT)
        ivconst.GC_WAIT_LSN_TIMEOUT = 0.5
        box.begin()
        for _, bid in ipairs(bids) do
            box.space._bucket:replace{bid, ivconst.BUCKET.ACTIVE}
            box.space._bucket:replace{bid, ivconst.BUCKET.SENDING}
            box.space._bucket:replace{bid, ivconst.BUCKET.SENT}
        end
        box.commit()
    end, {bids})
    msg = "Error during garbage collection step"
    g.replica_1_a:assert_log_exactly_once(msg, {timeout = 1,
        on_yield = function() ivshard.storage.garbage_collector_wakeup() end})

    vtest.storage_start(g.replica_1_b, global_cfg)
    g.replica_1_a:exec(function(bids)
        ivconst.GC_WAIT_LSN_TIMEOUT = _G.old_wait_lsn_timeout
        _G.old_wait_lsn_timeout = 0
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            for _, bid in ipairs(bids) do
                ilt.assert_not(box.space._bucket:get{bid})
            end
        end)
    end, {bids})
end
