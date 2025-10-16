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

test_group.test_recovery_do_not_spam_same_errors = function(g)
    local bids = vtest.storage_get_n_buckets(g.replica_2_a, 3)
    local uuid = g.replica_2_a:replicaset_uuid()
    vtest.storage_stop(g.replica_2_a)
    g.replica_1_a:exec(function(bids, uuid)
        rawset(_G, 'old_timeout', ivconst.RECOVERY_GET_STAT_TIMEOUT)
        ivconst.RECOVERY_GET_STAT_TIMEOUT = 0.01
        box.begin()
        for _, bid in ipairs(bids) do
            box.space._bucket:replace{bid, ivconst.BUCKET.RECEIVING, uuid}
        end
        box.commit()
    end, {bids, uuid})

    local msg = "Error during recovery of bucket"
    g.replica_1_a:wait_log_exactly_once(msg, {timeout = 0.1,
                                              on_yield = function()
        ivshard.storage.recovery_wakeup()
    end})

    vtest.storage_start(g.replica_2_a, global_cfg)
    g.replica_1_a:exec(function(bids)
        ivconst.RECOVERY_GET_STAT_TIMEOUT = _G.old_timeout
        _G.old_timeout = nil
        for _, bid in ipairs(bids) do
            box.space._bucket:replace{bid, ivconst.BUCKET.GARBAGE}
            box.space._bucket:delete{bid}
        end
    end, {bids})
end

test_group.test_gc_do_not_spam_same_errors = function(g)
    local bids = vtest.storage_get_n_buckets(g.replica_2_a, 3)
    vtest.storage_stop(g.replica_1_b)
    g.replica_1_a:exec(function(bids)
        rawset(_G, 'old_wait_lsn_timeout', ivconst.GC_WAIT_LSN_TIMEOUT)
        ivconst.GC_WAIT_LSN_TIMEOUT = 0.01
        box.begin()
        for _, bid in ipairs(bids) do
            box.space._bucket:replace{bid, ivconst.BUCKET.RECEIVING}
            box.space._bucket:replace{bid, ivconst.BUCKET.ACTIVE}
            box.space._bucket:replace{bid, ivconst.BUCKET.SENDING}
            box.space._bucket:replace{bid, ivconst.BUCKET.SENT}
        end
        box.commit()
    end, {bids})
    local msg = "Error during garbage collection step"
    g.replica_1_a:wait_log_exactly_once(msg, {timeout = 0.1,
                                              on_yield = function()
        ivshard.storage.garbage_collector_wakeup()
    end})

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

test_group.test_rebalancer_do_not_spam_same_errors = function(g)
    vtest.cluster_rebalancer_enable(g)
    g.replica_2_a:exec(function()
        rawset(_G, 'old_call', ivshard.storage.rebalancer_request_state)
        ivshard.storage.rebalancer_request_state = function()
            error('TimedOut')
        end
    end)
    local msg = "Error during downloading rebalancer states"
    g.replica_1_a:wait_log_exactly_once(msg, {timeout = 0.1,
        on_yield = function() ivshard.storage.rebalancer_wakeup() end})
    g.replica_2_a:exec(function()
        ivshard.storage.rebalancer_request_state = _G.old_call
    end)
    vtest.cluster_rebalancer_disable(g)
end
