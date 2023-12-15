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
        {
            replicas = {
                replica_3_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 15,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Test that manual vshard.storage.bucket_send() cannot lead to
-- doubled buckets (gh-414).
--
test_group.test_manual_bucket_send_doubled_buckets = function(g)
    vtest.cluster_exec_each_master(g, function()
        _G.bucket_recovery_pause()
    end)

    local uuid_2 = g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LONG_RECEIVE = true
        return ivutil.replicaset_uuid()
    end)

    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert(iverror.is_timeout(err))
        ilt.assert_not(ok, 'bucket_send not ok')
        return bid
    end, {uuid_2})

    g.replica_2_a:exec(function(bid, uuid)
        ivshard.storage.internal.errinj.ERRINJ_LONG_RECEIVE = false
        ilt.assert_equals(box.space._bucket:get(bid).status,
                          ivconst.BUCKET.ACTIVE)
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err, nil, 'bucket_send no error')
        ilt.assert(ok, 'bucket_send ok')
        _G.bucket_recovery_continue()
    end, {bid, g.replica_3_a:replicaset_uuid()})

    g.replica_3_a:exec(function(bid)
        ilt.assert_equals(box.space._bucket:get(bid).status,
                          ivconst.BUCKET.ACTIVE)
    end, {bid})

    g.replica_1_a:exec(function(bid)
        _G.bucket_recovery_continue()
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        ilt.assert_equals(box.space._bucket:get(bid), nil)
    end, {bid})
end
