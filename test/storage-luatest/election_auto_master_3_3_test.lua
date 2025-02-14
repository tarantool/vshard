local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local test_group = t.group('storage')
local vutil = require('vshard.util')

local cfg_template = {
    sharding = {
        {
            master = 'auto',
            replicas = {
                replica_1_a = {
                    election_mode = 'candidate',
                },
                replica_1_b = {
                    election_mode = 'voter',
                },
                replica_1_c = {
                    election_mode = 'voter',
                },
            },
        },
        {
            master = 'auto',
            replicas = {
                replica_2_a = {
                    election_mode = 'candidate',
                },
                replica_2_b = {
                    election_mode = 'voter',
                },
                replica_2_c = {
                    election_mode = 'voter',
                },
            },
        },
    },
    bucket_count = 30,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    -- `election_mode` was introduced only in 2.6.1.
    t.run_only_if(vutil.version_is_at_least(2, 6, 1, nil, 0, 0))
    global_cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_exec_each(g, function()
        ivconst.MASTER_SEARCH_WORK_INTERVAL = ivtest.busy_step
    end)

    vtest.cluster_exec_each_master(g, function()
        local s = box.schema.space.create('test', {
            is_sync = true,
            format = {
                {'id', 'unsigned'},
                {'bucket_id', 'unsigned'},
            },
        })
        s:create_index('id', {parts = {'id'}})
        s:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
    end)
end)

test_group.after_all(function(g)
    g.cluster:stop()
end)

local function check_auto_master()
    ilt.assert(ivshard.storage.internal.is_master)
    ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
end

local function check_not_auto_master()
    ilt.assert(not ivshard.storage.internal.is_master)
    ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
end

test_group.test_bootstrap = function(g)
    g.replica_1_a:exec(check_auto_master)
    g.replica_1_b:exec(check_not_auto_master)
    g.replica_1_c:exec(check_not_auto_master)
    g.replica_2_a:exec(check_auto_master)
    g.replica_2_b:exec(check_not_auto_master)
    g.replica_2_c:exec(check_not_auto_master)
end

test_group.test_bucket_send = function(g)
    --
    -- Send first bucket.
    --
    local bid1 = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        box.space.test:replace{1, bid}
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    --
    -- Make replica_2_b a candidate and kill master of rs2.
    -- replica_2_b should become leader automatically.
    --
    g.replica_2_b:exec(function()
        box.cfg{election_mode = 'candidate', read_only = false}
    end)
    g.replica_2_a:stop()
    g.replica_2_b:wait_election_leader()
    g.replica_2_b:exec(check_auto_master)
    --
    -- Next bucket send should notice the master change and send the bucket to
    -- the new master sooner or later.
    --
    local bid2 = g.replica_1_a:exec(function(uuid)
        ivtest.service_wait_for_new_ok(
            ivshard.storage.internal.conn_manager_service,
            {on_yield = function()
                ivshard.storage.internal.conn_manager_fiber:wakeup()
            end})
        local rs = ivshard.storage.internal.replicasets[uuid]
        ilt.assert_equals(rs.master, nil)

        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        ilt.assert_equals(rs.master.name, 'replica_2_b')
        _G.bucket_gc_wait()
        return bid
    end, {g.replica_2_b:replicaset_uuid()})

    -- Restore replica to the voter state.
    g.replica_2_a:start()
    vtest.cluster_cfg(g, global_cfg)
    g.replica_2_a:wait_vclock_of(g.replica_2_b)
    g.replica_2_b:exec(function()
        box.cfg{election_mode = 'voter', read_only = true}
    end)
    g.replica_2_a:wait_election_leader()

    -- Return buckets back using rebalancer.
    vtest.cluster_rebalancer_enable(g)
    local rebalancer = vtest.cluster_rebalancer_find(g)
    g[rebalancer]:exec(function()
        local internal = ivshard.storage.internal
        ivtest.service_wait_for_new_ok(internal.rebalancer_service,
            {on_yield = ivshard.storage.rebalancer_wakeup})
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
    end)

    -- Cleanup.
    vtest.cluster_rebalancer_disable(g)
    g.replica_2_a:exec(function(bid1, bid2)
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        local buckets_info = ivshard.storage.buckets_info()
        ilt.assert_equals(buckets_info[bid1], nil)
        ilt.assert_equals(buckets_info[bid2], nil)
    end, {bid1, bid2})
end
