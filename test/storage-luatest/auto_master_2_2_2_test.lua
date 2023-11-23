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

test_group.before_all(function(g)
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
            format = {
                {'id', 'unsigned'},
                {'bucket_id', 'unsigned'},
            },
        })
        s:create_index('id', {parts = {'id'}})
        s:create_index('bucket_id', {
            parts = {'bucket_id'}, unique = false
        })
    end)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_bootstrap = function(g)
    local function check_auto_master()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end
    local function check_not_auto_master()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end
    g.replica_1_a:exec(check_auto_master)
    g.replica_1_b:exec(check_not_auto_master)
    g.replica_2_a:exec(check_auto_master)
    g.replica_2_b:exec(check_not_auto_master)
    g.replica_3_a:exec(check_auto_master)
    g.replica_3_b:exec(check_not_auto_master)
end

test_group.test_locate = function(g)
    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    g.replica_2_a:exec(function(bid, uuid)
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid, g.replica_3_a:replicaset_uuid()})
    g.replica_3_a:exec(function(bid, uuid)
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})
    g.replica_1_a:exec(function(bid)
        local stat, err = ivshard.storage.bucket_stat(bid)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(stat.status, ivconst.BUCKET.ACTIVE)
    end, {bid})
end

test_group.test_notice_change = function(g)
    --
    -- Send first bucket.
    --
    local bid1 = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    --
    -- Master of the destination replicaset has changed.
    --
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs2_template = new_cfg_template.sharding[2]
    rs2_template.replicas.replica_2_a.read_only = true
    rs2_template.replicas.replica_2_b.read_only = false
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    --
    -- Next bucket send should notice the master change and send the bucket to
    -- the new master.
    --
    local bid2 = g.replica_1_a:exec(function(uuid)
        local rs = ivshard.storage.internal.replicasets[uuid]
        ilt.assert_equals(rs.master.name, 'replica_2_a')
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        ilt.assert_equals(rs.master.name, 'replica_2_b')
        _G.bucket_gc_wait()
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    --
    -- Cleanup.
    --
    g.replica_2_b:exec(function(bid1, bid2, uuid)
        local f1 = ifiber.new(ivshard.storage.bucket_send,
            bid1, uuid, {timeout = _G.iwait_timeout})
        f1:set_joinable(true)
        local f2 = ifiber.new(ivshard.storage.bucket_send,
            bid2, uuid, {timeout = _G.iwait_timeout})
        f2:set_joinable(true)
        local ok_fiber, ok, err = f1:join()
        ilt.assert(ok_fiber)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        ok_fiber, ok, err = f2:join()
        ilt.assert(ok_fiber)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid1, bid2, g.replica_1_a:replicaset_uuid()})

    vtest.cluster_cfg(g, global_cfg)
end

test_group.test_recovery = function(g)
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = true
    end)
    --
    -- Bucket is stuck in SENDING state on rs1 and RECEIVING on rs2.
    --
    local bid = g.replica_1_a:exec(function(uuid)
        _G.bucket_recovery_pause()
        local bid = _G.get_first_bucket()
        box.space.test:replace{1, bid}
        local ok, err = ivshard.storage.bucket_send(bid, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert(not ok)
        ilt.assert_not_equals(err, nil)
        ilt.assert_equals(box.space._bucket:get{bid}.status,
                          ivconst.BUCKET.SENDING)
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    --
    -- Rs2 changes the master while the recovery didn't catch up yet.
    --
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs2_template = new_cfg_template.sharding[2]
    rs2_template.replicas.replica_2_a.read_only = true
    rs2_template.replicas.replica_2_b.read_only = false
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    --
    -- Recovery on rs1 anyway finds the new master on rs2.
    --
    g.replica_1_a:exec(function(bid)
        _G.bucket_recovery_continue()
        _G.bucket_recovery_wait()
        ilt.assert_equals(box.space._bucket:get{bid}.status,
                          ivconst.BUCKET.ACTIVE)
        box.space.test:truncate()
    end, {bid})
    --
    -- Cleanup.
    --
    vtest.cluster_cfg(g, global_cfg)
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
    end)
end

test_group.test_noactivity_timeout_for_auto_master = function(g)
    local bid1, bid2 = g.replica_1_a:exec(function(uuid)
        local bid1 = _G.get_first_bucket()
        --
        -- Use the master connection for anything.
        --
        local ok, err = ivshard.storage.bucket_send(bid1, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        ilt.assert_not_equals(
            ivshard.storage.internal.replicasets[uuid].master, nil)
        --
        -- Wait for the noactivity timeout to expire. The connection must be
        -- dropped. The master role is reset, because it is automatic and wasn't
        -- validated for too long time. Might be outdated already. Better let it
        -- be re-discovered again when needed.
        --
        local old_timeout = ivconst.REPLICA_NOACTIVITY_TIMEOUT
        ivconst.REPLICA_NOACTIVITY_TIMEOUT = 0.01
        ifiber.sleep(ivconst.REPLICA_NOACTIVITY_TIMEOUT)
        ivtest.service_wait_for_new_ok(
            ivshard.storage.internal.conn_manager_service,
            {on_yield = function()
                ivshard.storage.internal.conn_manager_fiber:wakeup()
            end})
        ilt.assert_equals(
            ivshard.storage.internal.replicasets[uuid].master, nil)
        ivconst.REPLICA_NOACTIVITY_TIMEOUT = old_timeout
        --
        -- Re-discover the connection and the master role.
        --
        local bid2 = _G.get_first_bucket()
        ok, err = ivshard.storage.bucket_send(bid2, uuid,
                                              {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        ilt.assert_not_equals(
            ivshard.storage.internal.replicasets[uuid].master, nil)
        _G.bucket_gc_wait()
        return bid1, bid2
    end, {g.replica_2_a:replicaset_uuid()})
    --
    -- Cleanup.
    --
    g.replica_2_a:exec(function(uuid, bid1, bid2)
        local ok, err = ivshard.storage.bucket_send(bid1, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        ok, err = ivshard.storage.bucket_send(bid2, uuid,
                                              {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {g.replica_1_a:replicaset_uuid(), bid1, bid2})
end

test_group.test_conn_manager_connect_self = function(g)
    vtest.cluster_rebalancer_enable(g)
    t.assert_equals(vtest.cluster_rebalancer_find(g), 'replica_1_a')

    g.replica_1_a:exec(function(uuid)
        -- Create connections using rebalancer
        local rebalancer = ivshard.storage.internal.rebalancer_service
        ivtest.service_wait_for_new_ok(rebalancer,
            {on_yield = ivshard.storage.rebalancer_wakeup})

        -- Assert, that connection to self is created.
        local replicaset = ivshard.storage.internal.replicasets[uuid]
        ilt.assert_not_equals(replicaset.master, nil)

        local old_timeout = ivconst.REPLICA_NOACTIVITY_TIMEOUT
        ivconst.REPLICA_NOACTIVITY_TIMEOUT = 0.01
        ifiber.sleep(ivconst.REPLICA_NOACTIVITY_TIMEOUT)
        local conn_manager = ivshard.storage.internal.conn_manager_service
        ivtest.service_wait_for_new_ok(conn_manager, {on_yield = function()
            ivshard.storage.internal.conn_manager_fiber:wakeup()
        end})
        ilt.assert_equals(replicaset.master, nil)
        ivconst.REPLICA_NOACTIVITY_TIMEOUT = old_timeout
    end, {g.replica_1_a:replicaset_uuid()})

    -- Check, that assert not fails
    local err_msg = 'conn_manager_f has been failed'
    t.assert_equals(g.replica_1_a:grep_log(err_msg), nil)

    -- Cleanup
    vtest.cluster_rebalancer_disable(g)
end
