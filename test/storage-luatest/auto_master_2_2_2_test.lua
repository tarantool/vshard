local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{is_sync = false}, {is_sync = true}}
local test_group = t.group('storage', group_config)

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
    bucket_count = 30,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    t.run_only_if(not g.params.is_sync or
        vutil.version_is_at_least(2, 6, 3, nil, 0, 0))
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_exec_each(g, function()
        ivconst.MASTER_SEARCH_WORK_INTERVAL = ivtest.busy_step
    end)

    vtest.cluster_exec_each_master(g, function(is_sync)
        if is_sync then
            box.ctl.promote()
        end
        local s = box.schema.space.create('test', {
            -- 1.10 doesn't have is_sync option at all.
            is_sync = is_sync or nil,
            format = {
                {'id', 'unsigned'},
                {'bucket_id', 'unsigned'},
            },
        })
        s:create_index('id', {parts = {'id'}})
        s:create_index('bucket_id', {
            parts = {'bucket_id'}, unique = false
        })
    end, {g.params.is_sync})
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

local function promote_if_needed(g, server)
    if g.params.is_sync then
        server:exec(function() box.ctl.promote() end)
    end
end

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
    promote_if_needed(g, g.replica_2_b)
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
    promote_if_needed(g, g.replica_2_a)
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
    promote_if_needed(g, g.replica_2_b)
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
    promote_if_needed(g, g.replica_2_a)
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

test_group.test_master_discovery_on_disconnect = function(g)
    local function bucket_send(bid, uuid)
        local ok, err = ivshard.storage.bucket_send(
            bid, uuid, {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
    end
    local function bucket_gc_wait()
        _G.bucket_gc_wait()
    end
    local function send_bucket_to_new_master(storage_src, storage_dst)
        -- Make sure the bucket will not be delivered even if somehow the tiny
        -- send-timeout appeared to be enough to send it. Just for the
        -- simplicity of the test.
        storage_dst:exec(function()
            ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = true
        end)
        local bid = storage_src:exec(function(rs_uuid, master_uuid)
            local bid = _G.get_first_bucket()
            -- Try to send a bucket. Here the sender notices that the
            -- destination's master is disconnected. A master search is
            -- triggered then.
            local ok, err = ivshard.storage.bucket_send(
                bid, rs_uuid, {timeout = 0.01})
            ilt.assert(not ok)
            ilt.assert_not_equals(err, nil)
            -- Recovery will re-discover the master.
            ivtest.service_wait_for_new_ok(
                ivshard.storage.internal.recovery_service,
                {on_yield = ivshard.storage.recovery_wakeup,
                 timeout = iwait_timeout})
            local rs = ivshard.storage.internal.replicasets[rs_uuid]
            ilt.assert_not_equals(rs.master, nil)
            ilt.assert_equals(rs.master.uuid, master_uuid)
            ilt.assert_equals(box.space._bucket:get{bid}.status,
                              ivconst.BUCKET.ACTIVE)
            return bid
        end, {storage_dst:replicaset_uuid(), storage_dst:instance_uuid()})
        -- Now the bucket can be sent fine.
        storage_dst:exec(function()
            ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
        end)
        storage_src:exec(bucket_send, {bid, storage_dst:replicaset_uuid()})
        storage_src:exec(bucket_gc_wait)
    end
    vtest.cluster_exec_each(g, function()
        rawset(_G, 'test_old_idle_interval',
               ivconst.MASTER_SEARCH_IDLE_INTERVAL)
        ivconst.MASTER_SEARCH_IDLE_INTERVAL = iwait_timeout + 10
    end)
    --
    -- Discover the master first time.
    --
    local bid = vtest.storage_first_bucket(g.replica_1_a)
    g.replica_1_a:exec(bucket_send, {bid, g.replica_2_a:replicaset_uuid()})
    g.replica_1_a:exec(bucket_gc_wait)
    g.replica_2_a:exec(bucket_send, {bid, g.replica_1_a:replicaset_uuid()})
    g.replica_2_a:exec(bucket_gc_wait)
    --
    -- Blocking call while the known master is disconnected.
    --
    g.replica_2_a:stop()
    g.replica_2_b:update_box_cfg{read_only = false}
    promote_if_needed(g, g.replica_2_b)
    send_bucket_to_new_master(g.replica_1_a, g.replica_2_b)
    -- Can't GC the bucket until the old master is back. But can send it.
    g.replica_2_b:exec(bucket_send, {bid, g.replica_1_a:replicaset_uuid()})

    -- Restore everything back.
    g.replica_2_a:start()
    vtest.cluster_cfg(g, global_cfg)
    promote_if_needed(g, g.replica_2_a)
    g.replica_2_b:exec(bucket_gc_wait)
    g.replica_2_b:update_box_cfg{read_only = true}
    vtest.cluster_exec_each(g, function()
        ivconst.MASTER_SEARCH_IDLE_INTERVAL = _G.test_old_idle_interval
        _G.test_old_idle_interval = nil
    end)
end
