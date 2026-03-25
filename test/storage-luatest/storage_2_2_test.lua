local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local test_group = t.group('storage')

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true
                },
                replica_1_b = {},
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true
                },
                replica_2_b = {},
            },
        },
    },
    bucket_count = 20,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    t.run_only_if(vutil.version_is_at_least(2, 11, 0, nil, 0, 0))
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
        s:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
    end)
end)

test_group.after_each(function(g)
    vtest.cluster_cfg(g, vtest.config_new(cfg_template))
    vtest.cluster_exec_each_master(g, function() box.space.test:truncate() end)
    vtest.storage_wait_pairsync(g.replica_1_a, g.replica_1_b)
    vtest.storage_wait_pairsync(g.replica_2_a, g.replica_2_b)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

local function move_first_n_buckets(src_storage, dest_storage, n)
    for _ = 1, n do
        local moved_bucket = vtest.storage_first_bucket(src_storage)
        vtest.bucket_move(src_storage, dest_storage, moved_bucket)
    end
end

local function unsync_replicaset_and_switch_master(old_master, new_master)
    -- We should change the replication_synchro_quorum on old_master so that
    -- it can apply a transaction without a confirmation of another replica.
    -- Also we break the replication on replica (new_master in future) so that
    -- it can't get a vclock from old_master, because we must have an unsynced
    -- replicaset after master switch.
    old_master:exec(function() box.cfg{replication_synchro_quorum = 1} end)
    new_master:exec(function()
        box.cfg{replication = {}}
        t.helpers.retrying({}, function()
            for _, conn in pairs(box.info.replication) do
                t.assert_not(conn.upstream)
            end
        end)
    end)
    local old_master_vclock = old_master:exec(function()
        local tuple = {1, _G.get_first_bucket()}
        box.space.test:insert(tuple)
        t.assert_equals(box.space.test:select(), {tuple})
        return box.info.vclock
    end)
    -- Change masters and check that their vclocks are not equal.
    old_master:exec(function() box.cfg{read_only = true} end)
    new_master:exec(function(old_master_vclock)
        local comparator = function(c1, c2) return c1 <= (c2 or 0) end
        t.helpers.retrying({}, function()
            -- In some rare cases the downstream vclock of another instance may
            -- not be updated in time. It can lead to a situation when we will
            -- try to compare the same old_vclock of destination node with
            -- current vclock. It can hinder us to test on_master_enable.
            for _, replica in ipairs(box.info.replication) do
                local downstream = replica.downstream
                if downstream and downstream.vclock then
                    t.assert(require('vshard.util').vclock_compare(
                        old_master_vclock, downstream.vclock, comparator))
                end
            end
            t.assert_not_equals(old_master_vclock, box.info.vclock)
            t.assert_equals(box.space.test:select(), {})
        end)
        box.cfg{read_only = false}
    end, {old_master_vclock})
end

local function switch_master(old_master, new_master)
    new_master:exec(function() box.cfg{read_only = true} end)
    old_master:exec(function() box.cfg{read_only = false} end)
end

local function restore_full_replication(server, replica)
    server:exec(function(replication)
        box.cfg{replication = replication}
    end, {replica.net_box_uri, server.net_box_uri})
    vtest.storage_wait_pairsync(replica, server)
end

local function reset_replicaset_after_master_switch(old_master, new_master)
    new_master:exec(function() box.cfg{read_only = true} end)
    old_master:exec(function() box.cfg{read_only = false,
                                       replication_synchro_quorum = 2} end)
    vtest.storage_wait_pairsync(old_master, new_master)
end

local function cfg_swap_master_of_replicaset(g, rs_num)
    -- This function is applicable only for cluster with 'manual' master mode.
    -- Otherwise it can be dangerous to invoke it.
    local new_cfg_template = table.deepcopy(cfg_template)
    local replicas = new_cfg_template.sharding[rs_num].replicas
    local replica_a = replicas[string.format('replica_%s_a', rs_num)]
    local replica_b = replicas[string.format('replica_%s_b', rs_num)]
    replica_a.master, replica_b.master = not replica_a.master, replica_a.master
    -- If we reconfigure the vshard cluster without 'manual' box_cfg_mode,
    -- the nodes in replicasets will be reconfigured and as a consequence
    -- the replication will be restored. This is unacceptable for us, because
    -- after the vshard config changes we must have the previous topology in
    -- replicaset in order to test the behavious of new unsynced master.
    new_cfg_template.box_cfg_mode = 'manual'
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
end

local function cfg_make_cluster_auto_master(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    for i, rs in ipairs(new_cfg_template.sharding) do
        rs.master = 'auto'
        local replica_a = rs.replicas[string.format('replica_%s_a', i)]
        replica_a.read_only = false
        replica_a.master = nil
        local replica_b = rs.replicas[string.format('replica_%s_b', i)]
        replica_b.read_only = true
    end
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
end

local function minimize_rebalancer_intervals(server)
    -- We should lower the rebalancer idle and work intervals in order to
    -- not hang on long waiting of rebalancer's readiness after enabling
    -- of on_master_enable service or after sudden wakeup.
    server:exec(function()
        local consts = require('vshard.consts')
        rawset(_G, 'old_rebalancer_idle_interval',
               consts.REBALANCER_IDLE_INTERVAL)
        rawset(_G, 'old_rebalancer_work_interval',
               consts.REBALANCER_WORK_INTERVAL)
        consts.REBALANCER_IDLE_INTERVAL = 0.01
        consts.REBALANCER_WORK_INTERVAL = 0.01
    end)
end

local function reset_rebalancer_intervals(server)
    server:exec(function()
        local consts = require('vshard.consts')
        consts.REBALANCER_IDLE_INTERVAL = _G.old_rebalancer_idle_interval
        consts.REBALANCER_WORK_INTERVAL = _G.old_rebalancer_work_interval
    end)
end

local REBALANCER_BAD_START = 'Error during the start of rabalancing:.*' ..
                             'Newly elected master hasn\'t synchronized'

local REBALANCER_SENDS_ROUTES = 'The following rebalancer routes were sent'

local REBALANCER_RECEIVES_ROUTES = 'Apply rebalancer routes with'

local CLUSTER_IS_BALANSED = 'The cluster is balanced ok'

local NEW_MASTER_SYNC = 'New master has synchronized with other replicas'

local function test_rebalancer_sending_until_master_sync(g, old_sender, sender,
                                                         receiver)
    vtest.cluster_rebalancer_enable(g)
    t.helpers.retrying({}, function()
        t.assert(sender:grep_log(REBALANCER_BAD_START))
        t.assert_not(receiver:grep_log(REBALANCER_RECEIVES_ROUTES))
    end)
    restore_full_replication(sender, old_sender)
    t.helpers.retrying({}, function()
        t.assert(sender:grep_log(NEW_MASTER_SYNC))
        sender:exec(function() ivshard.storage.rebalancer_wakeup() end)
        t.assert(receiver:grep_log(REBALANCER_RECEIVES_ROUTES))
        t.assert(sender:grep_log(REBALANCER_SENDS_ROUTES))
        t.assert(sender:grep_log(CLUSTER_IS_BALANSED))
    end)
    reset_replicaset_after_master_switch(old_sender, sender)
    vtest.cluster_rebalancer_disable(g)
end

local function test_rebalancer_receiving_until_master_sync(g, old_receiver,
                                                           receiver, sender)
    vtest.cluster_rebalancer_enable(g)
    t.helpers.retrying({}, function()
        t.assert(receiver:grep_log(REBALANCER_BAD_START))
        t.assert(sender:grep_log(REBALANCER_SENDS_ROUTES))
    end)
    restore_full_replication(receiver, old_receiver)
    t.helpers.retrying({}, function()
        t.assert(receiver:grep_log(NEW_MASTER_SYNC))
        sender:exec(function() ivshard.storage.rebalancer_wakeup() end)
        t.assert(sender:grep_log(REBALANCER_SENDS_ROUTES))
        t.assert(receiver:grep_log(REBALANCER_RECEIVES_ROUTES))
        t.assert(sender:grep_log(CLUSTER_IS_BALANSED))
    end)
    reset_replicaset_after_master_switch(old_receiver, receiver)
    vtest.cluster_rebalancer_disable(g)
end

local function test_recovery_is_nop_until_master_sync(old_sender, sender,
                                                      receiver)
    -- We should set the errinj.ERRINJ_RECEIVE_PARTIALLY on the destination
    -- node in order to test the work of recovery service on unsynced and
    -- synced newly elected master.
    receiver:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = true
    end)
    local bucket_id = vtest.storage_first_bucket(sender)
    local rs_uuid = receiver:replicaset_uuid()
    sender:exec(function(bucket_id, rs_uuid)
        ivshard.storage.bucket_send(bucket_id, rs_uuid)
        t.assert_equals(box.space._bucket:get{bucket_id}.status, 'sending')
    end, {bucket_id, rs_uuid})
    receiver:exec(function() ivshard.storage.recovery_wakeup() end)
    -- The recovery service can't start until the on_master_enable service
    -- finishes its work (syncs new master).
    sender:exec(function(bucket_id)
        ivshard.storage.recovery_wakeup()
        t.helpers.retrying({}, function()
            t.assert_equals(box.space._bucket:get{bucket_id}.status, 'sending')
        end)
    end, {bucket_id})
    t.helpers.retrying({}, function()
        t.assert(sender:grep_log('Error during the start of recovery:.*' ..
                                 'Newly elected master hasn\'t synchronized'))
    end)
    restore_full_replication(sender, old_sender)
    t.helpers.retrying({}, function()
        t.assert(sender:grep_log(NEW_MASTER_SYNC))
    end)
    sender:exec(function(bucket_id)
        ivshard.storage.recovery_wakeup()
        t.helpers.retrying({}, function()
            t.assert_equals(box.space._bucket:get{bucket_id}.status, 'active')
        end)
    end, {bucket_id})
    t.assert(sender:grep_log('Finish bucket recovery step, 1 ' ..
                             'sending buckets are recovered among 1'))
    reset_replicaset_after_master_switch(old_sender, sender)
    receiver:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_RECEIVE_PARTIALLY = false
    end)
end

test_group.test_rebalancer_sending_with_manual_master_switch = function(g)
    minimize_rebalancer_intervals(g.replica_1_b)
    local old_sender, sender, receiver = g.replica_1_a, g.replica_1_b,
                                         g.replica_2_a
    move_first_n_buckets(old_sender, receiver, 3)
    unsync_replicaset_and_switch_master(old_sender, sender)
    cfg_swap_master_of_replicaset(g, 1)
    test_rebalancer_sending_until_master_sync(g, old_sender, sender, receiver)
    reset_rebalancer_intervals(g.replica_1_b)
end

test_group.test_rebalancer_receiving_with_manual_master_switch = function(g)
    minimize_rebalancer_intervals(g.replica_1_a)
    minimize_rebalancer_intervals(g.replica_2_b)
    local old_receiver, receiver, sender = g.replica_2_a, g.replica_2_b,
                                           g.replica_1_a
    move_first_n_buckets(sender, old_receiver, 3)
    unsync_replicaset_and_switch_master(old_receiver, receiver)
    cfg_swap_master_of_replicaset(g, 2)
    test_rebalancer_receiving_until_master_sync(g, old_receiver, receiver,
                                                sender)
    reset_rebalancer_intervals(g.replica_1_a)
    reset_rebalancer_intervals(g.replica_2_b)
end

test_group.test_recovery_with_manual_master_switch = function(g)
    local old_sender, sender, receiver = g.replica_1_a, g.replica_1_b,
                                         g.replica_2_a
    unsync_replicaset_and_switch_master(old_sender, sender)
    cfg_swap_master_of_replicaset(g, 1)
    test_recovery_is_nop_until_master_sync(old_sender, sender, receiver)
end

test_group.test_rebalancer_sending_with_auto_master_switch = function(g)
    minimize_rebalancer_intervals(g.replica_1_b)
    local old_sender, sender, receiver = g.replica_1_a, g.replica_1_b,
                                         g.replica_2_a
    move_first_n_buckets(old_sender, receiver, 3)
    cfg_make_cluster_auto_master(g)
    unsync_replicaset_and_switch_master(old_sender, sender)
    test_rebalancer_sending_until_master_sync(g, old_sender, sender, receiver)
    reset_rebalancer_intervals(g.replica_1_b)
end

test_group.test_rebalancer_receiving_with_auto_master_switch = function(g)
    minimize_rebalancer_intervals(g.replica_1_a)
    minimize_rebalancer_intervals(g.replica_2_b)
    local old_receiver, receiver, sender = g.replica_2_a, g.replica_2_b,
                                           g.replica_1_a
    move_first_n_buckets(sender, old_receiver, 3)
    cfg_make_cluster_auto_master(g)
    unsync_replicaset_and_switch_master(old_receiver, receiver)
    test_rebalancer_receiving_until_master_sync(g, old_receiver, receiver,
                                                sender)
    reset_rebalancer_intervals(g.replica_1_a)
    reset_rebalancer_intervals(g.replica_2_b)
end

test_group.test_recovery_with_auto_master_switch = function(g)
    local old_sender, sender, receiver = g.replica_1_a, g.replica_1_b,
                                         g.replica_2_a
    cfg_make_cluster_auto_master(g)
    unsync_replicaset_and_switch_master(old_sender, sender)
    test_recovery_is_nop_until_master_sync(old_sender, sender, receiver)
end

test_group.test_on_master_enable_during_multiple_master_switch = function(g)
    cfg_make_cluster_auto_master(g)
    unsync_replicaset_and_switch_master(g.replica_1_a, g.replica_1_b)
    g.replica_1_b:exec(function()
        t.assert(ivshard.storage.internal.is_master)
        local f = ivshard.storage.internal.on_master_enable_fiber
        local s = ivshard.storage.internal.on_master_enable_service
        ivtest.service_wait_for_activity(s, 'idling')
        t.assert(f and s)
        -- on_master_enable_fiber will be in "suspended" mode until
        -- new master syncs with other replica.
        t.assert_equals(f:status(), 'suspended')
        rawset(_G, 'on_master_enable_fiber_id', f:id())
        rawset(_G, 'on_master_enable_fiber_status', f:status())
        t.assert_not(ivshard.storage.internal.buckets_are_in_sync)
    end)
    switch_master(g.replica_1_a, g.replica_1_b)
    g.replica_1_a:exec(function()
        t.assert(ivshard.storage.internal.is_master)
    end)
    switch_master(g.replica_1_b, g.replica_1_a)
    g.replica_1_b:exec(function()
        t.assert(ivshard.storage.internal.is_master)
        local f = ivshard.storage.internal.on_master_enable_fiber
        local s = ivshard.storage.internal.on_master_enable_service
        t.assert(f and s)
        -- We should be aware that after new master switch the on_master_enable
        -- fiber and service will not be recreated. Otherwise there is a chance
        -- that old on_master_enable_fiber may interfere in switching of
        -- M.buckets_are_in_sync which can lead to prematurely start of
        -- rebalancer and recovery services.
        t.assert_equals(f:id(), _G.on_master_enable_fiber_id)
        t.assert_equals(f:status(), _G.on_master_enable_fiber_status)
        t.assert_not(ivshard.storage.internal.buckets_are_in_sync)
    end)
    restore_full_replication(g.replica_1_b, g.replica_1_a)
    t.helpers.retrying({}, function()
        g.replica_1_b:exec(function()
            local s = ivshard.storage.internal.on_master_enable_service
            ivtest.service_wait_for_activity(s, 'synced')
        end)
        t.assert(g.replica_1_b:grep_log(NEW_MASTER_SYNC))
    end)
    g.replica_1_b:exec(function()
        t.assert_not(ivshard.storage.internal.on_master_enable_fiber)
        t.assert_not(ivshard.storage.internal.on_master_enable_service)
        t.assert(ivshard.storage.internal.buckets_are_in_sync)
    end)
end
