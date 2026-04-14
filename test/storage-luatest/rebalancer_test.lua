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
    bucket_count = 30,
    replication_timeout = 0.1,
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

local function wait_n_buckets(storage, count)
    t.helpers.retrying({timeout = vtest.wait_timeout}, storage.exec,
                       storage, function(count)
        ivshard.storage.rebalancer_wakeup()
        local _status = box.space._bucket.index.status
        if _status:count({ivconst.BUCKET.ACTIVE}) ~= count then
            error('Wrong bucket count')
        end
    end, {count})
end

test_group.test_rebalancer_in_work = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].weight = 0
    new_cfg_template.sharding[2].weight = 0
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    vtest.cluster_rebalancer_enable(g)
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

test_group.test_locate_with_flag = function(g)
    t.assert_equals(vtest.cluster_rebalancer_find(g), 'replica_1_a')
    --
    -- Assign to another replicaset, with a non-minimal UUID.
    --
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].rebalancer = false
    new_cfg_template.sharding[2].rebalancer = true
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_a')
    --
    -- Automatically move the rebalancer together with the master role.
    --
    new_cfg_template.sharding[2].replicas.replica_2_a.read_only = true
    new_cfg_template.sharding[2].replicas.replica_2_b.read_only = false
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_b')
    --
    -- Assign to the replicaset with the maximal UUID.
    --
    new_cfg_template.sharding[2].rebalancer = false
    new_cfg_template.sharding[3].rebalancer = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_3_a')
    --
    -- Assign explicitly to a read-only replica.
    --
    new_cfg_template.sharding[3].master = nil
    new_cfg_template.sharding[3].replicas.replica_3_b.master = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_3_b')
    --
    -- Forbid the rebalancer on the min-UUID replicaset. Then the replicaset
    -- with the next UUID is used.
    --
    new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].rebalancer = false
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_a')
    --
    -- Forbid to run on the current master. But on a replica it won't run
    -- without an explicit flag. Hence no rebalancer at all.
    --
    new_cfg_template.sharding[2].replicas.replica_2_a.rebalancer = false
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g)
    --
    -- The master appears on another instance. The rebalancer can finally start.
    --
    new_cfg_template.sharding[2].replicas.replica_2_b.read_only = false
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_b')
    --
    -- Explicitly assign to a replica in a non-min-UUID replicaset. Without
    -- setting this flag for any replicaset.
    --
    new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].rebalancer = nil
    new_cfg_template.sharding[2].replicas.replica_2_b.rebalancer = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_b')
    --
    -- Cleanup.
    --
    vtest.cluster_cfg(g, global_cfg)
    wait_rebalancer_on_instance(g, 'replica_1_a')
end

test_group.test_rebalancer_mode = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    --
    -- Auto-mode won't ignore rebalancer flags. It can only do any difference
    -- when the rebalancer is not specified explicitly.
    --
    new_cfg_template.rebalancer_mode = 'auto'
    new_cfg_template.sharding[1].rebalancer = nil
    new_cfg_template.sharding[2].rebalancer = true
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_a')
    --
    -- The rebalancer false-flags are taken into account.
    --
    new_cfg_template.sharding[1].rebalancer = false
    new_cfg_template.sharding[2].rebalancer = false
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_3_a')
    --
    -- The flags don't matter then the rebalancer is off.
    --
    new_cfg_template.rebalancer_mode = 'off'
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, nil)
    --
    -- Manual with a rebalancer assigned explicitly to an instance.
    --
    new_cfg_template.rebalancer_mode = 'manual'
    new_cfg_template.sharding[2].rebalancer = nil
    new_cfg_template.sharding[2].replicas.replica_2_b.rebalancer = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_2_b')
    --
    -- Manual with a rebalancer assigned explicitly to a replicaset.
    --
    new_cfg_template.rebalancer_mode = 'manual'
    new_cfg_template.sharding[2].replicas.replica_2_b.rebalancer = nil
    new_cfg_template.sharding[3].rebalancer = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, 'replica_3_a')
    --
    -- Manual with no explicitly assigned rebalancer means no rebalancer at all.
    --
    new_cfg_template.sharding[3].rebalancer = nil
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_rebalancer_on_instance(g, nil)
    --
    -- Cleanup.
    --
    vtest.cluster_cfg(g, global_cfg)
    wait_rebalancer_on_instance(g, 'replica_1_a')
end

--
-- gh-573: test the behavior of the READONLY bucket.
--
test_group.test_readonly_recovery = function(g)
    g.replica_1_a:exec(function(uuid)
        --
        -- If bucket send ends unsuccessfully before sending happens, recovery
        -- makes it active again after some time.
        --
        local bid = _G.get_first_bucket()
        ilt.assert(ivshard.storage.bucket_refrw(bid))
        -- Unsuccessful send.
        local _, err = ivshard.storage.bucket_send(bid, uuid, {timeout = 0.01})
        ilt.assert(iverror.is_timeout(err))
        local bucket = ivshard.storage.bucket_stat(bid)
        ilt.assert_not(bucket.is_transfering)
        ilt.assert_equals(bucket.status, ivconst.BUCKET.READONLY)
        -- Impossible to pin the READONLY bucket.
        ilt.assert_not(ivshard.storage.bucket_pin(bid))
        -- Recovery restores the bucket to ACTIVE.
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.recovery_wakeup()
            local b = ivshard.storage.bucket_stat(bid)
            ilt.assert_equals(b.status, ivconst.BUCKET.ACTIVE)
        end)
        ilt.assert(ivshard.storage.bucket_unrefrw(bid))

        --
        -- it's impossible to prepare PINNED bucket for sending and cannot pin
        -- the READONLY bucket.
        --
        ilt.assert(ivshard.storage.bucket_pin(bid))
        _, err = ivshard.storage.bucket_send(bid, uuid, {timeout = 0.1})
        ilt.assert_equals(err.code, iverror.code.BUCKET_IS_PINNED)
        ilt.assert(ivshard.storage.bucket_unpin(bid))
    end, {g.replica_2_a:replicaset_uuid()})
end

--
-- Test, that the worker sends buckets in batches. Sending more buckets, than
-- the storage has leads to error. After error during preparation all buckets
-- can be recovered.
--
test_group.test_send_more_buckets_than_has = function(g)
    vtest.cluster_exec_each_master(g, function()
        local current_cfg = ivshard.storage.internal.current_cfg
        current_cfg.rebalancer_max_sending = 6
        ivshard.storage.cfg(current_cfg, box.info.uuid)
    end)

    vtest.cluster_recovery_disable(g)
    -- Start sending of 11 buckets, when replica has only 10.
    g.replica_1_a:exec(function(uuid)
        ilt.assert_equals(box.space._bucket:count(), 10)
        -- Returns ok no matter what.
        ilt.assert(ivshard.storage.rebalancer_apply_routes({[uuid] = 11}))
    end, {g.replica_2_a:replicaset_uuid()})
    -- Wait for error message to happen.
    t.helpers.retrying({timeout = vtest.iwait_timeout}, function()
        t.assert(g.replica_1_a:grep_log('Can not find active buckets'))
    end)
    -- Since recovery is disabled all non-sent buckets remain in READONLY state.
    g.replica_1_a:exec(function()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ivshard.storage.garbage_collector_wakeup()
            ilt.assert_equals(box.space._bucket:count(), 4)
        end)
        local transfer_flags =
            ivshard.storage.internal.rebalancer_transfering_buckets
        for _, b in box.space._bucket:pairs() do
            ilt.assert_equals(b.status, ivconst.BUCKET.READONLY)
            ilt.assert_not(transfer_flags[b.id])
        end
    end)
    -- Recovery restores the READONLY buckets back.
    vtest.cluster_recovery_enable(g)
    g.replica_1_a:exec(function()
        local active_key = {ivconst.BUCKET.ACTIVE}
        local _status = box.space._bucket.index.status
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_equals(_status:count(active_key), 4)
            ivshard.storage.recovery_wakeup()
        end)
    end)

    -- Restore the cluster with rebalancer.
    vtest.cluster_rebalancer_enable(g)
    wait_n_buckets(g.replica_1_a, cfg_template.bucket_count / 3)
    wait_n_buckets(g.replica_2_a, cfg_template.bucket_count / 3)
    wait_n_buckets(g.replica_3_a, cfg_template.bucket_count / 3)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_cfg(g, global_cfg)
end

--
-- gh-351: rebalancer should prefer buckets without rw refs.
--
test_group.test_buckets_with_no_refs_are_preferred = function(g)
    vtest.cluster_rebalancer_enable(g)
    -- Make rw "requests" to all buckets, except the single one.
    local bid = g.replica_1_a:exec(function()
        -- The maximum available bucket id is not refed.
        local idx = box.space._bucket.index.status
        local opts = {limit = 1, iterator = 'LE'}
        local bid = idx:select(ivconst.BUCKET.ACTIVE, opts)[1].id
        for _, b in box.space._bucket:pairs() do
            if b.id ~= bid then
                ivshard.storage.bucket_refrw(b.id)
            end
        end
        return bid
    end)

    -- Move one bucket, it must be the one without refs.
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].weight = cfg_template.bucket_count / 3 - 1
    new_cfg_template.sharding[2].weight = cfg_template.bucket_count / 3 + 1
    new_cfg_template.sharding[3].weight = cfg_template.bucket_count / 3
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    wait_n_buckets(g.replica_1_a, 9)
    wait_n_buckets(g.replica_2_a, 11)
    g.replica_2_a:exec(function(bid)
        local status = ivshard.storage.bucket_stat(bid).status
        ilt.assert_equals(status, ivconst.BUCKET.ACTIVE)
    end, {bid})

    -- Even with all bucket refed we still try to send them.
    new_cfg_template.sharding[1].weight = 0
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    g.replica_1_a:exec(function(bid)
        -- Wait for routes applier start and picking a buckets.
        local internal = ivshard.storage.internal
        local applier_name = 'routes_applier_service'
        ivtest.wait_for_not_nil(internal, applier_name,
                                {timeout = iwait_timeout,
                                 on_yield = ivshard.storage.rebalancer_wakeup})
        local service = internal[applier_name]
        ivtest.service_wait_for_activity(service, 'applying routes')
        -- End rw "requests".
        for _, b in box.space._bucket:pairs() do
            if b.id ~= bid then
                ivshard.storage.bucket_unrefrw(b.id)
            end
        end
    end, {bid})
    wait_n_buckets(g.replica_2_a, cfg_template.bucket_count / 2)
    wait_n_buckets(g.replica_3_a, cfg_template.bucket_count / 2)

    vtest.cluster_cfg(g, global_cfg)
    wait_n_buckets(g.replica_1_a, cfg_template.bucket_count / 3)
    wait_n_buckets(g.replica_2_a, cfg_template.bucket_count / 3)
    wait_n_buckets(g.replica_3_a, cfg_template.bucket_count / 3)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_exec_each_master(g, function()
        _G.bucket_gc_wait()
    end)
end
