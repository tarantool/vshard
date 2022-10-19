local t = require('luatest')
local fiber = require('fiber')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
local vconst = require('vshard.consts')
local verror = require('vshard.error')

local group_config = {{}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {memtx_use_mvcc_engine = true})
end

local test_group = t.group('storage', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
            },
        },
    },
    bucket_count = 20
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
    g.replica_1_a:exec(function()
        _G.bucket_recovery_pause()
    end)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

local function bucket_refro(bid)
    local ok, err = ivshard.storage.bucket_refro(bid)
    ilt.assert_equals(err, nil)
    ilt.assert(ok)
end

local function bucket_refro_fail(bid, errcode)
    local ok, err = ivshard.storage.bucket_refro(bid)
    ilt.assert(not ok)
    ilt.assert_equals(err.code, errcode)
end

local function bucket_unrefro(bid)
    local ok, err = ivshard.storage.bucket_unrefro(bid)
    ilt.assert_equals(err, nil)
    ilt.assert(ok)
end

local function bucket_gc_wait()
    _G.bucket_gc_wait()
end

local function bucket_update_status(bid, status)
    box.space._bucket:update({bid}, {{'=', 2, status}})
end

local function bucket_force_create(bid)
    ivshard.storage.bucket_force_create(bid)
end

local function bucket_force_drop(bid)
    ivshard.storage.bucket_force_drop(bid)
end

local function bucket_check_no_ref(bid)
    -- When called after GC polling, the bucket can be not visible in _bucket
    -- already, but not committed yet, and its ref could still be there.
    _G.wal_sync()
    ilt.assert_equals(ivshard.storage.internal.bucket_refs[bid], nil)
end

local function bucket_check_has_ref(bid)
    ilt.assert_not_equals(ivshard.storage.internal.bucket_refs[bid], nil)
end

local function bucket_check_ref_ro_lock(bid)
    ilt.assert(ivshard.storage.internal.bucket_refs[bid].ro_lock)
end

local function bucket_check_ref_no_ro_lock(bid)
    ilt.assert(not ivshard.storage.internal.bucket_refs[bid].ro_lock)
end

local function call_long_start(storage, bid, mode, err)
     local f = fiber.new(storage.exec, storage, function(bid, mode, err)
        rawset(_G, 'do_wait', true)
        rawset(_G, 'wait_f', function()
            rawset(_G, 'is_waiting', true)
            while _G.do_wait do
                ifiber.sleep(0.01)
            end
            -- Restore.
            _G.is_waiting = false
            _G.do_wait = true
            if err ~= nil then
                box.error(box.error.PROC_LUA, err)
            end
            return true
        end)
        return ivshard.storage.call(bid, mode, 'wait_f')
    end, {bid, mode, err})
    f:set_joinable(true)
    t.helpers.retrying({timeout = vtest.wait_timeout}, storage.exec, storage,
        function()
            if not _G.is_waiting then
                error('No request')
            end
        end)
    return f
end

local function call_long_stop()
    _G.do_wait = false
end

--
-- Test what happens when a bucket is unable to get new reads nor writes.
--
local function test_bucket_space_trigger_garbage(g, status)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b

    local bid = vtest.storage_first_bucket(rep_a)
    -- Ref the bucket to make it create a cashed ref object with non-zero refs
    -- in it. It should prevent its garbage collection until the refs are gone.
    rep_b:exec(bucket_refro, {bid})
    rep_a:exec(bucket_refro, {bid})

    -- Turn the bucket into garbage.
    t.assert_items_include({vconst.BUCKET.SENT, vconst.BUCKET.GARBAGE},
                           {status})
    rep_a:exec(bucket_update_status, {bid, status})
    rep_a:exec(bucket_refro_fail, {bid, verror.code.BUCKET_IS_LOCKED})

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_refro_fail, {bid, verror.code.BUCKET_IS_LOCKED})

    -- Collect the bucket.
    rep_b:exec(bucket_unrefro, {bid})
    rep_a:exec(bucket_unrefro, {bid})
    rep_a:exec(bucket_gc_wait)

    -- Ensure its ref is gone on all nodes.
    rep_a:exec(bucket_check_no_ref, {bid})
    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref, {bid})

    -- Ensure it works when a bucket has no refs too.
    rep_a:exec(bucket_force_create, {bid})
    rep_a:exec(bucket_update_status, {bid, status})
    rep_a:exec(bucket_gc_wait)

    rep_a:exec(bucket_check_no_ref, {bid})
    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref, {bid})

    -- Cleanup.
    rep_a:exec(bucket_force_create, {bid})
    rep_b:wait_vclock_of(rep_a)
end

local function test_bucket_space_trigger_active(g, status)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b

    -- Make a non-active bucket.
    local bid = vtest.storage_first_bucket(rep_a)
    rep_a:exec(bucket_update_status, {bid, vconst.BUCKET.SENDING})
    rep_a:exec(bucket_check_no_ref)

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref)

    -- Bucket turn into an active one works fine when it had no refs.
    t.assert_items_include({vconst.BUCKET.ACTIVE, vconst.BUCKET.PINNED},
                           {status})
    rep_a:exec(bucket_update_status, {bid, status})
    rep_a:exec(bucket_check_no_ref)

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref)

    -- Turn from a non-active ro-locked bucket back into active is an abnormal
    -- situation. Nonetheless, should drop the lock then.
    rep_a:exec(bucket_refro, {bid})
    rep_b:exec(bucket_refro, {bid})

    rep_a:exec(bucket_update_status, {bid, vconst.BUCKET.SENT})
    rep_a:exec(bucket_check_ref_ro_lock, {bid})

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_ref_ro_lock, {bid})

    rep_a:exec(bucket_update_status, {bid, status})
    rep_a:exec(bucket_check_ref_no_ro_lock, {bid})

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_ref_no_ro_lock, {bid})

    -- Restore.
    rep_a:exec(bucket_unrefro, {bid})
    rep_b:exec(bucket_unrefro, {bid})
    rep_a:exec(bucket_force_drop, {bid})
    rep_a:exec(bucket_check_no_ref)
    rep_a:exec(bucket_force_create, {bid})
    rep_b:wait_vclock_of(rep_a)
end

test_group.test_bucket_space_trigger_sent = function(g)
    test_bucket_space_trigger_garbage(g, vconst.BUCKET.SENT)
end

test_group.test_bucket_space_trigger_garbage = function(g)
    test_bucket_space_trigger_garbage(g, vconst.BUCKET.GARBAGE)
end

test_group.test_bucket_space_trigger_active = function(g)
    test_bucket_space_trigger_active(g, vconst.BUCKET.ACTIVE)
end

test_group.test_bucket_space_trigger_pinned = function(g)
    test_bucket_space_trigger_active(g, vconst.BUCKET.PINNED)
end

test_group.test_bucket_space_triggers_receiving = function(g)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b

    -- Commit of RECEIVING bucket works fine when no refs. That is a part of the
    -- normal process of bucket receipt.
    local bid = vtest.storage_first_bucket(rep_a)
    rep_a:exec(bucket_force_drop, {bid})
    rep_a:exec(bucket_update_status, {bid, vconst.BUCKET.RECEIVING})
    rep_a:exec(bucket_check_no_ref, {bid})

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref, {bid})

    rep_a:exec(bucket_force_drop, {bid})
    rep_a:exec(bucket_force_create, {bid})
    rep_b:wait_vclock_of(rep_a)

    -- Commit of RECEIVING bucket works fine when has refs. Abnormal situation.
    -- Nonetheless, should drop the refs then.
    rep_a:exec(bucket_refro, {bid})
    rep_b:exec(bucket_refro, {bid})
    rep_a:exec(bucket_check_has_ref, {bid})
    rep_a:exec(bucket_update_status, {bid, vconst.BUCKET.RECEIVING})
    rep_a:exec(bucket_check_no_ref, {bid})

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref, {bid})

    -- Restore.
    rep_a:exec(bucket_force_drop, {bid})
    rep_a:exec(bucket_force_create, {bid})
    rep_b:wait_vclock_of(rep_a)
end

--
-- Callro in the end can notice that the bucket is gone. Can't treat the request
-- as successful then - it could read a partially deleted bucket.
--
local function test_storage_callro_refro_loss(g, user_err)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b

    local bid = vtest.storage_first_bucket(rep_a)
    -- Start an RO request on the replica.
    local f = call_long_start(rep_b, bid, 'read', user_err)

    -- The bucket becomes deleted on the master due to any reason.
    rep_a:exec(bucket_force_drop, {bid})
    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref, {bid})

    -- The replica should fail to complete the RO request.
    rep_b:exec(call_long_stop)
    local ok_fiber, ok_storage, err = f:join()
    t.assert(ok_fiber)
    t.assert_equals(ok_storage, nil)
    -- The error is critical. Bucket refs are messed up.
    t.assert(err.code, verror.code.BUCKET_IS_CORRUPTED)
    -- The user's exception is also preserved.
    if user_err ~= nil then
        t.assert_equals(err.prev.message, user_err)
    else
        t.assert_equals(err.prev, nil)
    end
    rep_b:exec(bucket_check_no_ref, {bid})

    -- Restore.
    rep_a:exec(bucket_gc_wait)
    rep_a:exec(bucket_force_create, {bid})
    rep_b:wait_vclock_of(rep_a)
end

test_group.test_storage_callro_refro_loss = function(g)
    test_storage_callro_refro_loss(g)
    test_storage_callro_refro_loss(g, 'test_error')
end

--
-- Callrw in the end can notice that the bucket is gone. Can't treat the request
-- as successful then - it could write into a partially deleted bucket.
--
local function test_storage_callro_refrw_loss(g, user_err)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b

    local bid = vtest.storage_first_bucket(rep_a)
    -- Start an RW request on the master.
    local f = call_long_start(rep_a, bid, 'write', user_err)

    -- Change the master role during the RW request execution.
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs1_replicas = new_cfg_template.sharding[1].replicas
    rs1_replicas.replica_1_a.master = false
    rs1_replicas.replica_1_b.master = true
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    -- The bucket becomes deleted on the new master due to any reason.
    rep_b:exec(bucket_force_drop, {bid})
    rep_a:wait_vclock_of(rep_a)
    rep_a:exec(bucket_check_no_ref, {bid})

    -- The old master should fail to complete the RW request.
    rep_a:exec(call_long_stop)
    local ok_fiber, ok_storage, err = f:join()
    t.assert(ok_fiber)
    t.assert_equals(ok_storage, nil)
    -- The error is critical. Bucket refs are messed up.
    t.assert(err.code, verror.code.BUCKET_IS_CORRUPTED)
    -- The user's exception is also preserved.
    if user_err ~= nil then
        t.assert_equals(err.prev.message, user_err)
    else
        t.assert_equals(err.prev, nil)
    end
    rep_a:exec(bucket_check_no_ref, {bid})

    -- Restore.
    vtest.cluster_cfg(g, global_cfg)
    rep_a:exec(bucket_gc_wait)
    rep_a:exec(bucket_force_create, {bid})
    rep_b:wait_vclock_of(rep_a)
end

test_group.test_storage_callro_refrw_loss = function(g)
    test_storage_callro_refrw_loss(g)
    test_storage_callro_refrw_loss(g, 'test_error')
end
