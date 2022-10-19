local t = require('luatest')
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

local function bucket_set_protection(value)
    ivshard.storage.internal.is_bucket_protected = value
end

--
-- Test what happens when a bucket is unable to get new reads nor writes.
--
local function test_bucket_space_commit_trigger_garbage(g, status)
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
    -- GARBAGE bucket can't have refs, such updates are rejected. Have to pause
    -- the protection for them.
    local is_unsafe = status == vconst.BUCKET.GARBAGE
    if is_unsafe then
        vtest.cluster_exec_each(g, bucket_set_protection, {false})
    end
    rep_a:exec(bucket_update_status, {bid, status})
    rep_a:exec(bucket_refro_fail, {bid, verror.code.BUCKET_IS_LOCKED})

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_refro_fail, {bid, verror.code.BUCKET_IS_LOCKED})
    if is_unsafe then
        vtest.cluster_exec_each(g, bucket_set_protection, {true})
    end

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

local function test_bucket_space_commit_trigger_active(g, status)
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

test_group.test_bucket_space_commit_trigger_sent = function(g)
    test_bucket_space_commit_trigger_garbage(g, vconst.BUCKET.SENT)
end

test_group.test_bucket_space_commit_trigger_garbage = function(g)
    test_bucket_space_commit_trigger_garbage(g, vconst.BUCKET.GARBAGE)
    vtest.cluster_exec_each(g, bucket_set_protection, {true})
end

test_group.test_bucket_space_commit_trigger_active = function(g)
    test_bucket_space_commit_trigger_active(g, vconst.BUCKET.ACTIVE)
end

test_group.test_bucket_space_commit_trigger_pinned = function(g)
    test_bucket_space_commit_trigger_active(g, vconst.BUCKET.PINNED)
end

test_group.test_bucket_space_commit_trigger_receiving = function(g)
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
    -- Have to pause the protection because getting a RECEIVING bucket with refs
    -- normally is rejected.
    vtest.cluster_exec_each(g, bucket_set_protection, {false})
    rep_a:exec(bucket_update_status, {bid, vconst.BUCKET.RECEIVING})
    rep_a:exec(bucket_check_no_ref, {bid})

    rep_b:wait_vclock_of(rep_a)
    rep_b:exec(bucket_check_no_ref, {bid})
    vtest.cluster_exec_each(g, bucket_set_protection, {true})

    -- Restore.
    rep_a:exec(bucket_force_drop, {bid})
    rep_a:exec(bucket_force_create, {bid})
    rep_b:wait_vclock_of(rep_a)
end

--
-- Protection against illegal changes in _bucket.
--
test_group.test_bucket_space_reject_bad_replace = function(g)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b
    rep_a:exec(function()
        local bid = _G.get_first_bucket()
        local _bucket = box.space._bucket
        local ok, err
        local all_refs = ivshard.storage.internal.bucket_refs
        local function check_invalid_update(func, msg)
            ok, err = pcall(func)
            ilt.assert(not ok)
            -- Exceptions are wrapped into box errors' messages. Thus vshard
            -- error is serialized into a string.
            ilt.assert_str_contains(err.message, 'BUCKET_INVALID_UPDATE')
            ilt.assert_str_contains(err.message, msg)
        end
        --
        -- New bucket but there is an existing ref with the same ID. Can even be
        -- empty.
        --
        ivshard.storage.bucket_refro(bid)
        ivshard.storage.bucket_unrefro(bid)
        local ref = all_refs[bid]
        ilt.assert_not_equals(ref, nil)
        ivshard.storage.bucket_force_drop(bid)
        ilt.assert_equals(all_refs[bid], nil)
        all_refs[bid] = ref
        check_invalid_update(function()
            _bucket:replace{bid, ivconst.BUCKET.ACTIVE}
        end, "can't have a ref")
        all_refs[bid] = nil
        _bucket:replace{bid, ivconst.BUCKET.ACTIVE}
        ivshard.storage.bucket_force_create(bid)
        --
        -- Transitions blocked by RW ref presence.
        --
        ivshard.storage.bucket_refrw(bid)
        for _, status in pairs({
            ivconst.BUCKET.SENDING, ivconst.BUCKET.SENT
        }) do
            check_invalid_update(function()
                _bucket:update({bid}, {{'=', 2, status}})
            end, "can't have rw refs")
        end
        ivshard.storage.bucket_unrefrw(bid)
        --
        -- Transitions blocked by any ref.
        --
        for _, status in pairs({
            ivconst.BUCKET.GARBAGE, ivconst.BUCKET.RECEIVING
        }) do
            ivshard.storage.bucket_refro(bid)
            check_invalid_update(function()
                _bucket:update({bid}, {{'=', 2, status}})
            end, "can't have any refs")
            ivshard.storage.bucket_unrefro(bid)
            ivshard.storage.bucket_refrw(bid)
            check_invalid_update(function()
                _bucket:update({bid}, {{'=', 2, status}})
            end, "can't have any refs")
            ivshard.storage.bucket_unrefrw(bid)
        end
        --
        -- Invalid status.
        --
        check_invalid_update(function()
            _bucket:update({bid}, {{'=', 2, 'test_bad_status'}})
        end, "unknown bucket status")
        --
        -- Can't delete with any refs.
        --
        ivshard.storage.bucket_refro(bid)
        check_invalid_update(function() _bucket:delete({bid}) end,
                             "a bucket with refs")
        ivshard.storage.bucket_unrefro(bid)
        ivshard.storage.bucket_refrw(bid)
        check_invalid_update(function() _bucket:delete({bid}) end,
                             "a bucket with refs")
        ivshard.storage.bucket_unrefrw(bid)
    end)
    rep_b:wait_vclock_of(rep_a)
end

--
-- Protection against illegal _bucket truncation.
--
test_group.test_bucket_space_truncation = function(g)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b
    --
    -- Can't truncate _bucket.
    --
    rep_a:exec(function()
        local _bucket = box.space._bucket
        local ok, err = pcall(_bucket.truncate, _bucket)
        ilt.assert(not ok)
        ilt.assert_equals(err.code, box.error.UNSUPPORTED)
        ilt.assert_str_contains(err.message, '_bucket truncation')
    end)
    --
    -- Without protection _bucket can be truncated and will drop all the refs.
    --
    local bid = vtest.storage_first_bucket(rep_a)
    rep_a:exec(bucket_refro, {bid})
    rep_b:exec(bucket_refro, {bid})
    vtest.cluster_exec_each(g, bucket_set_protection, {false})
    rep_a:exec(function()
        local _bucket = box.space._bucket
        rawset(_G, 'all_refs', ivshard.storage.internal.bucket_refs)
        rawset(_G, 'all_buckets', _bucket:select())
        _bucket:truncate()
    end)
    rep_b:wait_vclock_of(rep_a)
    vtest.cluster_exec_each(g, bucket_set_protection, {true})
    rep_a:exec(function()
        local _bucket = box.space._bucket
        local new_all_refs = ivshard.storage.internal.bucket_refs
        ilt.assert_not_equals(_G.all_refs, new_all_refs)
        ilt.assert_equals((next(new_all_refs)), nil)
        _G.all_refs = nil
        box.begin()
        for _, t in pairs(_G.all_buckets) do
            _bucket:replace(t)
        end
        box.commit()
        _G.all_buckets = nil
    end)
    rep_b:wait_vclock_of(rep_a)
end
