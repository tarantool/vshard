local t = require('luatest')
local fiber = require('fiber')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')
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

local function bucket_gc_wait()
    _G.bucket_gc_wait()
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

local function bucket_set_protection(value)
    ivshard.storage.internal.is_bucket_protected = value
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
-- Callro in the end can notice that the bucket is gone. Can't treat the request
-- as successful then - it could read a partially deleted bucket.
--
local function test_storage_callro_refro_loss(g, user_err)
    local rep_a = g.replica_1_a
    local rep_b = g.replica_1_b

    local bid = vtest.storage_first_bucket(rep_a)
    -- Start an RO request on the replica.
    local f = call_long_start(rep_b, bid, 'read', user_err)

    -- The bucket becomes deleted on the master due to any reason. Need to pause
    -- the protection. Otherwise the replica would reject the bucket drop.
    rep_b:exec(bucket_set_protection, {false})
    vtest.cluster_exec_each(g, bucket_set_protection, {false})
    rep_a:exec(bucket_force_drop, {bid})
    rep_b:wait_vclock_of(rep_a)
    vtest.cluster_exec_each(g, bucket_set_protection, {true})
    rep_b:exec(bucket_set_protection, {true})
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

    -- The bucket becomes deleted on the new master due to any reason. Need to
    -- pause the protection. Otherwise the old master would reject the bucket
    -- drop.
    vtest.cluster_exec_each(g, bucket_set_protection, {false})
    rep_b:exec(bucket_force_drop, {bid})
    rep_a:wait_vclock_of(rep_a)
    vtest.cluster_exec_each(g, bucket_set_protection, {true})
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
