local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local lmsgpack = require('msgpack')
local netbox = require('net.box') -- for net.box:self()
local trigger = require('internal.trigger')
local ffi = require('ffi')
local yaml_encode = require('yaml').encode
local fiber_clock = lfiber.clock
local fiber_yield = lfiber.yield
local netbox_self = netbox.self
local netbox_self_call = netbox_self.call

local MODULE_INTERNALS = '__module_vshard_storage'
-- Reload requirements, in case this module is reloaded manually.
if rawget(_G, MODULE_INTERNALS) then
    local vshard_modules = {
        'vshard.consts', 'vshard.error', 'vshard.cfg', 'vshard.version',
        'vshard.replicaset', 'vshard.util', 'vshard.service_info',
        'vshard.storage.reload_evolution', 'vshard.rlist', 'vshard.registry',
        'vshard.heap', 'vshard.storage.ref', 'vshard.storage.sched',
        'vshard.storage.schema', 'vshard.storage.export_log',
        'vshard.storage.exports'
    }
    for _, module in pairs(vshard_modules) do
        package.loaded[module] = nil
    end
end
local rlist = require('vshard.rlist')
local consts = require('vshard.consts')
local lerror = require('vshard.error')
local lcfg = require('vshard.cfg')
local lreplicaset = require('vshard.replicaset')
local util = require('vshard.util')
local lregistry = require('vshard.registry')
local lservice_info = require('vshard.service_info')
local lref = require('vshard.storage.ref')
local lsched = require('vshard.storage.sched')
local lschema = require('vshard.storage.schema')
local reload_evolution = require('vshard.storage.reload_evolution')
local fiber_cond_wait = util.fiber_cond_wait
local index_has = util.index_has
local BSTATE = consts.BUCKET
local bucket_ref_new

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    ffi.cdef[[
        struct bucket_ref {
            uint32_t ro;
            uint32_t rw;
            bool rw_lock;
            bool ro_lock;
        };
    ]]
    bucket_ref_new = ffi.metatype("struct bucket_ref", {})
    --
    -- The module is loaded for the first time.
    --
    -- !!!WARNING: any change of this table must be reflected in
    -- `vshard.storage.reload_evolution` module to guarantee
    -- reloadability of the module.
    M = {
        ---------------- Common module attributes ----------------
        -- The last passed configuration.
        current_cfg = nil,
        --
        -- All known replicasets used for bucket re-balancing.
        -- See format in replicaset.lua.
        --
        replicasets = nil,
        -- Triggers on master switch event. They are called right
        -- before the event occurs.
        _on_master_enable = trigger.new('_on_master_enable'),
        _on_master_disable = trigger.new('_on_master_disable'),
        -- Trigger on bucket change event. It's called as the first
        -- operation in bucket related transaction. All supported
        -- types of trigger are inside vshard.consts.BUCKET_EVENT.
        _on_bucket_event = trigger.new('_on_bucket_event'),
        -- Bucket count stored on all replicasets.
        total_bucket_count = 0,
        errinj = {
            ERRINJ_CFG = false,
            ERRINJ_RELOAD = false,
            ERRINJ_CFG_DELAY = false,
            ERRINJ_LONG_RECEIVE = false,
            ERRINJ_LAST_RECEIVE_DELAY = false,
            ERRINJ_LAST_SEND_DELAY = false,
            ERRINJ_RECEIVE_PARTIALLY = false,
            ERRINJ_RECOVERY_PAUSE = false,
            ERRINJ_DISCOVERY = false,
            ERRINJ_BUCKET_GC_PAUSE = false,
            ERRINJ_BUCKET_GC_LONG_REPLICAS_TEST = false,
            ERRINJ_APPLY_ROUTES_STOP_DELAY = false,
        },
        -- This counter is used to restart background fibers with
        -- new reloaded code.
        module_version = 0,
        --
        -- Timeout to wait sync with slaves. Used on master
        -- demotion or on a manual sync() call.
        --
        sync_timeout = consts.DEFAULT_SYNC_TIMEOUT,
        -- References to a parent replicaset and self in it.
        this_replicaset = nil,
        this_replica = nil,
        -- Control whether _bucket changes should be validated and rejected if
        -- are invalid.
        is_bucket_protected = true,
        --
        -- Incremental generation of the _bucket space. It is
        -- incremented on each _bucket change and is used to
        -- detect that _bucket was not changed between yields.
        --
        bucket_generation = 0,
        -- Condition variable fired on generation update.
        bucket_generation_cond = lfiber.cond(),
        --
        -- References to the functions used as on_replace triggers on spaces.
        -- They are used to replace the triggers with new functions when reload
        -- happens. They are kept explicitly because the old functions are
        -- deleted on reload from the global namespace. On the other hand, they
        -- are still stored in space:on_replace() somewhere, but it is not known
        -- where. The only 100% way to be able to replace the old functions is
        -- to keep their references.
        --
        bucket_on_replace = nil,
        bucket_on_truncate = nil,
        --
        -- Reference to the function used as on_replace trigger on
        -- _schema space. Saved explicitly by the same reason as
        -- _bucket on_replace.
        -- It is used by replicas to wait for schema bootstrap
        -- because they might be configured earlier than the
        -- master.
        schema_on_replace = nil,
        -- Fast alternative to box.space._bucket:count(). But may be nil. Reset
        -- on each generation change.
        bucket_count_cache = nil,
        -- Fast alternative to checking multiple keys presence in
        -- box.space._bucket status index. But may be nil. Reset on each
        -- generation change.
        bucket_are_all_rw_cache = nil,
        -- Redirects for recently sent buckets. They are kept for a while to
        -- help routers find a new location for sent and deleted buckets without
        -- whole cluster scan.
        route_map = {},
        -- Flag whether vshard.storage.cfg() is in progress.
        is_cfg_in_progress = false,
        -- Flag whether vshard.storage.cfg() is finished.
        is_configured = false,
        -- Flag whether box.info.status is acceptable. For instance, 'loading'
        -- is not.
        is_loaded = false,
        -- Flag whether the instance is enabled manually. It is true by default
        -- for backward compatibility with old vshard.
        is_enabled = true,
        -- Flag whether this instance right now is in the master role.
        is_master = false,
        -- Reference to the function-proxy to most of the public functions. It
        -- allows to avoid 'if's in each function by adding expensive
        -- conditional checks in one rarely used version of the wrapper and no
        -- checks into the other almost always used wrapper.
        api_call_cache = nil,

        --------------- Instance state management ----------------
        -- Fiber for monitoring the instance state and making changes related
        -- to it.
        instance_watch_fiber = nil,
        instance_watch_service = nil,

        ----------------- Connection management ------------------
        conn_manager_fiber = nil,
        conn_manager_service = nil,

        ------------------- Garbage collection -------------------
        -- Fiber to remove garbage buckets data.
        collect_bucket_garbage_fiber = nil,
        -- Save statuses and errors for the gc fiber
        gc_service = nil,
        -- How many times the GC fiber performed the garbage collection.
        bucket_gc_count = 0,

        -------------------- Bucket recovery ---------------------
        recovery_fiber = nil,
        -- Save statuses and errors for the recovery fiber
        recovery_service = nil,

        ----------------------- Rebalancer -----------------------
        -- Fiber to rebalance a cluster.
        rebalancer_fiber = nil,
        -- Save statuses and errors for the rebalancer fiber
        rebalancer_service = nil,
        -- Fiber which applies routes one by one. Its presence and
        -- active status means that the rebalancing is in progress
        -- now on the current node.
        rebalancer_applier_fiber = nil,
        -- Save statuses and errors for the rebalancer_applier fiber
        routes_applier_service = nil,
        -- Internal flag to activate and deactivate rebalancer. Mostly
        -- for tests.
        is_rebalancer_active = true,
        -- Maximal allowed percent deviation of bucket count on a
        -- replicaset from etalon bucket count.
        rebalancer_disbalance_threshold = 0,
        -- How many more receiving buckets this replicaset can
        -- handle simultaneously. This is not just a constant
        -- 'max' number, because in that case a 'count' would be
        -- needed. And such a 'count' wouldn't be precise, because
        -- 'receiving' buckets can appear not only from
        -- bucket_recv. For example, the tests can manually create
        -- receiving buckets.
        rebalancer_receiving_quota = consts.DEFAULT_REBALANCER_MAX_RECEIVING,
        -- Identifier of a bucket that rebalancer is sending or
        -- receiving now. If a bucket has state SENDING/RECEIVING,
        -- but its id is not stored here, it means, that its
        -- transfer has failed.
        rebalancer_transfering_buckets = {},
        -- How many worker fibers will execute the rebalancer
        -- order in parallel. Each fiber sends 1 bucket at a
        -- moment.
        rebalancer_worker_count = consts.DEFAULT_REBALANCER_WORKER_COUNT,
        -- Map of bucket ro/rw reference counters. These counters
        -- works like bucket pins, but countable and are not
        -- persisted. Persistence is not needed since the refs are
        -- used to keep a bucket during a request execution, but
        -- on restart evidently each request fails.
        bucket_refs = {},
        -- Condition variable fired each time a bucket locked for
        -- RW refs reaches 0 of the latter.
        bucket_rw_lock_is_ready_cond = lfiber.cond(),

        ------------------------- Reload -------------------------
        -- Version of the loaded module. This number is used on
        -- reload to determine which upgrade scripts to run.
        reload_version = reload_evolution.version,
    }
else
    bucket_ref_new = ffi.typeof("struct bucket_ref")

    -- It is not set when reloaded from an old vshard version.
    if M.is_enabled == nil then
        M.is_enabled = true
    end
    if M.bucket_gc_count == nil then
        M.bucket_gc_count = 0
    end
    if M.is_bucket_protected == nil then
        M.is_bucket_protected = true
    end
    if M._on_bucket_event == nil then
        M._on_bucket_event = trigger.new('_on_bucket_event')
    end
    if M.is_master == nil then
        M.is_master = M.this_replicaset.master == M.this_replica
    end
end

--
-- Invoke a function on this instance. Arguments are unpacked into the function
-- as arguments.
-- The function returns pcall() as is, because is used from places where
-- exceptions are not allowed.
--
local local_call

if util.version_is_at_least(3, 0, 0, 'beta', 1, 18) then

local_call = function(func_name, args)
    return pcall(netbox_self_call, netbox_self, func_name, args)
end

else -- < 3.0.0-beta1-18

-- net_box.self.call() doesn't work with C stored and Lua persistent
-- functions before 3.0.0-beta1-18, so we try to call it via func.call
-- API prior to using net_box.self API.
local_call = function(func_name, args)
    local func = box.func and box.func[func_name]
    if not func then
        return pcall(netbox_self_call, netbox_self, func_name, args)
    end
    return pcall(func.call, func, args)
end

end

local function master_call(replicaset, func, args, opts)
    local deadline = fiber_clock() + opts.timeout
    local did_first_attempt = false
    while true do
        local res, err = replicaset:callrw(func, args, opts)
        if res ~= nil then
            return res
        end
        if err == nil then
            return
        end
        err = lerror.make(err)
        if err.code ~= lerror.code.NON_MASTER then
            return nil, err
        end
        if not replicaset:update_master(err.replica, err.master) then
            return nil, err
        end
        local timeout = deadline - fiber_clock()
        if timeout <= 0 then
            return nil, lerror.timeout()
        end
        if not did_first_attempt then
            opts = table.copy(opts)
            did_first_attempt = true
        end
        opts.timeout = timeout
    end
end

--
-- Get number of buckets stored on this storage. Regardless of their state.
--
-- The idea is that all the code should use one function ref to get the bucket
-- count. But inside the function never branches. Instead, it points at one of 2
-- branch-less functions. Cached one simply returns a number which is supposed
-- to be super fast. Non-cached remembers the count and changes the global
-- function to the cached one. So on the next call it is cheap. No 'if's at all.
--
local bucket_count

local function bucket_count_cache()
    return M.bucket_count_cache
end

local function bucket_count_not_cache()
    local count = box.space._bucket:count()
    M.bucket_count_cache = count
    bucket_count = bucket_count_cache
    return count
end

bucket_count = bucket_count_not_cache

--
-- Can't expose bucket_count to the public API as is. Need this proxy-call.
-- Because the original function changes at runtime.
--
local function bucket_count_public()
    return bucket_count()
end

--
-- Check if all buckets on the storage are writable. The idea is the same as
-- with bucket count - the value changes very rare, and is cached most of the
-- time. Only that its non-cached calculation is more expensive than with count.
--
local bucket_are_all_rw

local function bucket_are_all_rw_cache()
    return M.bucket_are_all_rw_cache
end

local function bucket_are_all_rw_not_cache()
    local status_index = box.space._bucket.index.status
    local status = consts.BUCKET
    local res = not index_has(status_index, status.SENDING) and
       not index_has(status_index, status.SENT) and
       not index_has(status_index, status.RECEIVING) and
       not index_has(status_index, status.GARBAGE)

    M.bucket_are_all_rw_cache = res
    bucket_are_all_rw = bucket_are_all_rw_cache
    return res
end

bucket_are_all_rw = bucket_are_all_rw_not_cache

local function bucket_are_all_rw_public()
    return bucket_are_all_rw()
end

--
-- Trigger for on replace into _bucket to update its generation.
--
local function bucket_generation_increment()
    bucket_count = bucket_count_not_cache
    bucket_are_all_rw = bucket_are_all_rw_not_cache
    M.bucket_count_cache = nil
    M.bucket_are_all_rw_cache = nil
    M.bucket_generation = M.bucket_generation + 1
    M.bucket_generation_cond:broadcast()
end

local function bucket_transfer_start(bid)
    if M.rebalancer_transfering_buckets[bid] then
        return nil, lerror.vshard(lerror.code.WRONG_BUCKET, bid,
                                  'transfer is already in progress', nil)
    end
    M.rebalancer_transfering_buckets[bid] = true
    return true
end

local function bucket_transfer_end(bid)
    assert(M.rebalancer_transfering_buckets[bid])
    M.rebalancer_transfering_buckets[bid] = nil
    bucket_generation_increment()
end

--
-- Handle a bad update of _bucket space.
--
local function bucket_reject_update(bid, message, ...)
    if not M.is_bucket_protected then
        return
    end
    error(lerror.vshard(lerror.code.BUCKET_INVALID_UPDATE, bid,
                        message:format(...)))
end

--
-- Make _bucket-related data up to date with the committed changes.
--
local function bucket_commit_update(bucket)
    local status = bucket.status
    local bid = bucket.id
    if status == consts.BUCKET.SENT or status == consts.BUCKET.GARBAGE then
        local ref = M.bucket_refs[bid]
        if ref ~= nil then
            ref.ro_lock = true
        end
        return
    end
    if status == consts.BUCKET.RECEIVING then
        -- It shouldn't have a ref, but prepare to anything. Could be, for
        -- example, ACTIVE changed into RECEIVING manually via
        -- _bucket:replace().
        if M.bucket_refs[bid] ~= nil then
            M.bucket_refs[bid] = nil
        end
        return
    end
    if status == consts.BUCKET.PINNED or status == consts.BUCKET.ACTIVE then
        local ref = M.bucket_refs[bid]
        if ref ~= nil then
            -- Lock shouldn't be ever set here. But could be that an admin
            -- decided to restore a SENT bucket back into ACTIVE state due to
            -- any reason.
            ref.ro_lock = false
        end
        return
    end
    -- Ignore unknown statuses. Could be set no idea how, but just ignore.
end

local function bucket_commit_delete(bucket)
    M.bucket_refs[bucket.id] = nil
end

local bucket_state_edges = {
    -- From nothing. Have to cast nil to box.NULL explicitly then.
    [box.NULL] = {BSTATE.RECEIVING},
    [BSTATE.ACTIVE] = {BSTATE.PINNED, BSTATE.SENDING},
    [BSTATE.PINNED] = {BSTATE.ACTIVE},
    [BSTATE.SENDING] = {BSTATE.ACTIVE, BSTATE.SENT},
    [BSTATE.SENT] = {BSTATE.GARBAGE},
    [BSTATE.RECEIVING] = {BSTATE.GARBAGE, BSTATE.ACTIVE},
    [BSTATE.GARBAGE] = {box.NULL},
}

-- Turn the edges into a table where edges[src][dst] would be true if the
-- src->dst transition is allowed.
for src, dsts in pairs(bucket_state_edges) do
    local dst_dict = {}
    for _, dst in pairs(dsts) do
        dst_dict[dst] = true
    end
    bucket_state_edges[src] = dst_dict
end

--
-- Validate whether _bucket update is possible. Bad ones need to be rejected for
-- the sake of keeping data consistency. Ideally it should never throw. But
-- there sure are bugs and this trigger should help to catch them from time to
-- time.
--
local function bucket_prepare_update(old_bucket, new_bucket)
    local new_status = new_bucket.status
    local old_status = old_bucket ~= nil and old_bucket.status or box.NULL
    local bid = new_bucket.id
    local ref = M.bucket_refs[bid]
    -- Check if existing refs allow the bucket to transfer to a new status.
    if old_bucket == nil and ref ~= nil then
        bucket_reject_update(bid, "new bucket can't have a ref object")
    end
    if new_status == BSTATE.SENDING or new_status == BSTATE.SENT then
        if ref ~= nil and ref.rw > 0 then
            bucket_reject_update(bid, "'%s' bucket can't have rw refs",
                                 new_status)
        end
    elseif new_status == BSTATE.GARBAGE or new_status == BSTATE.RECEIVING then
        if ref ~= nil and (ref.rw > 0 or ref.ro > 0) then
            bucket_reject_update(bid, "'%s' bucket can't have any refs",
                                 new_status)
        end
    end
    if new_status == old_status then
        return
    end
    local src = bucket_state_edges[old_status]
    if not src or not bucket_state_edges[new_status] then
        bucket_reject_update(bid, "unknown bucket status '%s'",
                             not src and old_status or new_status)
    elseif not src[new_status] then
        bucket_reject_update(bid, "transition '%s' to '%s' is not allowed",
                             old_status == box.NULL and 'nil' or old_status,
                             new_status == box.NULL and 'nil' or new_status)
    end
end

local function bucket_prepare_delete(bucket)
    local bid = bucket.id
    local status = bucket.status
    local ref = M.bucket_refs[bid]
    if ref ~= nil and (ref.ro > 0 or ref.rw > 0) then
        bucket_reject_update(bid, "can't delete a bucket with refs")
    end
    local src = bucket_state_edges[status]
    if not src then
        bucket_reject_update(bid, "unknown bucket status '%s'", status)
    elseif not src[box.NULL] then
        bucket_reject_update(bid, "can't delete '%s' bucket", status)
    end
end

--
-- Handle commit or rollback in _bucket space. The trigger not only processes
-- normal changes, but also tries to repair abnormal ones into at least some
-- detectable state. Throwing an error here is too late, would lead to a crash.
--
local function bucket_on_commit_or_rollback_f(row_pairs, is_commit)
    local bucket_space_id = box.space._bucket.id
    for _, t1, t2, space_id in row_pairs() do
        local old, new
        if is_commit then
            old, new = t1, t2
        else
            old, new = t2, t1
        end
        assert(space_id == bucket_space_id)
        if new ~= nil then
            bucket_commit_update(new)
        elseif old ~= nil then
            bucket_commit_delete(old)
        end
    end
    bucket_generation_increment()
end

local function bucket_on_commit_f(row_pairs)
    bucket_on_commit_or_rollback_f(row_pairs, true)
end

local function bucket_on_rollback_f(row_pairs)
    bucket_on_commit_or_rollback_f(row_pairs, false)
end

local function bucket_on_replace_f(old_bucket, new_bucket)
    if new_bucket == nil then
        assert(old_bucket ~= nil)
        bucket_prepare_delete(old_bucket)
    else
        bucket_prepare_update(old_bucket, new_bucket)
    end
    local f_commit = bucket_on_commit_f
    -- One transaction can affect many buckets. Do not create a trigger for each
    -- bucket. Instead create it only first time. For that the trigger replaces
    -- itself when installed not first time.
    -- Although in case of hot reload during changing _bucket the function
    -- pointer will change and more than one trigger could be installed.
    -- Shouldn't matter anyway.
    if not pcall(box.on_commit, f_commit, f_commit) then
        box.on_commit(f_commit)
        box.on_rollback(bucket_on_rollback_f)
    end
end

--
-- Make _bucket-related data up to date with the committed changes.
--
local function bucket_on_truncate_commit_f(row_pairs)
    local bucket_space_id = box.space._bucket.id
    local truncate_space_id = box.space._truncate.id
    for _, _, new, space_id in row_pairs() do
        if space_id == truncate_space_id and new ~= nil and
           new[1] == bucket_space_id then
            M.bucket_refs = {}
            bucket_generation_increment()
        end
    end
end

--
-- Validate truncation of _bucket. It works not via _bucket triggers, have to
-- attach to _truncate space.
--
local function bucket_on_truncate_f(_, new_tuple)
    if new_tuple == nil or new_tuple[1] ~= box.space._bucket.id then
        return
    end
    -- There is no a valid truncation of _bucket. Because it would require to
    -- walk all the ref objects and check that they are all empty. There might
    -- be millions. Yielding during iteration isn't possible - it would break
    -- truncation without MVCC. Easier to just ban the truncation.
    if M.is_bucket_protected then
        box.error(box.error.UNSUPPORTED, 'vshard', '_bucket truncation')
        return
    end
    -- Same hack as with _bucket commit trigger.
    local f = bucket_on_truncate_commit_f
    if not pcall(box.on_commit, f, f) then
        box.on_commit(f)
    end
end

local function bucket_generation_wait(timeout)
    return fiber_cond_wait(M.bucket_generation_cond, timeout)
end

--
-- Check if this replicaset is locked. It means be invisible for
-- the rebalancer.
--
local function is_this_replicaset_locked()
    return M.this_replicaset and M.this_replicaset.lock
end

--
-- Check if @a bucket can accept 'write' requests. Writable
-- buckets can accept 'read' too.
--
local function bucket_is_writable(bucket)
    return bucket.status == consts.BUCKET.ACTIVE or
           bucket.status == consts.BUCKET.PINNED
end

--
-- Check if @a bucket can accept 'read' requests.
--
local function bucket_is_readable(bucket)
    return bucket_is_writable(bucket) or bucket.status == consts.BUCKET.SENDING
end

--
-- Check if a bucket is sending or receiving.
--
local function bucket_is_transfer_in_progress(bucket)
    return bucket.status == consts.BUCKET.SENDING or
           bucket.status == consts.BUCKET.RECEIVING
end

--
-- Check that a bucket with the specified id has the needed
-- status.
-- @param bucket_generation Generation since the last check.
-- @param bucket_id Id of the bucket to check.
-- @param status Expected bucket status.
-- @retval New bucket generation.
--
local function bucket_guard_xc(bucket_generation, bucket_id, status)
    if bucket_generation ~= M.bucket_generation then
        local _bucket = box.space._bucket
        local ok, b = pcall(_bucket.get, _bucket, {bucket_id})
        if not ok or (not b and status) or (b and b.status ~= status) then
            local msg =
                string.format("bucket status is changed, was %s, became %s",
                              status, b and b.status or 'dropped')
            error(lerror.vshard(lerror.code.WRONG_BUCKET, bucket_id, msg, nil))
        end
        return M.bucket_generation
    end
    return bucket_generation
end

--
-- Add a positive or a negative value to the receiving buckets
-- quota. Typically this is what bucket_recv() and recovery do.
-- @return Whether the value was added successfully and the quota
--        is changed.
local function bucket_receiving_quota_add(count)
    local new_quota = M.rebalancer_receiving_quota + count
    if new_quota >= 0 then
        local max_quota = M.current_cfg.rebalancer_max_receiving
        if new_quota <= max_quota then
            M.rebalancer_receiving_quota = new_quota
        else
            M.rebalancer_receiving_quota = max_quota
        end
        return true
    end
    return false
end

--
-- Reset the receiving buckets quota. It is used by recovery, when
-- it is discovered, that no receiving buckets are left. In case
-- there was an error which somehow prevented quota return, and a
-- part of the quota was lost.
--
local function bucket_receiving_quota_reset()
    M.rebalancer_receiving_quota = M.current_cfg.rebalancer_max_receiving
end

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------

local function schema_install_triggers()
    local _bucket = box.space._bucket
    if M.bucket_on_replace then
        local ok, err = pcall(_bucket.on_replace, _bucket, nil,
                              M.bucket_on_replace)
        if not ok then
            log.warn('Could not drop old trigger from '..
                     '_bucket: %s', err)
        end
    end
    _bucket:on_replace(bucket_on_replace_f)
    M.bucket_on_replace = bucket_on_replace_f

    local _truncate = box.space._truncate
    if M.bucket_on_truncate then
        local ok, err = pcall(_truncate.on_replace, _truncate, nil,
            M.bucket_on_truncate)
        if not ok then
            log.warn('Could not drop old trigger from '..
                     '_truncate: %s', err)
        end
    end
    _truncate:on_replace(bucket_on_truncate_f)
    M.bucket_on_truncate = bucket_on_truncate_f
end

local function schema_install_on_replace(_, new)
    -- Wait not just for _bucket to appear, but for the entire
    -- schema. This might be important if the schema will ever
    -- consist of more than just _bucket.
    if new == nil or new[1] ~= 'vshard_version' then
        return
    end
    schema_install_triggers()

    local _schema = box.space._schema
    local ok, err = pcall(_schema.on_replace, _schema, nil, M.schema_on_replace)
    if not ok then
        log.warn('Could not drop trigger from _schema inside of the '..
                 'trigger: %s', err)
    end
    M.schema_on_replace = nil
    -- Drop the caches which might have been created while the
    -- schema was being replicated.
    bucket_generation_increment()
end

--
-- Install the triggers later when there is an actual schema to install them on.
-- On replicas it might happen that they are vshard-configured earlier than the
-- master and therefore don't have the schema right away.
--
local function schema_install_triggers_delayed()
    log.info('Could not find _bucket space to install triggers - delayed '..
             'until the schema is replicated')
    assert(not box.space._bucket)
    local _schema = box.space._schema
    if M.schema_on_replace then
        local ok, err = pcall(_schema.on_replace, _schema, nil,
                              M.schema_on_replace)
        if not ok then
            log.warn('Could not drop trigger from _schema: %s', err)
        end
    end
    _schema:on_replace(schema_install_on_replace)
    M.schema_on_replace = schema_install_on_replace
end

local function schema_upgrade_replica()
    local version = lschema.current_version()
    -- Replica can't do upgrade - it is read-only. And it
    -- shouldn't anyway - that would conflict with master doing
    -- the same. So the upgrade is either non-critical, and the
    -- replica can work with the new code but old schema. Or it
    -- it is critical, and need to wait the schema upgrade from
    -- the master.
    -- Or it may happen, that the upgrade just is not possible.
    -- For example, when an auto-upgrade tries to change a too old
    -- schema to the newest, skipping some intermediate versions.
    -- For example, from 1.2.3.4 to 1.7.8.9, when it is assumed
    -- that a safe upgrade should go 1.2.3.4 -> 1.2.4.1 ->
    -- 1.3.1.1 and so on step by step.
    if version ~= lschema.latest_version then
        log.info('Replica\' vshard schema version is not latest - current '..
                 '%s vs latest %s, but the replica still can work', version,
                 lschema.latest_version)
    end
    -- In future for hard changes the replica may be suspended
    -- until its schema is synced with master. Or it may
    -- reject to upgrade in case of incompatible changes. Now
    -- there are too few versions so as such problems could
    -- appear.
end

local function this_is_master()
    return M.is_master
end

local function check_is_master()
    if this_is_master() then
        return true, nil
    end
    local master_id = M.this_replicaset.master
    if master_id then
        master_id = master_id.id
    end
    return nil, lerror.vshard(lerror.code.NON_MASTER, M.this_replica.id,
                              M.this_replicaset.id, master_id)
end

local function on_master_disable(new_func, old_func)
    local func = M._on_master_disable(new_func, old_func)
    -- If a trigger is set after storage.cfg(), then notify an
    -- user, that the current instance is not master.
    if old_func == nil and not this_is_master() then
        M._on_master_disable:run()
    end
    return func
end

local function on_master_enable(new_func, old_func)
    local func = M._on_master_enable(new_func, old_func)
    -- Same as above, but notify, that the instance is master.
    if old_func == nil and this_is_master() then
        M._on_master_enable:run()
    end
    return func
end

local function on_bucket_event(new_func, old_func)
    return M._on_bucket_event(new_func, old_func)
end

--------------------------------------------------------------------------------
-- Recovery
--------------------------------------------------------------------------------

--
-- Check if a rebalancing is in progress. It is true, if the node
-- applies routes received from a rebalancer node in the special
-- fiber.
--
local function rebalancing_is_in_progress()
    local f = M.rebalancer_applier_fiber
    return f ~= nil and f:status() ~= 'dead'
end

local function recovery_bucket_stat(bid)
    local ok, err = check_is_master()
    if not ok then
        return nil, err
    end
    local b = box.space._bucket:get{bid}
    if b == nil then
        return
    end
    return {
        id = bid,
        status = b.status,
        is_transfering = M.rebalancer_transfering_buckets[bid],
    }
end

--
-- Check if a local bucket which is SENDING has actually finished the transfer
-- and can be made SENT.
--
local function recovery_local_bucket_is_sent(local_bucket, remote_bucket)
    if local_bucket.status ~= consts.BUCKET.SENDING then
        return false
    end
    if not remote_bucket then
        return false
    end
    return bucket_is_writable(remote_bucket)
end

--
-- Check if a local bucket is GARBAGE. Can be true only if it is RECEIVING and
-- apparently wasn't fully received before the transfer failed. Other buckets
-- can't turn to GARBAGE straight away because might have at least RO refs.
--
local function recovery_local_bucket_is_garbage(local_bucket, remote_bucket)
    if local_bucket.status ~= consts.BUCKET.RECEIVING then
        return false
    end
    if not remote_bucket then
        return false
    end
    if bucket_is_writable(remote_bucket) then
        return true
    end
    if remote_bucket.status == consts.BUCKET.SENDING then
        assert(not remote_bucket.is_transfering)
        assert(not M.rebalancer_transfering_buckets[local_bucket.id])
        return true
    end
    return false
end

--
-- Check if a local bucket can become active.
--
local function recovery_local_bucket_is_active(local_bucket, remote_bucket)
    if M.rebalancer_transfering_buckets[local_bucket.id] then
        return false
    end
    if not remote_bucket then
        return true
    end
    local status = remote_bucket.status
    return status == consts.BUCKET.SENT or status == consts.BUCKET.GARBAGE
end

--
-- Check status of each transferring bucket. Resolve status where
-- possible.
--
local function recovery_step_by_type(type)
    local _bucket = box.space._bucket
    local is_step_empty = true
    local recovered = 0
    local total = 0
    local start_format = 'Starting %s buckets recovery step'
    for _, bucket in _bucket.index.status:pairs(type) do
        lfiber.testcancel()
        total = total + 1
        local bucket_id = bucket.id
        if M.rebalancer_transfering_buckets[bucket_id] then
            goto continue
        end
        assert(bucket_is_transfer_in_progress(bucket))
        local peer_id = bucket.destination
        local destination = M.replicasets[peer_id]
        if not destination then
            -- No replicaset master for a bucket. Wait until it
            -- appears.
            if is_step_empty then
                log.info(start_format, type)
                log.warn('Can not find for bucket %s its peer %s', bucket_id,
                         peer_id)
                is_step_empty = false
            end
            goto continue
        end
        lfiber.testcancel()
        local remote_bucket, err = master_call(
            destination, 'vshard.storage._call',
            {'recovery_bucket_stat', bucket_id},
            {timeout = consts.RECOVERY_GET_STAT_TIMEOUT})
        -- Check if it is not a bucket error, and this result can
        -- not be used to recovery anything. Try later.
        if remote_bucket == nil and err ~= nil then
            if is_step_empty then
                if err == nil then
                    err = 'unknown'
                end
                log.info(start_format, type)
                log.error('Error during recovery of bucket %s on replicaset '..
                          '%s: %s', bucket_id, peer_id, err)
                is_step_empty = false
            end
            goto continue
        end
        -- Do nothing until the bucket on both sides stopped
        -- transferring.
        if remote_bucket and remote_bucket.is_transfering then
            goto continue
        end
        -- It is possible that during lookup a new request arrived
        -- which finished the transfer.
        bucket = _bucket:get{bucket_id}
        if not bucket or not bucket_is_transfer_in_progress(bucket) then
            goto continue
        end
        if is_step_empty then
            log.info(start_format, type)
        end
        lfiber.testcancel()
        if recovery_local_bucket_is_sent(bucket, remote_bucket) then
            _bucket:update({bucket_id}, {{'=', 2, consts.BUCKET.SENT}})
            recovered = recovered + 1
        elseif recovery_local_bucket_is_garbage(bucket, remote_bucket) then
            _bucket:update({bucket_id}, {{'=', 2, consts.BUCKET.GARBAGE}})
            recovered = recovered + 1
        elseif recovery_local_bucket_is_active(bucket, remote_bucket) then
            _bucket:replace({bucket_id, consts.BUCKET.ACTIVE})
            recovered = recovered + 1
        elseif is_step_empty then
            log.info('Bucket %s is %s local and %s on replicaset %s, waiting',
                     bucket_id, bucket.status, remote_bucket.status, peer_id)
        end
        is_step_empty = false
::continue::
    end
    if not is_step_empty then
        log.info('Finish bucket recovery step, %d %s buckets are recovered '..
                 'among %d', recovered, type, total)
    end
    return total, recovered
end

--
-- Infinite function to resolve status of buckets, whose 'sending'
-- has failed due to tarantool or network problems. Restarts on
-- reload.
--
local function recovery_service_f(service)
    local module_version = M.module_version
    -- Change of _bucket increments bucket generation. Recovery has its own
    -- bucket generation which is <= actual. Recovery is finished, when its
    -- generation == bucket generation. In such a case the fiber does nothing
    -- until next _bucket change.
    local bucket_generation_recovered = -1
    local bucket_generation_current = M.bucket_generation
    local ok, sleep_time, is_all_recovered, total, recovered
    -- Interrupt recovery if a module has been reloaded. Perhaps,
    -- there was found a bug, and reload fixes it.
    while module_version == M.module_version do
        service:next_iter()
        if M.errinj.ERRINJ_RECOVERY_PAUSE then
            M.errinj.ERRINJ_RECOVERY_PAUSE = 1
            lfiber.testcancel()
            lfiber.sleep(0.01)
            goto continue
        end
        is_all_recovered = true
        if bucket_generation_recovered == bucket_generation_current then
            goto sleep
        end

        service:set_activity('recovering sending')
        lfiber.testcancel()
        ok, total, recovered = pcall(recovery_step_by_type,
                                     consts.BUCKET.SENDING)
        if not ok then
            is_all_recovered = false
            log.error(service:set_status_error(
                'Error during sending buckets recovery: %s', total))
        elseif total ~= recovered then
            is_all_recovered = false
        end

        service:set_activity('recovering receiving')
        lfiber.testcancel()
        ok, total, recovered = pcall(recovery_step_by_type,
                                     consts.BUCKET.RECEIVING)
        if not ok then
            is_all_recovered = false
            log.error(service:set_status_error(
                'Error during receiving buckets recovery: %s', total))
        elseif total == 0 then
            bucket_receiving_quota_reset()
        else
            bucket_receiving_quota_add(recovered)
            if total ~= recovered then
                is_all_recovered = false
            end
        end

    ::sleep::
        if not is_all_recovered then
            service:set_status('recovering')
            bucket_generation_recovered = -1
        else
            service:set_status_ok()
            bucket_generation_recovered = bucket_generation_current
        end
        bucket_generation_current = M.bucket_generation

        local sleep_activity = 'idling'
        if not is_all_recovered then
            -- One option - some buckets are not broken. Their transmission is
            -- still in progress. Don't need to retry immediately. Another
            -- option - network errors when tried to repair the buckets. Also no
            -- need to retry often. It won't help.
            sleep_time = consts.RECOVERY_BACKOFF_INTERVAL
            sleep_activity = 'backoff'
        elseif bucket_generation_recovered ~= bucket_generation_current then
            sleep_time = 0
        else
            sleep_time = consts.TIMEOUT_INFINITY
        end
        if module_version == M.module_version then
            service:set_activity(sleep_activity)
            lfiber.testcancel()
            bucket_generation_wait(sleep_time)
        end
    ::continue::
    end
end

local function recovery_f()
    assert(not M.recovery_service)
    local service = lservice_info.new('recovery')
    M.recovery_service = service
    local ok, err = pcall(recovery_service_f, service)
    assert(M.recovery_service == service)
    M.recovery_service = nil
    if not ok then
        error(err)
    end
end

--
-- Immediately wakeup recovery fiber, if exists.
--
local function recovery_wakeup()
    if M.recovery_fiber then
        M.recovery_fiber:wakeup()
    end
end

--------------------------------------------------------------------------------
-- Replicaset
--------------------------------------------------------------------------------

-- Vclock comparing function
local function vclock_lesseq(vc1, vc2)
    local lesseq = true
    for i, lsn in ipairs(vc1) do
        lesseq = lesseq and lsn <= (vc2[i] or 0)
        if not lesseq then
            break
        end
    end
    return lesseq
end

local function wait_lsn(timeout, interval)
    local info = box.info
    local current_id = info.id
    local vclock = info.vclock
    local deadline = fiber_clock() + timeout
    repeat
        local done = true
        for _, replica in ipairs(box.info.replication) do
            -- We should not check the current instance as there's
            -- no downstream. Moreover, it's not guaranteed that the
            -- first replica is the same as the current one, so ids
            -- have to be compared.
            if replica.id == current_id then
                goto continue
            end
            local down = replica.downstream
            if not down or (down.status == 'stopped' or
                            not vclock_lesseq(vclock, down.vclock)) then
                done = false
                break
            end
            ::continue::
        end
        if done then
            return true
        end
        lfiber.sleep(interval)
    until fiber_clock() > deadline
    return nil, lerror.timeout()
end

local function sync(timeout)
    if timeout ~= nil and type(timeout) ~= 'number' then
        error('Usage: vshard.storage.sync([timeout: number])')
    end
    return wait_lsn(timeout or M.sync_timeout, 0.001)
end

--------------------------------------------------------------------------------
-- Buckets
--------------------------------------------------------------------------------

--
-- Check that an action of a specified mode can be applied to a
-- bucket.
-- @param bucket_id Bucket identifier.
-- @param mode 'Read' or 'write' mode.
--
-- @retval bucket Bucket that can accept an action of a specified
--         mode.
-- @retval bucket and error object Bucket that can not accept the
--         action, and a reason why.
--
local function bucket_check_state(bucket_id, mode)
    assert(type(bucket_id) == 'number')
    assert(mode == 'read' or mode == 'write')
    local bucket = box.space._bucket:get({bucket_id})
    local reason
    if not bucket then
        reason = 'Not found'
    elseif mode == 'read' then
        if bucket_is_readable(bucket) then
            return bucket
        end
        reason = 'read is prohibited'
    elseif not bucket_is_writable(bucket) then
        if bucket_is_transfer_in_progress(bucket) then
            return bucket, lerror.vshard(lerror.code.TRANSFER_IS_IN_PROGRESS,
                                         bucket_id, bucket.destination)
        end
        reason = 'write is prohibited'
    else
        local _, err = check_is_master()
        return bucket, err
    end
    local dst = bucket and bucket.destination or M.route_map[bucket_id]
    return bucket, lerror.vshard(lerror.code.WRONG_BUCKET, bucket_id, reason,
                                 dst)
end

--
-- Take Read-Only reference on the bucket identified by
-- @a bucket_id. Under such reference a bucket can not be deleted
-- from the storage. Its transfer still can start, but can not
-- finish until ref == 0.
-- @param bucket_id Identifier of a bucket to ref.
--
-- @retval true The bucket is referenced ok.
-- @retval nil, error Can not ref the bucket. An error object is
--         returned via the second value.
--
local function bucket_refro(bucket_id)
    local ref = M.bucket_refs[bucket_id]
    if not ref then
        local _, err = bucket_check_state(bucket_id, 'read')
        if err then
            return nil, err
        end
        ref = bucket_ref_new()
        ref.ro = 1
        M.bucket_refs[bucket_id] = ref
    elseif ref.ro_lock then
        return nil, lerror.vshard(lerror.code.BUCKET_IS_LOCKED, bucket_id)
    elseif ref.ro == 0 and ref.rw == 0 then
    -- RW is more strict ref than RO so rw != 0 is sufficient to
    -- take an RO ref.
        local _, err = bucket_check_state(bucket_id, 'read')
        if err then
            return nil, err
        end
        ref.ro = 1
    else
        ref.ro = ref.ro + 1
    end
    return true
end

--
-- Remove one RO reference.
--
local function bucket_unrefro(bucket_id)
    local ref = M.bucket_refs[bucket_id]
    local count = ref and ref.ro or 0
    if count == 0 then
        return nil, lerror.vshard(lerror.code.BUCKET_IS_CORRUPTED, bucket_id,
                                  "no ro refs on unref")
    end
    if count == 1 then
        ref.ro = 0
        if ref.ro_lock then
            -- Garbage collector is waiting for the bucket if RO
            -- is locked. Let it know it has one more bucket to
            -- collect. It relies on generation, so its increment
            -- is enough.
            bucket_generation_increment()
        end
        return true
    end
    ref.ro = count - 1
    return true
end

--
-- Same as bucket_refro, but more strict - the bucket transfer
-- can not start until a bucket has such refs. And if the bucket
-- is already scheduled for transfer then it can not take new RW
-- refs. The rebalancer waits until all RW refs gone and starts
-- transfer.
--
local function bucket_refrw(bucket_id)
    local ref = M.bucket_refs[bucket_id]
    if not ref then
        local _, err = bucket_check_state(bucket_id, 'write')
        if err then
            return nil, err
        end
        ref = bucket_ref_new()
        ref.rw = 1
        M.bucket_refs[bucket_id] = ref
    elseif ref.rw_lock then
        return nil, lerror.vshard(lerror.code.BUCKET_IS_LOCKED, bucket_id)
    elseif ref.rw == 0 then
        local _, err = bucket_check_state(bucket_id, 'write')
        if err then
            return nil, err
        end
        ref.rw = 1
    else
        ref.rw = ref.rw + 1
    end
    return true
end

--
-- Remove one RW reference.
--
local function bucket_unrefrw(bucket_id)
    local ref = M.bucket_refs[bucket_id]
    if not ref or ref.rw == 0 then
        return nil, lerror.vshard(lerror.code.BUCKET_IS_CORRUPTED, bucket_id,
                                  "no rw refs on unref")
    end
    if ref.rw == 1 and ref.rw_lock then
        ref.rw = 0
        M.bucket_rw_lock_is_ready_cond:broadcast()
    else
        ref.rw = ref.rw - 1
    end
    return true
end

--
-- Ensure that a bucket ref exists and can be referenced for an RW
-- request.
--
local function bucket_refrw_touch(bucket_id)
    local status, err = bucket_refrw(bucket_id)
    if not status then
        return nil, err
    end
    bucket_unrefrw(bucket_id)
    return M.bucket_refs[bucket_id]
end

--
-- Ref/unref shortcuts for an obscure mode.
--

local function bucket_ref(bucket_id, mode)
    if mode == 'read' then
        return bucket_refro(bucket_id)
    elseif mode == 'write' then
        return bucket_refrw(bucket_id)
    else
        error('Unknown mode')
    end
end

local function bucket_unref(bucket_id, mode)
    if mode == 'read' then
        return bucket_unrefro(bucket_id)
    elseif mode == 'write' then
        return bucket_unrefrw(bucket_id)
    else
        error('Unknown mode')
    end
end

--
-- Return information about bucket
--
local function bucket_stat(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_stat(bucket_id)')
    end
    local stat, err = bucket_check_state(bucket_id, 'read')
    if stat then
        stat = stat:tomap()
        if M.rebalancer_transfering_buckets[bucket_id] then
            stat.is_transfering = true
        end
    end
    return stat, err
end

--
-- Create bucket range manually for initial bootstrap, tests or
-- emergency cases. Buckets id, id + 1, id + 2, ..., id + count
-- are inserted.
-- @param first_bucket_id Identifier of a first bucket in a range.
-- @param count Bucket range length to insert. By default is 1.
--
local function bucket_force_create_impl(first_bucket_id, count)
    local _bucket = box.space._bucket
    box.begin()
    local limit = consts.BUCKET_CHUNK_SIZE
    for i = first_bucket_id, first_bucket_id + count - 1 do
        -- Buckets are protected with on_replace trigger, which
        -- prohibits the creation of the ACTIVE buckets from nowhere,
        -- as this is done only for bootstrap and not during the normal
        -- work of vshard. So, as we don't want to disable the protection
        -- of the buckets for the whole replicaset for bootstrap, a bucket's
        -- status must go the following way: none -> RECEIVING -> ACTIVE.
        _bucket:insert({i, consts.BUCKET.RECEIVING})
        _bucket:replace({i, consts.BUCKET.ACTIVE})
        limit = limit - 1
        if limit == 0 then
            box.commit()
            box.begin()
            limit = consts.BUCKET_CHUNK_SIZE
        end
    end
    box.commit()
end

local function bucket_force_create(first_bucket_id, count)
    if type(first_bucket_id) ~= 'number' or
       (count ~= nil and (type(count) ~= 'number' or
                          math.floor(count) ~= count)) then
        error('Usage: bucket_force_create(first_bucket_id, count)')
    end
    count = count or 1
    local ok, err = pcall(bucket_force_create_impl, first_bucket_id, count)
    if not ok then
        box.rollback()
        return nil, err
    end
    return true
end

--
-- Drop bucket manually for tests or emergency cases
--
local function bucket_force_drop(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_force_drop(bucket_id)')
    end

    box.space._bucket:delete({bucket_id})
    return true
end


--
-- Receive bucket data. If the bucket is not presented here, it is
-- created as RECEIVING.
-- @param bucket_id Bucket to receive.
-- @param from Source ID (UUID or name).
-- @param data Bucket data in the format:
--        [{space_name, [space_tuples]}, ...].
-- @param opts Options. Now the only possible option is 'is_last'.
--        It is set to true when the data portion is last and the
--        bucket can be activated here.
--
-- @retval nil, error Error occurred.
-- @retval true The data is received ok.
--
local function bucket_recv_xc(bucket_id, from, data, opts)
    if not from or not M.replicasets[from] then
        return nil, lerror.vshard(lerror.code.NO_SUCH_REPLICASET, from)
    end
    local _bucket = box.space._bucket
    local b = _bucket:get{bucket_id}
    local recvg = consts.BUCKET.RECEIVING
    local is_last = opts and opts.is_last
    if not b then
        if is_last then
            local msg = 'last message is received, but the bucket does not '..
                        'exist anymore'
            return nil, lerror.vshard(lerror.code.WRONG_BUCKET, bucket_id, msg,
                                      from)
        end
        if is_this_replicaset_locked() then
            return nil, lerror.vshard(lerror.code.REPLICASET_IS_LOCKED)
        end
        if not bucket_receiving_quota_add(-1) then
            return nil, lerror.vshard(lerror.code.TOO_MANY_RECEIVING)
        end
        local timeout = opts and opts.timeout or
                        consts.DEFAULT_BUCKET_SEND_TIMEOUT
        local ok, err = lsched.move_start(timeout)
        if not ok then
            return nil, err
        end
        assert(lref.count == 0)
        -- Move schedule is done only for the time of _bucket update.
        -- The reason is that one bucket_send() calls bucket_recv() on the
        -- remote storage multiple times. If the latter would schedule new moves
        -- on each call, it could happen that the scheduler would block it in
        -- favor of refs right in the middle of bucket_send().
        -- It would lead to a deadlock, because refs won't be able to start -
        -- the bucket won't be writable.
        -- This way still provides fair scheduling, but does not have the
        -- described issue.
        ok, err = pcall(_bucket.insert, _bucket, {bucket_id, recvg, from})
        lsched.move_end(1)
        if not ok then
            return nil, lerror.make(err)
        end
    elseif b.status ~= recvg then
        local msg = string.format("bucket state is changed: was receiving, "..
                                  "became %s", b.status)
        return nil, lerror.vshard(lerror.code.WRONG_BUCKET, bucket_id, msg,
                                  from)
    end
    local bucket_generation = M.bucket_generation
    local limit = consts.BUCKET_CHUNK_SIZE
    local event_type = consts.BUCKET_EVENT.RECV
    for _, row in ipairs(data) do
        local space_name, space_data = row[1], row[2]
        local space = box.space[space_name]
        if space == nil then
            local err = box.error.new(box.error.NO_SUCH_SPACE, space_name)
            return nil, lerror.box(err)
        end
        box.begin()
        M._on_bucket_event:run(event_type, bucket_id, {spaces = {space_name}})
        for _, tuple in ipairs(space_data) do
            local ok, err = pcall(space.insert, space, tuple)
            if not ok then
                box.rollback()
                return nil, lerror.vshard(lerror.code.BUCKET_RECV_DATA_ERROR,
                                          bucket_id, space.name,
                                          box.tuple.new(tuple), err)
            end
            limit = limit - 1
            if limit == 0 then
                box.commit()
                if M.errinj.ERRINJ_RECEIVE_PARTIALLY then
                    box.error(box.error.INJECTION,
                              "the bucket is received partially")
                end
                bucket_generation =
                    bucket_guard_xc(bucket_generation, bucket_id, recvg)
                box.begin()
                M._on_bucket_event:run(event_type, bucket_id,
                                       {spaces = {space_name}})
                limit = consts.BUCKET_CHUNK_SIZE
            end
        end
        box.commit()
        if M.errinj.ERRINJ_RECEIVE_PARTIALLY then
            box.error(box.error.INJECTION, "the bucket is received partially")
        end
        bucket_generation = bucket_guard_xc(bucket_generation, bucket_id, recvg)
    end
    if is_last then
        _bucket:replace({bucket_id, consts.BUCKET.ACTIVE})
        bucket_receiving_quota_add(1)
        if M.errinj.ERRINJ_LONG_RECEIVE then
            box.error(box.error.TIMEOUT)
        end
    end
    return true
end

--
-- Extract bucket_recv() args from an msgpack object. Only the tuples remain as
-- msgpack objects to preserve their original content which could otherwise be
-- altered by Lua.
--
local function bucket_recv_parse_raw_args(raw)
    local it = raw:iterator()
    local arg_count = it:decode_array_header()
    local bucket_id = it:decode()
    local from = it:decode()
    local data_size = it:decode_array_header()
    local data = table.new(data_size, 0)
    for i = 1, data_size do
        it:decode_array_header()
        local space_name = it:decode()
        local space_size = it:decode_array_header()
        local space_data = table.new(space_size, 0)
        for j = 1, space_size do
            space_data[j] = it:take()
        end
        data[i] = {space_name, space_data}
    end
    local opts = arg_count >= 4 and it:decode() or nil
    return bucket_id, from, data, opts
end

--
-- Exception safe version of bucket_recv_xc.
--
local function bucket_recv(bucket_id, from, data, opts)
    local status, ret, err
    ret, err = check_is_master()
    if not ret then
        return nil, err
    end
    -- Can understand both msgpack and normal args. That is done because having
    -- a separate bucket_recv_raw() function would mean the new vshard storages
    -- wouldn't be able to send buckets to the old ones which have only
    -- bucket_recv(). Or it would require some work on bucket_send() to try
    -- bucket_recv_raw() and fallback to bucket_recv().
    if util.feature.msgpack_object and lmsgpack.is_object(bucket_id) then
        bucket_id, from, data, opts = bucket_recv_parse_raw_args(bucket_id)
    end
    while opts and opts.is_last and M.errinj.ERRINJ_LAST_RECEIVE_DELAY do
        lfiber.sleep(0.01)
    end
    status, err = bucket_transfer_start(bucket_id)
    if not status then
        return nil, err
    end
    status, ret, err = pcall(bucket_recv_xc, bucket_id, from, data, opts)
    bucket_transfer_end(bucket_id)
    if status then
        if ret then
            return ret
        end
    else
        err = ret
    end
    box.rollback()
    return nil, err
end

--
-- Test which of the passed bucket IDs can be safely garbage collected.
--
local function bucket_test_gc(bids)
    local refs = M.bucket_refs
    local bids_not_ok = table.new(consts.BUCKET_CHUNK_SIZE, 0)
    -- +1 because the expected max count is exactly the chunk size. No need to
    -- yield on the last one. But if somewhy more is received, then do the
    -- yields.
    local limit = consts.BUCKET_CHUNK_SIZE + 1
    local _bucket = box.space._bucket
    local ref, bucket, status
    for i, bid in ipairs(bids) do
        if M.rebalancer_transfering_buckets[bid] then
            goto not_ok_bid
        end
        bucket = _bucket:get(bid)
        status = bucket ~= nil and bucket.status or consts.BUCKET.GARBAGE
        if status ~= consts.BUCKET.GARBAGE and status ~= consts.BUCKET.SENT then
            goto not_ok_bid
        end
        ref = refs[bid]
        if ref == nil then
            goto next_bid
        end
        if ref.ro ~= 0 then
            goto not_ok_bid
        end
        assert(ref.rw == 0)
        assert(ref.ro_lock)
        goto next_bid
    ::not_ok_bid::
        table.insert(bids_not_ok, bid)
    ::next_bid::
        if i % limit == 0 then
            fiber_yield()
        end
    end
    return {bids_not_ok = bids_not_ok}
end

--
-- Public wrapper for sharded spaces list getter.
--
local function storage_sharded_spaces()
    return table.deepcopy(lschema.find_sharded_spaces())
end

if M.errinj.ERRINJ_RELOAD then
    error('Error injection: reload')
end

--
-- Collect content of the readable bucket.
--
local function bucket_collect(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_collect(bucket_id)')
    end
    local _, err = bucket_check_state(bucket_id, 'read')
    if err then
        return nil, err
    end
    local data = {}
    local spaces = lschema.find_sharded_spaces()
    local idx = lschema.shard_index
    for _, space in pairs(spaces) do
        assert(space.index[idx] ~= nil)
        local space_data = space.index[idx]:select({bucket_id})
        table.insert(data, {space.name, space_data})
    end
    return data
end

-- Discovery used by routers. It returns limited number of
-- buckets to avoid stalls when _bucket is huge.
local function buckets_discovery_extended(opts)
    local limit = consts.BUCKET_CHUNK_SIZE
    local buckets = table.new(limit, 0)
    local active = consts.BUCKET.ACTIVE
    local pinned = consts.BUCKET.PINNED
    local next_from
    local errcnt = M.errinj.ERRINJ_DISCOVERY
    if errcnt then
        if errcnt > 0 then
            M.errinj.ERRINJ_DISCOVERY = errcnt - 1
            if errcnt % 2 == 0 then
                box.error(box.error.INJECTION, 'discovery')
            end
        else
            M.errinj.ERRINJ_DISCOVERY = false
        end
    end
    -- No way to select by {status, id}, because there are two
    -- statuses to select. A router would need to maintain a
    -- separate iterator for each status it wants to get. This may
    -- be implemented in future. But _bucket space anyway 99% of
    -- time contains only active and pinned buckets. So there is
    -- no big benefit in optimizing that. Perhaps a compound index
    -- {status, id} could help too.
    for _, bucket in box.space._bucket:pairs({opts.from},
                                             {iterator = box.index.GE}) do
        local status = bucket.status
        if status == active or status == pinned then
            table.insert(buckets, bucket.id)
        end
        limit = limit - 1
        if limit == 0 then
            next_from = bucket.id + 1
            break
        end
    end
    -- Buckets list can even be empty, if all buckets in the
    -- scanned chunk are not active/pinned. But next_from still
    -- should be returned. So as the router could request more.
    return {buckets = buckets, next_from = next_from}
end

--
-- Collect array of active bucket identifiers for discovery.
--
local function buckets_discovery(opts)
    if opts then
        -- Private method. Is not documented intentionally.
        return buckets_discovery_extended(opts)
    end
    local ret = {}
    local status = box.space._bucket.index.status
    for _, bucket in status:pairs({consts.BUCKET.ACTIVE}) do
        table.insert(ret, bucket.id)
    end
    for _, bucket in status:pairs({consts.BUCKET.PINNED}) do
        table.insert(ret, bucket.id)
    end
    return ret
end

--
-- Send a bucket to other replicaset.
--
local function bucket_send_xc(bucket_id, destination, opts, exception_guard)
    local id = M.this_replicaset.id
    local status, ok
    local ref, err = bucket_refrw_touch(bucket_id)
    if not ref then
        return nil, err
    end
    ref.rw_lock = true
    exception_guard.ref = ref
    exception_guard.drop_rw_lock = true
    if not opts or not opts.timeout then
        opts = opts and table.copy(opts) or {}
        opts.timeout = consts.DEFAULT_BUCKET_SEND_TIMEOUT
    end
    local timeout = opts.timeout
    local deadline = fiber_clock() + timeout
    while ref.rw ~= 0 do
        timeout = deadline - fiber_clock()
        ok, err = fiber_cond_wait(M.bucket_rw_lock_is_ready_cond, timeout)
        if not ok then
            return nil, err
        end
        lfiber.testcancel()
    end

    local _bucket = box.space._bucket
    local bucket = _bucket:get({bucket_id})
    if is_this_replicaset_locked() then
        return nil, lerror.vshard(lerror.code.REPLICASET_IS_LOCKED)
    end
    if bucket.status == consts.BUCKET.PINNED then
        return nil, lerror.vshard(lerror.code.BUCKET_IS_PINNED, bucket_id)
    end
    local replicaset = M.replicasets[destination]
    if replicaset == nil then
        return nil, lerror.vshard(lerror.code.NO_SUCH_REPLICASET, destination)
    end
    if destination == id then
        return nil, lerror.vshard(lerror.code.MOVE_TO_SELF, bucket_id, id)
    end
    local data = {}
    local spaces = lschema.find_sharded_spaces()
    local limit = consts.BUCKET_CHUNK_SIZE
    local idx = lschema.shard_index
    local bucket_generation = M.bucket_generation
    local sendg = consts.BUCKET.SENDING

    ok, err = lsched.move_start(timeout)
    if not ok then
        return nil, err
    end
    assert(lref.count == 0)
    -- Move is scheduled only for the time of _bucket update because:
    --
    -- * it is consistent with bucket_recv() (see its comments);
    --
    -- * gives the same effect as if move was in the scheduler for the whole
    --   bucket_send() time, because refs won't be able to start anyway - the
    --   bucket is not writable.
    ok, err = pcall(_bucket.replace, _bucket, {bucket_id, sendg, destination})
    lsched.move_end(1)
    if not ok then
        return nil, lerror.make(err)
    end

    -- From this moment the bucket is SENDING. Such a status is
    -- even stronger than the lock.
    ref.rw_lock = false
    exception_guard.drop_rw_lock = false
    for _, space in pairs(spaces) do
        local index = space.index[idx]
        local space_data = {}
        for _, t in index:pairs({bucket_id}) do
            table.insert(space_data, t)
            limit = limit - 1
            if limit == 0 then
                table.insert(data, {space.name, space_data})
                status, err = master_call(
                    replicaset, 'vshard.storage.bucket_recv',
                    {bucket_id, id, data}, opts)
                bucket_generation =
                    bucket_guard_xc(bucket_generation, bucket_id, sendg)
                if not status then
                    return status, lerror.make(err)
                end
                limit = consts.BUCKET_CHUNK_SIZE
                data = {}
                space_data = {}
            end
        end
        table.insert(data, {space.name, space_data})
    end
    status, err = master_call(replicaset, 'vshard.storage.bucket_recv',
                              {bucket_id, id, data}, opts)
    if not status then
        return status, lerror.make(err)
    end
    while M.errinj.ERRINJ_LAST_SEND_DELAY do
        lfiber.sleep(0.01)
    end
    -- It is safe to turn bucket to SENT as it has already been fully
    -- sent to the destination and can be garbade collected regardless of
    -- receiving is_last message by the destination, as out there it will be
    -- converted to ACTIVE status by recovery process. Moreover, it's preferable
    -- to turn it into SENT at the source before turning to ACTIVE at the
    -- destination, as it allows to avoid doubled buckets problem caused by
    -- manual vshard.storage.bucket_send() call. The following example
    -- illustrates how it can happen.
    --
    -- Storage S1 has bucket B, which is sent to S2, but then connection breaks.
    -- The bucket is in the state S1 {B: sending-to-s2}, S2 {B: active}. Now if
    -- the user will do vshard.storage.bucket_send(S2 -> S3), then we will get
    -- this: S1 {B: sending-to-s2}, S2: {}, S3: {B: active}. Now when recovery
    -- fiber will wakeup on S1, it will see that B is sending-to-s2 but S2
    -- doesn't have the bucket. Recovery will then assume that S2 already
    -- deleted B, and will recover it on S1. Now we have S1 {B: active} and
    -- S3 {B: active}, doubled buckets situation (gh-414)
    _bucket:replace({bucket_id, consts.BUCKET.SENT, destination})
    -- Always send at least two messages to prevent the case, when
    -- a bucket is sent, hung in the network. Then it is recovered
    -- to active on the source, and then the message arrives and
    -- the same bucket is activated on the destination.
    status, err = master_call(replicaset, 'vshard.storage.bucket_recv',
                              {bucket_id, id, {}, {is_last = true}}, opts)
    if not status then
        return status, lerror.make(err)
    end
    return true
end

--
-- Exception and recovery safe version of bucket_send_xc.
--
local function bucket_send(bucket_id, destination, opts)
    if type(bucket_id) ~= 'number' or type(destination) ~= 'string' then
        error('Usage: bucket_send(bucket_id, destination)')
    end
    local status, ret, err
    ret, err = check_is_master()
    if not ret then
        return nil, err
    end
    status, err = bucket_transfer_start(bucket_id)
    if not status then
        return nil, err
    end
    local exception_guard = {}
    status, ret, err = pcall(bucket_send_xc, bucket_id, destination, opts,
                             exception_guard)
    if exception_guard.drop_rw_lock then
        exception_guard.ref.rw_lock = false
    end
    bucket_transfer_end(bucket_id)
    if status then
        if ret then
            return ret
        end
    else
        err = ret
        ret = status
    end
    return ret, err
end

--
-- Pin a bucket to a replicaset. Pinned bucket can not be sent
-- even if is breaks the cluster balance.
-- @param bucket_id Bucket identifier to pin.
-- @retval true A bucket is pinned.
-- @retval nil, err A bucket can not be pinned. @A err is the
--         reason why.
--
local function bucket_pin(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_pin(bucket_id)')
    end
    local bucket, err = bucket_check_state(bucket_id, 'write')
    if err then
        return nil, err
    end
    assert(bucket)
    if bucket.status ~= consts.BUCKET.PINNED then
        assert(bucket.status == consts.BUCKET.ACTIVE)
        box.space._bucket:update({bucket_id}, {{'=', 2, consts.BUCKET.PINNED}})
    end
    return true
end

--
-- Return a pinned bucket back into active state.
-- @param bucket_id Bucket identifier to unpin.
-- @retval true A bucket is unpinned.
-- @retval nil, err A bucket can not be unpinned. @A err is the
--         reason why.
--
local function bucket_unpin(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_unpin(bucket_id)')
    end
    local bucket, err = bucket_check_state(bucket_id, 'write')
    if err then
        return nil, err
    end
    assert(bucket)
    if bucket.status == consts.BUCKET.PINNED then
        box.space._bucket:update({bucket_id}, {{'=', 2, consts.BUCKET.ACTIVE}})
    else
        assert(bucket.status == consts.BUCKET.ACTIVE)
    end
    return true
end

--------------------------------------------------------------------------------
-- Garbage collector
--------------------------------------------------------------------------------
--
-- Delete from a space tuples with a specified bucket id.
-- @param space Space to cleanup.
-- @param bucket_id Id of the bucket to cleanup.
-- @param status Bucket status for guard checks.
--
local function gc_bucket_in_space_xc(space, bucket_id, status)
    local bucket_index = space.index[lschema.shard_index]
    if #bucket_index:select({bucket_id}, {limit = 1}) == 0 then
        return
    end
    local bucket_generation = M.bucket_generation
    local pk_parts = space.index[0].parts
::restart::
    local limit = consts.BUCKET_CHUNK_SIZE
    box.begin()
    M._on_bucket_event:run(consts.BUCKET_EVENT.GC, bucket_id,
                           {spaces = {space.name}})
    for _, tuple in bucket_index:pairs({bucket_id}) do
        space:delete(util.tuple_extract_key(tuple, pk_parts))
        limit = limit - 1
        if limit == 0 then
            box.commit()
            bucket_generation =
                bucket_guard_xc(bucket_generation, bucket_id, status)
            goto restart
        end
    end
    box.commit()
end

--
-- Exception safe version of gc_bucket_in_space_xc.
--
local function gc_bucket_in_space(space, bucket_id, status)
    local ok, err = pcall(gc_bucket_in_space_xc, space, bucket_id, status)
    if not ok then
        box.rollback()
    end
    return ok, err
end

--
-- Drop buckets with the given status along with their data in all spaces.
-- @param status Status of target buckets.
-- @param route_map Destinations of deleted buckets are saved into this table.
--
local function gc_bucket_drop_xc(status, route_map)
    local limit = consts.BUCKET_CHUNK_SIZE
    local _bucket = box.space._bucket
    local sharded_spaces = lschema.find_sharded_spaces()
    for _, b in _bucket.index.status:pairs(status) do
        local id = b.id
        local ref = M.bucket_refs[id]
        if ref then
            assert(ref.rw == 0)
            if ref.ro ~= 0 then
                assert(ref.ro_lock)
                goto continue
            end
        end
        for _, space in pairs(sharded_spaces) do
            gc_bucket_in_space_xc(space, id, status)
            limit = limit - 1
            if limit == 0 then
                lfiber.sleep(0)
                limit = consts.BUCKET_CHUNK_SIZE
            end
        end
        route_map[id] = b.destination
        _bucket:delete{id}
    ::continue::
    end
end

--
-- Exception safe version of gc_bucket_drop_xc.
--
local function gc_bucket_drop(status, route_map)
    local err
    status, err = pcall(gc_bucket_drop_xc, status, route_map)
    if not status then
        box.rollback()
        return nil, err
    end
    return true
end

--
-- Try to turn a pack of SENT buckets to GARBAGE. Returns whether all buckets
-- were approved for deletion.
--
local function gc_bucket_process_sent_one_batch_xc(batch)
    local ok, err = wait_lsn(consts.GC_WAIT_LSN_TIMEOUT,
                             consts.GC_WAIT_LSN_STEP)
    if not ok then
        local msg = 'Failed to delete sent buckets - could not sync '..
                    'with replicas'
        local err2 = lerror.vshard(lerror.code.BUCKET_GC_ERROR, msg)
        err2.prev = err
        error(err2)
    end
    local rs = M.this_replicaset
    local opts = {
        timeout = consts.GC_MAP_CALL_TIMEOUT,
        -- Skip self - local buckets are already validated.
        except = M.this_replica.id,
    }
    local map_res
    map_res, err = rs:map_call('vshard.storage._call',
                               {'bucket_test_gc', batch}, opts)
    while M.errinj.ERRINJ_BUCKET_GC_LONG_REPLICAS_TEST do
        lfiber.sleep(0.01)
    end
    if map_res == nil then
        error(err)
    end
    local bids_ok_map = table.new(0, consts.BUCKET_CHUNK_SIZE)
    -- By default all buckets ok to be deleted. Then if a bucket appears not to
    -- be ok on at least one replica, it is deleted from the map. The rest are
    -- approved by each replica.
    for _, bid in ipairs(batch) do
        bids_ok_map[bid] = true
    end
    local is_done = true
    for _, res in pairs(map_res) do
        res, err = res[1], res[2]
        -- Can't fail so far.
        assert(err == nil)
        res = res.bids_not_ok
        if next(res) ~= nil then
            is_done = false
            for _, bid in ipairs(res) do
                bids_ok_map[bid] = nil
            end
        end
    end
    box.begin()
    local _bucket = box.space._bucket
    for bid, _ in pairs(bids_ok_map) do
        local bucket = _bucket:get(bid)
        local ref = M.bucket_refs[bid]
        if bucket == nil or bucket.status ~= consts.BUCKET.SENT or
           ref ~= nil and ref.ro ~= 0 then
            box.rollback()
            local msg = ('Failed to delete a sent bucket %s - it was changed '..
                         'outside of bucket GC procedure'):format(bid)
            error(lerror.vshard(lerror.code.BUCKET_GC_ERROR, msg))
        end
        assert(ref == nil or ref.rw == 0)
        _bucket:update({bid}, {{'=', 2, consts.BUCKET.GARBAGE}})
    end
    box.commit()
    return is_done
end

--
-- Try to turn all SENT buckets to GARBAGE. Returns a flag whether it managed to
-- get rid of all SENT buckets present when the function was called. Might be
-- blocked by some of them having RO refs on replicas or by the replicas being
-- not available.
--
local function gc_bucket_process_sent_xc()
    local limit = consts.BUCKET_CHUNK_SIZE
    local _bucket = box.space._bucket
    local batch = table.new(limit, 0)
    local i = 0
    local is_done = true
    local ref
    for _, b in _bucket.index.status:pairs(consts.BUCKET.SENT) do
        i = i + 1
        local bid = b.id
        if M.rebalancer_transfering_buckets[bid] then
            goto continue
        end
        ref = M.bucket_refs[bid]
        if ref ~= nil then
            assert(ref.rw == 0)
            if ref.ro ~= 0 then
                goto continue
            end
        end
        table.insert(batch, bid)
        if #batch < limit then
            goto continue
        end
        is_done = gc_bucket_process_sent_one_batch_xc(batch) and is_done
        batch = table.new(limit, 0)
    ::continue::
        if i % limit == 0 then
            fiber_yield()
        end
    end
    if next(batch) then
        is_done = gc_bucket_process_sent_one_batch_xc(batch) and is_done
    end
    return is_done
end

--
-- Exception safe version gc_bucket_process_sent_xc.
--
local function gc_bucket_process_sent()
    local ok, res = pcall(gc_bucket_process_sent_xc)
    if not ok then
        box.rollback()
        return false, nil, res
    end
    return true, res
end

--
-- Garbage collector. Works on masters. The garbage collector wakes up when
-- state of any bucket changes.
-- After wakeup it follows the plan:
-- 1) Check if state of any bucket has really changed. If not, then sleep again;
-- 2) Delete all GARBAGE buckets along with their data in chunks of limited
--    size.
-- 3) Turn those SENT buckets to GARBAGE, which are not used on any instance in
--    the replicaset.
-- 3) Bucket destinations are saved into a global route_map to reroute incoming
--    requests from routers in case they didn't notice the buckets being moved.
--    The saved routes are scheduled for deletion after a timeout, which is
--    checked on each iteration of this loop.
-- 4) Sleep, go to (1).
-- For each step details see comments in the code.
--
local function gc_bucket_service_f(service)
    local module_version = M.module_version
    -- Changes of _bucket increments bucket generation. Garbage
    -- collector has its own bucket generation which is <= actual.
    -- Garbage collection is finished, when collector's
    -- generation == bucket generation. In such a case the fiber
    -- does nothing until next _bucket change.
    local bucket_generation_collected = -1
    local bucket_generation_current = M.bucket_generation
    -- Deleted buckets are saved into a route map to redirect routers if they
    -- didn't discover new location of the buckets yet. However route map does
    -- not grow infinitely. Otherwise it would end up storing redirects for all
    -- buckets in the cluster. Which could also be outdated.
    -- Garbage collector periodically drops old routes from the map. For that it
    -- remembers state of route map in one moment, and after a while clears the
    -- remembered routes from the global route map.
    local route_map = M.route_map
    local route_map_old = {}
    local route_map_deadline = 0
    local status, err, is_done
    while M.module_version == module_version do
        service:next_iter()
        if M.errinj.ERRINJ_BUCKET_GC_PAUSE then
            M.errinj.ERRINJ_BUCKET_GC_PAUSE = 1
            lfiber.testcancel()
            lfiber.sleep(0.01)
            goto continue
        end
        if bucket_generation_collected ~= bucket_generation_current then
            service:set_activity('gc garbage')
            lfiber.testcancel()
            status, err = gc_bucket_drop(consts.BUCKET.GARBAGE, route_map)
            if status then
                service:set_activity('gc sent')
                lfiber.testcancel()
                status, is_done, err = gc_bucket_process_sent()
            end
            if not status then
                box.rollback()
                log.error(service:set_status_error(
                    'Error during garbage collection step: %s', err))
            elseif is_done then
                -- Don't use global generation. During the collection it could
                -- already change. Instead, remember the generation known before
                -- the collection has started.
                -- Since the collection also changes the generation, it makes
                -- the GC happen always at least twice. But typically on the
                -- second iteration it should not find any buckets to collect,
                -- and then the collected generation matches the global one.
                bucket_generation_collected = bucket_generation_current
            else
                -- Needs a retry. Can happen if not all SENT buckets were able
                -- to turn into GARBAGE.
                status = false
            end
        else
            status = true
            service:set_status_ok()
        end

        local sleep_time = route_map_deadline - fiber_clock()
        if sleep_time <= 0 then
            local chunk = consts.LUA_CHUNK_SIZE
            util.table_minus_yield(route_map, route_map_old, chunk)
            route_map_old = util.table_copy_yield(route_map, chunk)
            if next(route_map_old) then
                sleep_time = consts.BUCKET_SENT_GARBAGE_DELAY
            else
                sleep_time = consts.TIMEOUT_INFINITY
            end
            route_map_deadline = fiber_clock() + sleep_time
        end
        bucket_generation_current = M.bucket_generation

        local sleep_activity = 'idling'
        if bucket_generation_current ~= bucket_generation_collected then
            -- Generation was changed during collection. Or *by* collection.
            if status then
                -- Retry immediately. If the generation was changed by the
                -- collection itself, it will notice it next iteration, and go
                -- to proper sleep.
                sleep_time = 0
            else
                -- An error happened during the collection. Does not make sense
                -- to retry on each iteration of the event loop. The most likely
                -- errors are either a WAL error, or a transaction abort, or
                -- SENT buckets are still RO-refed on a replica - all these
                -- issues are not going to be fixed quickly anyway. Backoff.
                sleep_time = consts.GC_BACKOFF_INTERVAL
                sleep_activity = 'backoff'
            end
        end

        M.bucket_gc_count = M.bucket_gc_count + 1
        if M.module_version == module_version then
            service:set_activity(sleep_activity)
            lfiber.testcancel()
            bucket_generation_wait(sleep_time)
        end
    ::continue::
    end
end

local function gc_bucket_f()
    assert(not M.gc_service)
    local service = lservice_info.new('gc')
    M.gc_service = service
    local ok, err = pcall(gc_bucket_service_f, service)
    assert(M.gc_service == service)
    M.gc_service = nil
    if not ok then
        error(err)
    end
end

--
-- Immediately wakeup bucket garbage collector.
--
local function garbage_collector_wakeup()
    if M.collect_bucket_garbage_fiber then
        M.collect_bucket_garbage_fiber:wakeup()
    end
end

--
-- Delete data of a specified garbage bucket. If a bucket is not
-- garbage, then force option must be set. A bucket is not
-- deleted from _bucket space.
-- @param bucket_id Identifier of a bucket to delete data from.
-- @param opts Options. Can contain only 'force' flag to delete a
--        bucket regardless of is it garbage or not.
--
local function bucket_delete_garbage(bucket_id, opts)
    if bucket_id == nil or (opts ~= nil and type(opts) ~= 'table') then
        error('Usage: bucket_delete_garbage(bucket_id, opts)')
    end
    opts = opts or {}
    local bucket = box.space._bucket:get({bucket_id})
    local bucket_status = bucket and bucket.status
    if bucket ~= nil and bucket_status ~= consts.BUCKET.GARBAGE and
       not opts.force then
        error('Can not delete not garbage bucket. Use "{force=true}" to '..
              'ignore this attention')
    end
    local sharded_spaces = lschema.find_sharded_spaces()
    for _, space in pairs(sharded_spaces) do
        local status, err = gc_bucket_in_space(space, bucket_id, bucket_status)
        if not status then
            error(err)
        end
    end
end

--------------------------------------------------------------------------------
-- Rebalancer
--------------------------------------------------------------------------------
--
-- Calculate a set of metrics:
-- * maximal disbalance over all replicasets;
-- * needed buckets for each replicaset.
-- @param replicasets Map of type: {
--     <uuid or name> = {bucket_count = number, weight = number},
--     ...
-- }
--
-- @retval Maximal disbalance over all replicasets, and UUID or name of
--        a replicaset having it.
--
local function rebalancer_calculate_metrics(replicasets)
    local max_disbalance = 0
    local max_disbalance_id
    for replicaset_id, replicaset in pairs(replicasets) do
        local needed = replicaset.etalon_bucket_count - replicaset.bucket_count
        if replicaset.etalon_bucket_count ~= 0 then
            local disbalance =
                math.abs(needed) / replicaset.etalon_bucket_count * 100
            if disbalance > max_disbalance then
                max_disbalance = disbalance
                max_disbalance_id = replicaset_id
            end
        elseif replicaset.bucket_count ~= 0 then
            max_disbalance = math.huge
            max_disbalance_id = replicaset_id
        end
        assert(needed >= 0 or -needed <= replicaset.bucket_count)
        replicaset.needed = needed
    end
    return max_disbalance, max_disbalance_id
end

--
-- Move @a needed bucket count from a pool to @a dst_id and
-- remember the route in @a bucket_routes table.
--
local function rebalancer_take_buckets_from_pool(bucket_pool, bucket_routes,
                                                 dst_id, needed)
    local to_remove_from_pool = {}
    for src_id, bucket_count in pairs(bucket_pool) do
        local count = math.min(bucket_count, needed)
        local src = bucket_routes[src_id]
        if src == nil then
            bucket_routes[src_id] = {[dst_id] = count}
        else
            local dst = src[dst_id]
            if dst == nil then
                src[dst_id] = count
            else
                src[dst_id] = dst + count
            end
        end
        local new_count = bucket_pool[src_id] - count
        needed = needed - count
        bucket_pool[src_id] = new_count
        if new_count == 0 then
            table.insert(to_remove_from_pool, src_id)
        end
        if needed == 0 then
            break
        end
    end
    for _, src_id in pairs(to_remove_from_pool) do
        bucket_pool[src_id] = nil
    end
end

--
-- Build a table with routes defining from which node to which one
-- how many buckets should be moved to reach the best balance in
-- a cluster.
-- @param replicasets Map of type: {
--     id = {bucket_count = number, weight = number,
--           needed = number},
--     ...
-- }      This parameter is a result of
--        rebalancer_calculate_metrics().
--
-- @retval Bucket routes. It is a map of type: {
--     src_id = {
--         dst_id = number, -- Bucket count to move from
--                             src to dst.
--         ...
--     },
--     ...
-- }
--
local function rebalancer_build_routes(replicasets)
    -- Map of type: {
    --     id = number, -- free buckets of id.
    -- }
    local bucket_pool = {}
    for replicaset_id, replicaset in pairs(replicasets) do
        if replicaset.needed < 0 then
            bucket_pool[replicaset_id] = -replicaset.needed
            replicaset.needed = 0
        end
    end
    local bucket_routes = {}
    for replicaset_id, replicaset in pairs(replicasets) do
        if replicaset.needed > 0 then
            rebalancer_take_buckets_from_pool(bucket_pool, bucket_routes,
                                              replicaset_id, replicaset.needed)
        end
    end
    return bucket_routes
end

--
-- Dispenser is a container of routes received from the
-- rebalancer. Its task is to hand out the routes to worker fibers
-- in a round-robin manner so as any two sequential results are
-- different. It allows to spread dispensing evenly over the
-- receiver nodes.
--
local function route_dispenser_create(routes)
    local rlist = rlist.new()
    local map = {}
    for id, bucket_count in pairs(routes) do
        local new = {
            -- Receiver's ID.
            id = id,
            -- Rest of buckets to send. The receiver will be
            -- dispensed this number of times.
            bucket_count = bucket_count,
            -- Constant value to be able to track progress.
            need_to_send = bucket_count,
            -- Number of *successfully* sent buckets.
            progress = 0,
            -- If a user set too long max number of receiving
            -- buckets, or too high number of workers, worker
            -- fibers will receive 'throttle' errors, perhaps
            -- quite often. So as not to clog the log each
            -- destination is logged as throttled only once.
            is_throttle_warned = false,
        }
        -- Map of destinations is stored in addition to the queue,
        -- because
        -- 1) It is possible, that there are no more buckets to
        --    send, but suddenly one of the workers trying to send
        --    the last bucket receives a throttle error. In that
        --    case the bucket is put back, and the destination
        --    returns to the queue;
        -- 2) After all buckets are sent, and the queue is empty,
        --    the main applier fiber does some analysis on the
        --    destinations.
        map[id] = new
        rlist:add_tail(new)
    end
    return {
        rlist = rlist,
        map = map,
        -- Error, which occurred in `bucket_send` and led to
        -- skipping of the above-mentioned id from dispenser.
        error = nil,
    }
end

--
-- Put one bucket back to the dispenser. It happens, if the worker
-- receives a throttle error. This is the only error that can be
-- tolerated.
--
local function route_dispenser_put(dispenser, id)
    local dst = dispenser.map[id]
    if dst then
        local bucket_count = dst.bucket_count + 1
        dst.bucket_count = bucket_count
        if bucket_count == 1 then
            dispenser.rlist:add_tail(dst)
        end
    end
end

--
-- In case if a receiver responded with a serious error it is not
-- safe to send more buckets to there. For example, if it was a
-- timeout, it is unknown whether the bucket was received or not.
-- If it was a box error like index key conflict, then it is even
-- worse and the cluster is broken.
--
local function route_dispenser_skip(dispenser, id)
    local map = dispenser.map
    local dst = map[id]
    if dst then
        map[id] = nil
        dispenser.rlist:remove(dst)
    end
end

--
-- Set that the receiver @a id was throttled. When it happens
-- first time it is logged.
--
local function route_dispenser_throttle(dispenser, id)
    local dst = dispenser.map[id]
    if dst then
        local old_value = dst.is_throttle_warned
        dst.is_throttle_warned = true
        return not old_value
    end
    return false
end

--
-- Notify the dispenser that a bucket was successfully sent to
-- @a id. It has no any functional purpose except tracking
-- progress.
--
local function route_dispenser_sent(dispenser, id)
    local dst = dispenser.map[id]
    if dst then
        local new_progress = dst.progress + 1
        dst.progress = new_progress
        local need_to_send = dst.need_to_send
        return new_progress == need_to_send, need_to_send
    end
    return false
end

--
-- Take a next destination to send a bucket to.
--
local function route_dispenser_pop(dispenser)
    local rlist = dispenser.rlist
    local dst = rlist.first
    if dst then
        local bucket_count = dst.bucket_count - 1
        dst.bucket_count = bucket_count
        rlist:remove(dst)
        if bucket_count > 0 then
            rlist:add_tail(dst)
        end
        return dst.id
    end
    return nil
end

--
-- Body of one rebalancer worker. All the workers share a
-- dispenser to synchronize their round-robin, and a quit
-- condition to be able to quit, when one of the workers sees that
-- no more buckets are stored, and others took a nap because of
-- throttling.
--
local function rebalancer_worker_f(worker_id, dispenser, quit_cond)
    lfiber.name(string.format('vshard.rebalancer_worker_%d', worker_id))
    local _status = box.space._bucket.index.status
    local opts = {timeout = consts.REBALANCER_CHUNK_TIMEOUT}
    local active_key = {consts.BUCKET.ACTIVE}
    local id = route_dispenser_pop(dispenser)
    local worker_throttle_count = 0
    local bucket_id, is_found
    while id do
        is_found = false
        -- Can't just take a first active bucket. It may be
        -- already locked by a manual bucket_send in another
        -- fiber.
        for _, bucket in _status:pairs(active_key) do
            bucket_id = bucket.id
            if not M.rebalancer_transfering_buckets[bucket_id] then
                is_found = true
                break
            end
        end
        if not is_found then
            log.error('Can not find active buckets')
            break
        end
        local ret, err = bucket_send(bucket_id, id, opts)
        if ret then
            worker_throttle_count = 0
            local finished, total = route_dispenser_sent(dispenser, id)
            if finished then
                log.info('%d buckets were successfully sent to %s', total, id)
            end
            goto continue
        end
        route_dispenser_put(dispenser, id)
        if err.type ~= 'ShardingError' or
           err.code ~= lerror.code.TOO_MANY_RECEIVING then
            log.error('Error during rebalancer routes applying: receiver %s, '..
                      'error %s', id, err)
            log.info('Can not finish transfers to %s, skip to next round', id)
            worker_throttle_count = 0
            dispenser.error = dispenser.error or err
            route_dispenser_skip(dispenser, id)
            goto continue
        end
        worker_throttle_count = worker_throttle_count + 1
        if route_dispenser_throttle(dispenser, id) then
            log.error('Too many buckets is being sent to %s', id)
        end
        if worker_throttle_count < dispenser.rlist.count then
            goto continue
        end
        log.info('The worker was asked for throttle %d times in a row. '..
                 'Sleep for %d seconds', worker_throttle_count,
                 consts.REBALANCER_WORK_INTERVAL)
        worker_throttle_count = 0
        if not fiber_cond_wait(quit_cond, consts.REBALANCER_WORK_INTERVAL) then
            log.info('The worker is back')
        end
::continue::
        id = route_dispenser_pop(dispenser)
    end
    quit_cond:broadcast()
end

--
-- Main applier of rebalancer routes. It manages worker fibers,
-- logs total results.
--
local function rebalancer_service_apply_routes_f(service, routes)
    lfiber.name('vshard.rebalancer_applier')
    service:set_activity('applying routes')
    local worker_count = M.rebalancer_worker_count
    setmetatable(routes, {__serialize = 'mapping'})
    log.info('Apply rebalancer routes with %d workers:\n%s', worker_count,
             yaml_encode(routes))
    local dispenser = route_dispenser_create(routes)
    local _status = box.space._bucket.index.status
    assert(_status:count({consts.BUCKET.SENDING}) == 0)
    assert(_status:count({consts.BUCKET.RECEIVING}) == 0)
    -- Can not assign it on fiber.create() like
    -- var = fiber.create(), because when it yields, we have no
    -- guarantee that an event loop does not contain events
    -- between this fiber and its creator.
    M.rebalancer_applier_fiber = lfiber.self()
    local quit_cond = lfiber.cond()
    local workers = table.new(worker_count, 0)
    for i = 1, worker_count do
        local f = lfiber.new(rebalancer_worker_f, i, dispenser, quit_cond)
        f:set_joinable(true)
        workers[i] = f
    end
    log.info('Rebalancer workers have started, wait for their termination')
    for i = 1, worker_count do
        local f = workers[i]
        local ok, res = f:join()
        if not ok then
            log.error(service:set_status_error(
                'Rebalancer worker %d threw an exception: %s', i, res))
        end
    end
    if not dispenser.error then
        log.info('Rebalancer routes are applied')
        service:set_status_ok()
    else
        log.info(service:set_status_error(
            "Couldn't apply some rebalancer routes: %s", dispenser.error))
    end
    local throttled = {}
    for id, dst in pairs(dispenser.map) do
        if dst.is_throttle_warned then
            table.insert(throttled, id)
        end
    end
    if next(throttled) then
        log.warn('Note, the replicasets {%s} reported too many receiving '..
                 'buckets. Perhaps you need to increase '..
                 '"rebalancer_max_receiving" or decrease '..
                 '"rebalancer_worker_count"', table.concat(throttled, ', '))
    end
    service:set_activity('idling')
end

local function rebalancer_apply_routes_f(routes)
    assert(not M.routes_applier_service)
    local service = lservice_info.new('routes_applier')
    M.routes_applier_service = service
    local ok, err = pcall(rebalancer_service_apply_routes_f, service, routes)
    -- Delay service destruction in order to check states and errors
    while M.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY do
        lfiber.sleep(0.001)
    end
    assert(M.routes_applier_service == service)
    M.routes_applier_service = nil
    if not ok then
        error(err)
    end
end

--
-- Apply routes table of type: {
--     dst_id = number, -- Bucket count to send.
--     ...
-- }. Is used by a rebalancer.
--
local function rebalancer_apply_routes(routes)
    if is_this_replicaset_locked() then
        return nil, lerror.vshard(lerror.code.REPLICASET_IS_LOCKED);
    end
    local ok, err = check_is_master()
    if not ok then
        return nil, err
    end
    assert(not rebalancing_is_in_progress())
    -- Can not apply routes here because of gh-946 in tarantool
    -- about problems with long polling. Apply routes in a fiber.
    lfiber.create(rebalancer_apply_routes_f, routes)
    return true
end

--
-- From each replicaset download bucket count, check all buckets
-- to have SENT or ACTIVE state.
-- @retval not nil Argument of rebalancer_calculate_metrics().
-- @retval     nil Not all replicasets have only SENT and ACTIVE
--         buckets, or some replicasets are unavailable.
--
local function rebalancer_download_states()
    local replicasets = {}
    local total_bucket_locked_count = 0
    local total_bucket_active_count = 0
    for id, replicaset in pairs(M.replicasets) do
        local state = master_call(
            replicaset, 'vshard.storage.rebalancer_request_state', {},
            {timeout = consts.REBALANCER_GET_STATE_TIMEOUT})
        if state == nil then
            return
        end
        local bucket_count = state.bucket_active_count +
                             state.bucket_pinned_count
        if replicaset.lock then
            total_bucket_locked_count = total_bucket_locked_count + bucket_count
        else
            total_bucket_active_count = total_bucket_active_count + bucket_count
            replicasets[id] = {bucket_count = bucket_count,
                               weight = replicaset.weight,
                               pinned_count = state.bucket_pinned_count}
        end
    end
    local sum = total_bucket_active_count + total_bucket_locked_count
    if sum == M.total_bucket_count then
        return replicasets, total_bucket_active_count
    else
        log.info('Total active bucket count is not equal to total. '..
                 'Possibly a boostrap is not finished yet. Expected %d, but '..
                 'found %d', M.total_bucket_count, sum)
    end
end

--
-- Background rebalancer. Works on a storage which has the
-- smallest replicaset id and which is master.
--
local function rebalancer_service_f(service)
    local module_version = M.module_version
    while module_version == M.module_version do
        service:next_iter()
        while not M.is_rebalancer_active do
            log.info('Rebalancer is disabled. Sleep')
            M.rebalancer_service:set_activity('disabled')
            lfiber.testcancel()
            lfiber.sleep(consts.REBALANCER_IDLE_INTERVAL)
        end
        service:set_activity('downloading states')
        lfiber.testcancel()
        local status, replicasets, total_bucket_active_count =
            pcall(rebalancer_download_states)
        if M.module_version ~= module_version then
            return
        end
        if not status or replicasets == nil then
            if not status then
                log.error(service:set_status_error(
                    'Error during downloading rebalancer states: %s',
                    replicasets))
            end
            log.info('Some buckets are not active, retry rebalancing later')
            service:set_activity('idling')
            lfiber.testcancel()
            lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
            goto continue
        end
        lreplicaset.calculate_etalon_balance(replicasets,
                                             total_bucket_active_count)
        local max_disbalance, max_disbalance_id =
            rebalancer_calculate_metrics(replicasets)
        local threshold = M.rebalancer_disbalance_threshold
        if max_disbalance <= threshold then
            local balance_msg
            if max_disbalance > 0 then
                local rep = replicasets[max_disbalance_id]
                balance_msg = string.format(
                    'The cluster is balanced ok with max disbalance %f%% at '..
                    '"%s": etalon bucket count is %d, stored count is %d. '..
                    'The disbalance is smaller than your threshold %f%%, '..
                    'nothing to do.', max_disbalance, max_disbalance_id,
                    rep.etalon_bucket_count, rep.bucket_count, threshold)
            else
                balance_msg = 'The cluster is balanced ok.'
            end
            log.info('%s Schedule next rebalancing after %f seconds',
                     balance_msg, consts.REBALANCER_IDLE_INTERVAL)
            service:set_status_ok()
            service:set_activity('idling')
            lfiber.testcancel()
            lfiber.sleep(consts.REBALANCER_IDLE_INTERVAL)
            goto continue
        end
        local routes = rebalancer_build_routes(replicasets)
        -- Routes table can not be empty. If it had been empty,
        -- then max_disbalance would have been calculated
        -- incorrectly.
        assert(next(routes) ~= nil)
        for src_id, src_routes in pairs(routes) do
            service:set_activity('applying routes')
            local rs = M.replicasets[src_id]
            lfiber.testcancel()
            local status, err = master_call(
                rs, 'vshard.storage.rebalancer_apply_routes', {src_routes},
                {timeout = consts.REBALANCER_APPLY_ROUTES_TIMEOUT})
            if not status then
                log.error(service:set_status_error(
                    'Error during routes appying on "%s": %s. '..
                    'Try rebalance later', rs, lerror.make(err)))
                service:set_activity('idling')
                lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
                goto continue
            end
        end
        log.info('Rebalance routes are sent. Schedule next wakeup after '..
                 '%f seconds', consts.REBALANCER_WORK_INTERVAL)
        service:set_activity('idling')
        lfiber.testcancel()
        lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
::continue::
    end
end

local function rebalancer_f()
    assert(not M.rebalancer_service)
    local service = lservice_info.new('rebalancer')
    M.rebalancer_service = service
    local ok, err = pcall(rebalancer_service_f, service)
    assert(M.rebalancer_service == service)
    M.rebalancer_service = nil
    if not ok then
        error(err)
    end
end

--
-- Check all buckets of the host storage to have SENT or ACTIVE
-- state, return active bucket count.
-- @retval not nil Count of active buckets.
-- @retval     nil Not SENT or not ACTIVE buckets were found.
--
local function rebalancer_request_state()
    local ok, err = check_is_master()
    if not ok then
        return nil, err
    end
    if not M.is_rebalancer_active or rebalancing_is_in_progress() then
        return
    end
    local _bucket = box.space._bucket
    local status_index = _bucket.index.status
    if #status_index:select({consts.BUCKET.SENDING}, {limit = 1}) > 0 then
        return
    end
    if #status_index:select({consts.BUCKET.RECEIVING}, {limit = 1}) > 0 then
        return
    end
    if #status_index:select({consts.BUCKET.GARBAGE}, {limit = 1}) > 0 then
        return
    end
    return {
        bucket_active_count = status_index:count({consts.BUCKET.ACTIVE}),
        bucket_pinned_count = status_index:count({consts.BUCKET.PINNED}),
    }
end

--
-- Immediately wakeup rebalancer, if it exists on the current
-- node.
--
local function rebalancer_wakeup()
    if M.rebalancer_fiber ~= nil then
        M.rebalancer_fiber:wakeup()
    end
end

--
-- Disable/enable rebalancing. Disabled rebalancer sleeps until it
-- is enabled back. If not a rebalancer node is disabled, it does
-- not sends its state to rebalancer.
--
local function rebalancer_disable()
    M.is_rebalancer_active = false
end
local function rebalancer_enable()
    M.is_rebalancer_active = true
end

--
-- Find ID (UUID or name) of the instance which should run the
-- rebalancer service.
--
local function rebalancer_cfg_find_instance(cfg)
    assert(cfg.rebalancer_mode ~= 'off')
    local target_id
    local is_assigned
    local is_auto = cfg.rebalancer_mode == 'auto'
    for _, rs in pairs(cfg.sharding) do
        if rs.rebalancer == false then
            goto next_rs
        end
        for replica_id, replica in pairs(rs.replicas) do
            local is_rebalancer = rs.rebalancer or replica.rebalancer
            local no_rebalancer = replica.rebalancer == false
            if is_rebalancer and not is_assigned then
                is_assigned = true
                target_id = nil
            end
            local ok = true
            ok = ok and not no_rebalancer
            ok = ok and ((is_auto and replica.master) or replica.rebalancer)
            ok = ok and (not target_id or replica_id < target_id)
            ok = ok and (not is_assigned or is_rebalancer)
            if ok then
                target_id = replica_id
            end
        end
    ::next_rs::
    end
    return target_id
end

local function rebalancer_cfg_find_replicaset(cfg)
    assert(cfg.rebalancer_mode ~= 'off')
    local target_id
    local is_assigned
    local is_auto = cfg.rebalancer_mode == 'auto'
    for rs_id, rs in pairs(cfg.sharding) do
        local is_rebalancer = rs.rebalancer
        local no_rebalancer = rs.rebalancer == false
        if is_rebalancer and not is_assigned then
            is_assigned = true
            target_id = nil
        end
        local ok = true
        ok = ok and not no_rebalancer
        ok = ok and (rs.master == 'auto')
        ok = ok and (is_auto or is_rebalancer)
        ok = ok and (not target_id or rs_id < target_id)
        ok = ok and (not is_assigned or is_rebalancer)
        if ok then
            target_id = rs_id
            is_assigned = is_rebalancer
        end
    end
    return target_id
end

local function rebalancer_is_needed()
    if not M.is_configured then
        return false
    end
    local cfg = M.current_cfg
    if cfg.rebalancer_mode == 'off' then
        return false
    end
    local this_replica_id = M.this_replica.id
    local this_replicaset_id = M.this_replicaset.id

    local this_replicaset_cfg = cfg.sharding[this_replicaset_id]
    if this_replicaset_cfg.rebalancer == false then
        return false
    end
    local this_replica_cfg = this_replicaset_cfg.replicas[this_replica_id]
    if this_replica_cfg.rebalancer == false then
        return false
    end

    local id = rebalancer_cfg_find_instance(cfg)
    if id then
        return this_replica_id == id
    end
    id = rebalancer_cfg_find_replicaset(cfg)
    if id then
        return this_replicaset_id == id and this_is_master()
    end
    return false
end

--
-- Start or stop the rebalancer depending on the config and instance state.
--
local function rebalancer_role_update()
    local need_rebalancer = rebalancer_is_needed()
    if M.rebalancer_fiber then
        if not need_rebalancer then
            log.info('Stopping the rebalancer')
            M.rebalancer_fiber:cancel()
            M.rebalancer_fiber = nil
            return
        end
    elseif need_rebalancer then
        if not M.rebalancer_fiber then
            log.info('Starting the rebalancer')
            M.rebalancer_fiber =
                util.reloadable_fiber_create('vshard.rebalancer', M,
                                             'rebalancer_f')
            return
        end
    end
    if M.rebalancer_fiber then
        log.info('Wakeup the rebalancer')
        -- Configuration had changed. Time to check the balance.
        M.rebalancer_fiber:wakeup()
    end
end

--------------------------------------------------------------------------------
-- API
--------------------------------------------------------------------------------

-- Call wrapper
-- There is two modes for call operation: read and write, explicitly used for
-- call protocol: there is no way to detect what corresponding function does.
-- NOTE: may be a custom function call api without any checks is needed,
-- for example for some monitoring functions.
--
-- NOTE: this function uses pcall-style error handling
-- @retval nil, err Error.
-- @retval values Success.
local function storage_call(bucket_id, mode, name, args)
    local ok_ref, err = bucket_ref(bucket_id, mode)
    if not ok_ref then
        return nil, err
    end
    local ok, ret1, ret2, ret3 = local_call(name, args)
    if not ok then
        ret1 = lerror.make(ret1)
    end
    ok_ref, err = bucket_unref(bucket_id, mode)
    if not ok_ref then
        -- It should not normally happen. But the bucket could be deleted
        -- manually or due to a bug. Then the read request could see
        -- inconsistent data - the bucket could be half-deleted, for example.
        -- Regardless of what user's function returns, treat it as an error.
        if not ok then
            -- User function could also fail, return it as a stack of errors.
            -- Otherwise would be lost. Logs could produce too much garbage
            -- under high RPS.
            err.prev = ret1
        end
        return nil, err
    end
    -- Truncate nil values. Can't return them all because empty values turn into
    -- box.NULL. Even if user's function actually returned just 1 value, this
    -- would lead to '1, box.NULL, box.NULL' on the client. Not visible on the
    -- router when called normally, but won't help if the router did
    -- 'return_raw' and just forwards everything as is without truncation.
    -- Although this solution truncates really returned nils.
    if ret3 == nil then
        if ret2 == nil then
            if ret1 == nil then
                return ok
            end
            return ok, ret1
        end
        return ok, ret1, ret2
    end
    return ok, ret1, ret2, ret3
end

--
-- Bind a new storage ref to the current box session. Is used as a part of
-- Map-Reduce API.
--
local function storage_ref(rid, timeout)
    local ok, err = lref.add(rid, box.session.id(), timeout)
    if not ok then
        return nil, err
    end
    return bucket_count()
end

--
-- Drop a storage ref from the current box session. Is used as a part of
-- Map-Reduce API.
--
local function storage_unref(rid)
    return lref.del(rid, box.session.id())
end

--
-- Execute a user's function under an infinite storage ref protecting from
-- bucket moves. The ref should exist before, and is deleted after, regardless
-- of the function result. Is used as a part of Map-Reduce API.
--
local function storage_map(rid, name, args)
    local ok, err, res
    local sid = box.session.id()
    ok, err = lref.use(rid, sid)
    if not ok then
        return nil, err
    end
    ok, res = local_call(name, args)
    if not ok then
        lref.del(rid, sid)
        return nil, lerror.make(res)
    end
    ok, err = lref.del(rid, sid)
    if not ok then
        return nil, err
    end
    if res ~= nil then
        return true, res
    end
    return true
end

local function storage_service_info()
    return {
        is_master = this_is_master(),
        name = box.info.name,
    }
end

local service_call_api

local function service_call_test_api(...)
    return service_call_api, ...
end

service_call_api = setmetatable({
    bucket_recv = bucket_recv,
    bucket_test_gc = bucket_test_gc,
    rebalancer_apply_routes = rebalancer_apply_routes,
    rebalancer_request_state = rebalancer_request_state,
    recovery_bucket_stat = recovery_bucket_stat,
    storage_ref = storage_ref,
    storage_unref = storage_unref,
    storage_map = storage_map,
    info = storage_service_info,
    test_api = service_call_test_api,
}, {__serialize = function(api)
    local res = {}
    for k, _ in pairs(api) do
        table.insert(res, k)
    end
    table.sort(res)
    return res
end})

local function service_call(service_name, ...)
    return service_call_api[service_name](...)
end

--------------------------------------------------------------------------------
-- Master management
--------------------------------------------------------------------------------

local function master_role_update()
    if this_is_master() and M.is_configured then
        if not M.collect_bucket_garbage_fiber then
            M.collect_bucket_garbage_fiber =
                util.reloadable_fiber_create('vshard.gc', M, 'gc_bucket_f')
        end
        if not M.recovery_fiber then
            M.recovery_fiber =
                util.reloadable_fiber_create('vshard.recovery', M, 'recovery_f')
        end
    else
        if M.collect_bucket_garbage_fiber then
            M.collect_bucket_garbage_fiber:cancel()
            M.collect_bucket_garbage_fiber = nil
            log.info("GC stopped")
        end
        if M.recovery_fiber ~= nil then
            M.recovery_fiber:cancel()
            M.recovery_fiber = nil
            log.info('Recovery stopped')
        end
    end
end

local function master_on_disable()
    log.info("Stepping down from the master role")
    M.is_master = false
    M._on_master_disable:run()
    master_role_update()
    rebalancer_role_update()
end

local function master_on_enable()
    log.info("Stepping up into the master role")
    M.is_master = true
    M._on_master_enable:run()
    master_role_update()
    rebalancer_role_update()
end

local function master_auto_synchronize()
    if not M.this_replicaset.is_master_auto then
        return
    end
    -- Box.info.ro property is selected as a good indication whether the
    -- instance is master, because it works with the classical solution when
    -- box.cfg.read_only is used to assign the master manually, and with the
    -- automatic election mechanism - the elected leader might be read-only in
    -- certain situations, but a fully functioning one will be writable
    -- eventually.
    local is_master = not box.info.ro
    if is_master == this_is_master() then
        return
    end
    if is_master then
        master_on_enable()
    else
        master_on_disable()
    end
end

local function master_on_required()
    if M.conn_manager_fiber then
        M.conn_manager_fiber:wakeup()
    end
end

--------------------------------------------------------------------------------
-- Instance state management
--------------------------------------------------------------------------------

local function instance_watch_service_f(service)
    local module_version = M.module_version
    while module_version == M.module_version do
        service:next_iter()
        service:set_activity('idling')

        lfiber.testcancel()
        local is_ro = box.info.ro
        if is_ro then
            box.ctl.wait_rw()
        else
            box.ctl.wait_ro()
        end

        if is_ro ~= box.info.ro then
            local prefix = 'Instance state has changed from '
            if is_ro then
                log.info(prefix .. 'read-only to writable')
            else
                log.info(prefix .. 'writable to read-only')
            end
            service:set_activity('master switch')
            master_auto_synchronize()
        end
        service:set_status_ok()
    end
end

local function instance_watch_f()
    assert(not M.instance_watch_service)
    local service = lservice_info.new('instance_watch')
    M.instance_watch_service = service
    local ok, err = pcall(instance_watch_service_f, service)
    assert(M.instance_watch_service == service)
    M.instance_watch_service = nil
    if not ok then
        error(err)
    end
end

local function instance_watch_update()
    if M.this_replicaset.is_master_auto then
        if not M.instance_watch_fiber then
            M.instance_watch_fiber = util.reloadable_fiber_create(
                'vshard.state_watch', M, 'instance_watch_f')
        end
        return
    end
    if M.instance_watch_fiber then
        M.instance_watch_fiber:cancel()
        M.instance_watch_fiber = nil
    end
end

--------------------------------------------------------------------------------
-- Connection management
--------------------------------------------------------------------------------

local function conn_manager_locate_masters(service)
    local is_all_done = true
    local is_done, _, err
    for rs_id, rs in pairs(M.replicasets) do
        if rs.is_master_auto and not rs.master and rs.master_wait_count > 0 then
            is_done, _, err = rs:locate_master()
            if err then
                log.error(service:set_status_error(
                    'Error during master discovery for %s: %s', rs_id, err))
            end
            is_all_done = is_all_done and is_done
        end
    end
    return is_all_done
end

local function conn_manager_collect_idle_conns()
    local ts = fiber_clock()
    local count = 0
    for rs_id, rs in pairs(M.replicasets) do
        for _, replica in pairs(rs.replicas) do
            if replica == rs.master and rs.is_master_auto and
               not replica:is_connected() then
                log.warn('Discarded a not connected master %s in rs %s',
                         rs.master, rs_id)
                rs.master = nil
            end
            local c = replica.conn
            if c and replica.activity_ts and
               replica.activity_ts + consts.REPLICA_NOACTIVITY_TIMEOUT < ts then
                if replica == rs.master and rs.is_master_auto then
                    rs.master = nil
                end
                replica.conn = nil
                c:close()
                count = count + 1
            end
        end
    end
    if count > 0 then
        log.info('Closed %s unused connections', count)
    end
end

local function conn_manager_service_f(service)
    local module_version = M.module_version
    while module_version == M.module_version do
        service:next_iter()
        conn_manager_collect_idle_conns()
        local timeout
        service:set_activity('master discovery')
        lfiber.testcancel()
        if not conn_manager_locate_masters(service) then
            timeout = consts.MASTER_SEARCH_WORK_INTERVAL
            service:set_activity('backoff')
        else
            service:set_status_ok()
            service:set_activity('idling')
            timeout = consts.MASTER_SEARCH_IDLE_INTERVAL
        end
        lfiber.testcancel()
        lfiber.sleep(timeout)
    end
end

local function conn_manager_f()
    assert(not M.conn_manager_service)
    local service = lservice_info.new('conn_manager')
    M.conn_manager_service = service
    local ok, err = pcall(conn_manager_service_f, service)
    assert(M.conn_manager_service == service)
    M.conn_manager_service = nil
    if not ok then
        error(err)
    end
end

local function conn_manager_update()
    if not M.conn_manager_fiber then
        M.conn_manager_fiber = util.reloadable_fiber_create(
            'vshard.conn_man', M, 'conn_manager_f')
    end
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function storage_cfg_find_replicaset_id_for_instance(cfg, instance_id)
    for rs_id, rs in pairs(cfg.sharding) do
        for replica_id, _ in pairs(rs.replicas) do
            if replica_id == instance_id then
                return rs_id
            end
        end
    end
end

local function storage_cfg_build_local_box_cfg(cfgctx)
    local box_cfg = cfgctx.new_box_cfg
    if box_cfg.listen == nil then
        box_cfg.listen = cfgctx.new_instance_cfg.listen
        if box_cfg.listen == nil then
            box_cfg.listen = cfgctx.new_instance_cfg.uri
        end
    end
    if box_cfg.replication == nil then
        box_cfg.replication = {}
        for _, replica in pairs(cfgctx.new_replicaset_cfg.replicas) do
            table.insert(box_cfg.replication, replica.uri)
        end
    end
    if box_cfg.read_only == nil and not cfgctx.is_master_auto then
        box_cfg.read_only = not cfgctx.is_master_in_cfg
    end
    local cfg_rs_uuid, cfg_replica_uuid
    if cfgctx.new_cfg.identification_mode == 'name_as_key' then
        cfg_rs_uuid = box_cfg.replicaset_uuid or cfgctx.new_replicaset_cfg.uuid
        cfg_replica_uuid = box_cfg.instance_uuid or cfgctx.new_instance_cfg.uuid
    else
        cfg_rs_uuid = box_cfg.replicaset_uuid or cfgctx.replicaset_id
        cfg_replica_uuid = box_cfg.instance_uuid or cfgctx.instance_id
    end
    if type(box.cfg) == 'function' then
        if cfgctx.new_cfg.identification_mode == 'name_as_key' then
            box_cfg.instance_name = cfgctx.instance_id
            box_cfg.replicaset_name = cfgctx.replicaset_id
        end
        box_cfg.instance_uuid = cfg_replica_uuid
        box_cfg.replicaset_uuid = cfg_rs_uuid
    else
        local info = box.info
        if cfgctx.new_cfg.identification_mode == 'name_as_key' then
            -- Strictly verify names in case of reconfiguration. There's no
            -- other way to check, that config is applied to the correct
            -- instance. Changing names (even from nil) must be done externally.
            -- During bootstrap names will be set by vshard.
            if cfgctx.instance_id ~= info.name then
                error(string.format('Instance name mismatch: already set ' ..
                                    '"%s" but "%s" in arguments', info.name,
                                    cfgctx.instance_id))
            end
            local rs_name = info.replicaset and info.replicaset.name
            if cfgctx.replicaset_id ~= rs_name then
                error(string.format('Replicaset name mismatch: already set ' ..
                                    '"%s" but "%s" in vshard config',
                                    rs_name, cfgctx.replicaset_id))
            end
        end
        if cfg_replica_uuid and cfg_replica_uuid ~= info.uuid then
            error(string.format('Instance UUID mismatch: already set ' ..
                                '"%s" but "%s" in arguments', info.uuid,
                                cfg_replica_uuid))
        end
        local real_rs_uuid = util.replicaset_uuid(info)
        if cfg_rs_uuid and cfg_rs_uuid ~= real_rs_uuid then
            error(string.format('Replicaset UUID mismatch: already set ' ..
                                '"%s" but "%s" in vshard config',
                                real_rs_uuid, cfg_rs_uuid))
        end
    end
end

local function storage_cfg_master_prepare(cfgctx)
    local do_disable_via_config = false
    local do_enable_via_config = false
    if not cfgctx.was_master_auto and not cfgctx.is_master_auto then
        if cfgctx.was_master_in_cfg and not cfgctx.is_master_in_cfg then
            do_disable_via_config = true
        elseif not cfgctx.was_master_in_cfg and cfgctx.is_master_in_cfg then
            do_enable_via_config = true
        end
    elseif not cfgctx.was_master_auto and cfgctx.is_master_auto then
        log.info('Enabling master auto-detection')
    elseif cfgctx.was_master_auto and not cfgctx.is_master_auto then
        log.info('Disabling master auto-detection')
        if cfgctx.is_master_in_cfg then
            do_enable_via_config = true
        else
            do_disable_via_config = true
        end
    else
        assert(cfgctx.was_master_auto and cfgctx.is_master_auto)
    end

    if do_disable_via_config then
        log.info('Disabling master via config')
        if not cfgctx.old_cfg or cfgctx.old_cfg.read_only == nil then
            table.insert(cfgctx.rollback_guards, {
                name = 'master_disable_prepare', func = function()
                    log.info('Revert master disable')
                    box.cfg({read_only = false})
                end
            })
            box.cfg({read_only = true})
            sync(cfgctx.new_cfg.sync_timeout)
        end
    elseif do_enable_via_config then
        --
        -- Promote does not require sync, because a replica can not have data,
        -- that is not on the current master - the replica is read only. But
        -- read_only can not be set to false here, because until box.cfg is
        -- called, it can not be guaranteed, that the promotion will be
        -- successful.
        --
        log.info('Enabling master via config')
    end
end

local function storage_cfg_master_commit(cfgctx)
    if not cfgctx.was_master_auto and not cfgctx.is_master_auto then
        if cfgctx.was_master_in_cfg and not cfgctx.is_master_in_cfg then
            master_on_disable()
        elseif not cfgctx.was_master_in_cfg and cfgctx.is_master_in_cfg then
            if cfgctx.new_cfg.read_only == nil then
                box.cfg({read_only = false})
            end
            master_on_enable()
        end
    elseif not cfgctx.was_master_auto and cfgctx.is_master_auto then
        master_auto_synchronize()
    elseif cfgctx.was_master_auto and not cfgctx.is_master_auto then
        if cfgctx.is_master_in_cfg ~= this_is_master() then
            if cfgctx.is_master_in_cfg then
                master_on_enable()
            else
                master_on_disable()
            end
        end
    else
        assert(cfgctx.was_master_auto and cfgctx.is_master_auto)
        master_auto_synchronize()
    end
end

local function storage_cfg_context_extend(cfgctx)
    local instance_id = cfgctx.instance_id
    if instance_id == nil then
        error('Usage: cfg(configuration, this_replica_id)')
    end
    local full_cfg = lcfg.check(cfgctx.full_cfg, M.current_cfg)
    local new_cfg = lcfg.extract_vshard(full_cfg)
    if new_cfg.weights or new_cfg.zone then
        error('Weights and zone are not allowed for storage configuration')
    end
    cfgctx.new_cfg = new_cfg
    cfgctx.old_cfg = M.current_cfg

    local replicaset_id = storage_cfg_find_replicaset_id_for_instance(
        new_cfg, instance_id)
    if not replicaset_id then
        error(string.format("Local replica %s wasn't found in config",
                            instance_id))
    end
    cfgctx.replicaset_id = replicaset_id

    if cfgctx.old_cfg then
        -- Fallback to uuid for reload from the old version.
        local old_instance_id = M.this_replica.id or M.this_replica.uuid
        local old_replicaset_id = M.this_replicaset.id or M.this_replicaset.uuid
        local old_rs_cfg = cfgctx.old_cfg.sharding[old_replicaset_id]
        cfgctx.old_replicaset_cfg = old_rs_cfg
        cfgctx.old_instance_cfg = old_rs_cfg.replicas[old_instance_id]
        cfgctx.was_master_in_cfg = cfgctx.old_instance_cfg.master
        cfgctx.was_master_auto = old_rs_cfg.master == 'auto'
    end
    local new_rs_cfg = new_cfg.sharding[replicaset_id]
    cfgctx.new_replicaset_cfg = new_rs_cfg
    cfgctx.new_instance_cfg = new_rs_cfg.replicas[instance_id]
    cfgctx.new_box_cfg = lcfg.extract_box(full_cfg, cfgctx.new_instance_cfg)
    cfgctx.is_master_in_cfg = cfgctx.new_instance_cfg.master
    cfgctx.is_master_auto = new_rs_cfg.master == 'auto'
end

local function storage_cfg_services_update()
    instance_watch_update()
    conn_manager_update()
    master_role_update()
    rebalancer_role_update()
end

local function storage_cfg_xc(cfgctx)
    storage_cfg_context_extend(cfgctx)
    local instance_id = cfgctx.instance_id
    local replicaset_id = cfgctx.replicaset_id
    local new_cfg = cfgctx.new_cfg
    if M.replicasets then
        log.info("Starting reconfiguration of replica %s", instance_id)
    else
        log.info("Starting configuration of replica %s", instance_id)
    end
    if cfgctx.new_cfg.box_cfg_mode == 'manual' then
        if type(box.cfg) == 'function' then
            local msg = "Box must be configured, when box_cfg_mode is 'manual'"
            error(lerror.vshard(lerror.code.STORAGE_IS_DISABLED, msg))
        end
        log.info("Box configuration was skipped due to the 'manual' " ..
                 "box_cfg_mode")
    end

    local new_replicasets = lreplicaset.buildall(new_cfg)
    for _, rs in pairs(new_replicasets) do
        rs.on_master_required = master_on_required
    end
    -- It is considered that all possible errors during cfg
    -- process occur only before this place.
    -- This check should be placed as late as possible.
    if M.errinj.ERRINJ_CFG then
        error('Error injection: cfg')
    end

    if cfgctx.new_cfg.box_cfg_mode ~= 'manual' and not cfgctx.is_reload then
        storage_cfg_master_prepare(cfgctx)
        storage_cfg_build_local_box_cfg(cfgctx)

        box.cfg(cfgctx.new_box_cfg)
        cfgctx.rollback_guards = {}
        while M.errinj.ERRINJ_CFG_DELAY do
            lfiber.sleep(0.01)
        end
        log.info("Box has been configured")
    end
    lref.cfg()
    lsched.cfg(new_cfg)
    lschema.cfg(new_cfg)
    lreplicaset.rebind_replicasets(new_replicasets, M.replicasets)
    lreplicaset.outdate_replicasets(M.replicasets)
    M.replicasets = new_replicasets
    M.this_replicaset = new_replicasets[replicaset_id]
    M.this_replica = M.this_replicaset.replicas[instance_id]
    M.total_bucket_count = new_cfg.bucket_count
    M.rebalancer_disbalance_threshold = new_cfg.rebalancer_disbalance_threshold
    M.rebalancer_receiving_quota = new_cfg.rebalancer_max_receiving
    M.rebalancer_worker_count = new_cfg.rebalancer_max_sending
    M.sync_timeout = new_cfg.sync_timeout
    M.current_cfg = new_cfg
    storage_cfg_master_commit(cfgctx)
    storage_cfg_services_update()

    if this_is_master() then
        local uri = luri.parse(M.this_replica.uri)
        lschema.upgrade(lschema.latest_version, uri.login, uri.password)
    else
        schema_upgrade_replica()
    end

    -- Check for master specifically. On master _bucket space must exist.
    -- Because it should have done the schema bootstrap. Shall not ever try to
    -- do anything delayed.
    if this_is_master() or box.space._bucket then
        schema_install_triggers()
    else
        schema_install_triggers_delayed()
    end

    M.is_configured = true
    -- If the config was the first one, some services could need to be updated
    -- to take it into account. For example, to know that the _bucket was
    -- created and is protected with all the triggers.
    storage_cfg_services_update()
    -- Destroy connections, not used in a new configuration.
    collectgarbage()
end

local function storage_cfg(cfg, this_replica_id, is_reload)
    if M.is_cfg_in_progress then
        return error(lerror.vshard(lerror.code.STORAGE_CFG_IS_IN_PROGRESS))
    end
    local cfgctx = {
        instance_id = this_replica_id,
        full_cfg = cfg,
        is_reload = is_reload,
        rollback_guards = {},
    }

    M.is_cfg_in_progress = true
    local ok, err = pcall(storage_cfg_xc, cfgctx)
    if not ok then
        for i = #cfgctx.rollback_guards, 1, -1 do
            local guard = cfgctx.rollback_guards[i]
            local guard_ok, guard_err = pcall(guard.func)
            if not guard_ok then
                log.info('Failed to rollback cfg guard %s - %s', guard.name,
                         guard_err)
            end
        end
    end
    M.is_cfg_in_progress = false
    if not ok then
        error(err)
    end
end

--------------------------------------------------------------------------------
-- Monitoring
--------------------------------------------------------------------------------

local function storage_buckets_info(bucket_id)
    local ibuckets = setmetatable({}, { __serialize = 'mapping' })

    for _, bucket in box.space._bucket:pairs({bucket_id}) do
        local ref = M.bucket_refs[bucket.id]
        local desc = {
            id = bucket.id,
            status = bucket.status,
            destination = bucket.destination,
        }
        if ref then
            if ref.ro ~= 0 then desc.ref_ro = ref.ro end
            if ref.rw ~= 0 then desc.ref_rw = ref.rw end
            if ref.ro_lock then desc.ro_lock = ref.ro_lock end
            if ref.rw_lock then desc.rw_lock = ref.rw_lock end
        end
        ibuckets[bucket.id] = desc
    end

    return ibuckets
end

local function storage_info(opts)
    local state = {
        alerts = {},
        replication = {},
        bucket = {},
        status = consts.STATUS.GREEN,
    }
    local code = lerror.code
    local alert = lerror.alert
    local this_id = M.this_replicaset.id
    local this_master = M.this_replicaset.master
    if this_master == nil and not M.this_replicaset.is_master_auto then
        table.insert(state.alerts, alert(code.MISSING_MASTER, this_id))
        state.status = math.max(state.status, consts.STATUS.ORANGE)
    end
    local is_named = M.this_replica.id == M.this_replica.name
    if this_master and this_master ~= M.this_replica then
        for _, replica in pairs(box.info.replication) do
            if (not is_named and replica.uuid ~= this_master.uuid)
                or (is_named and replica.name ~= this_master.name) then
                goto cont
            end
            state.replication.status = replica.upstream.status
            if replica.upstream.status ~= 'follow' then
                state.replication.idle = replica.upstream.idle
                table.insert(state.alerts, alert(code.UNREACHABLE_MASTER,
                                                 this_id,
                                                 replica.upstream.status))
                if replica.upstream.idle > consts.REPLICATION_THRESHOLD_FAIL then
                    state.status = math.max(state.status, consts.STATUS.RED)
                elseif replica.upstream.idle > consts.REPLICATION_THRESHOLD_HARD then
                    state.status = math.max(state.status, consts.STATUS.ORANGE)
                else
                    state.status = math.max(state.status, consts.STATUS.YELLOW)
                end
                goto cont
            end

            state.replication.lag = replica.upstream.lag
            if state.replication.lag >= consts.REPLICATION_THRESHOLD_FAIL then
                table.insert(state.alerts, alert(code.OUT_OF_SYNC))
                state.status = math.max(state.status, consts.STATUS.RED)
            elseif state.replication.lag >= consts.REPLICATION_THRESHOLD_HARD then
                table.insert(state.alerts, alert(code.HIGH_REPLICATION_LAG,
                                                 state.replication.lag))
                state.status = math.max(state.status, consts.STATUS.ORANGE)
            elseif state.replication.lag >= consts.REPLICATION_THRESHOLD_SOFT then
                table.insert(state.alerts, alert(code.HIGH_REPLICATION_LAG,
                                                 state.replication.lag))
                state.status = math.max(state.status, consts.STATUS.YELLOW)
            end
            ::cont::
        end
    elseif this_is_master() then
        state.replication.status = 'master'
        local replica_count = 0
        local not_available_replicas = 0
        for _, replica in pairs(box.info.replication) do
            if (not is_named and replica.uuid ~= M.this_replica.uuid)
                or (is_named and replica.name ~= M.this_replica.name) then
                replica_count = replica_count + 1
                if replica.downstream == nil or
                   replica.downstream.vclock == nil then
                    table.insert(state.alerts, alert(code.UNREACHABLE_REPLICA,
                                                     is_named and replica.name
                                                     or replica.uuid))
                    state.status = math.max(state.status, consts.STATUS.YELLOW)
                    not_available_replicas = not_available_replicas + 1
                end
            end
        end
        local available_replicas = replica_count - not_available_replicas
        if replica_count > 0 and available_replicas == 0 then
            table.insert(state.alerts, alert(code.UNREACHABLE_REPLICASET,
                                             this_id))
            state.status = math.max(state.status, consts.STATUS.RED)
        elseif replica_count > 1 and available_replicas == 1 then
            table.insert(state.alerts, alert(code.LOW_REDUNDANCY))
            state.status = math.max(state.status, consts.STATUS.ORANGE)
        end
    else
        state.replication.status = 'slave'
    end

    if is_this_replicaset_locked() then
        state.bucket.lock = true
    end
    local status = box.space._bucket.index.status
    local pinned = status:count({consts.BUCKET.PINNED})
    state.bucket.total = box.space._bucket:count()
    state.bucket.active = status:count({consts.BUCKET.ACTIVE}) + pinned
    state.bucket.garbage = status:count({consts.BUCKET.SENT})
    state.bucket.receiving = status:count({consts.BUCKET.RECEIVING})
    state.bucket.sending = status:count({consts.BUCKET.SENDING})
    state.bucket.pinned = pinned
    if state.bucket.receiving ~= 0 and state.bucket.sending ~= 0 then
        --
        --Some buckets are receiving and some buckets are sending at same time,
        --this may be a balancer issue, alert it.
        --
        table.insert(state.alerts, alert(lerror.code.INVALID_REBALANCING))
        state.status = math.max(state.status, consts.STATUS.YELLOW)
    end

    local ireplicasets = {}
    for id, replicaset in pairs(M.replicasets) do
        local master = replicaset.master
        local master_info
        if replicaset.is_master_auto then
            master_info = 'auto'
        elseif not master then
            master_info = 'missing'
        else
            local uri = master:safe_uri()
            local conn = master.conn
            master_info = {
                uri = uri, uuid = conn and conn.peer_uuid,
                name = is_named and master.name or nil,
                state = conn and conn.state, error = conn and conn.error,
            }
        end
        ireplicasets[id] = {
            uuid = replicaset.uuid,
            name = is_named and replicaset.name or nil,
            master = master_info,
        }
    end
    state.replicasets = ireplicasets
    state.identification_mode = M.current_cfg.identification_mode
    state.uri = M.this_replica:safe_uri()
    if opts and opts.with_services then
        state.services = {
            gc = M.gc_service and M.gc_service:info(),
            rebalancer = M.rebalancer_service and M.rebalancer_service:info(),
            recovery = M.recovery_service and M.recovery_service:info(),
            routes_applier = M.routes_applier_service and
                M.routes_applier_service:info(),
            instance_watch = M.instance_watch_service and
                M.instance_watch_service:info(),
        }
    end
    return state
end

--------------------------------------------------------------------------------
-- Public API protection
--------------------------------------------------------------------------------

--
-- Arguments are listed explicitly instead of '...' because the latter does not
-- jit.
--
local function storage_api_call_safe(func, arg1, arg2, arg3, arg4)
    return func(arg1, arg2, arg3, arg4)
end

--
-- Unsafe proxy is loaded with protections. But it is used rarely and only in
-- the beginning of instance's lifetime.
--
local function storage_api_call_unsafe(func, arg1, arg2, arg3, arg4)
    -- box.info is quite expensive. Avoid calling it again when the instance
    -- is finally loaded.
    if not M.is_loaded then
        if type(box.cfg) == 'function' then
            local msg = 'box seems not to be configured'
            return error(lerror.vshard(lerror.code.STORAGE_IS_DISABLED, msg))
        end
        local status = box.info.status
        -- 'Orphan' is allowed because even if a replica is an orphan, it still
        -- could be up to date. Just not all other replicas are connected.
        if status ~= 'running' and status ~= 'orphan' then
            local msg = ('instance status is "%s"'):format(status)
            return error(lerror.vshard(lerror.code.STORAGE_IS_DISABLED, msg))
        end
        M.is_loaded = true
    end
    if not M.is_configured then
        local msg = 'storage is not configured'
        return error(lerror.vshard(lerror.code.STORAGE_IS_DISABLED, msg))
    end
    if not M.is_enabled then
        local msg = 'storage is disabled explicitly'
        return error(lerror.vshard(lerror.code.STORAGE_IS_DISABLED, msg))
    end
    M.api_call_cache = storage_api_call_safe
    return func(arg1, arg2, arg3, arg4)
end

M.api_call_cache = storage_api_call_unsafe

local function storage_make_api(func)
    return function(arg1, arg2, arg3, arg4)
        return M.api_call_cache(func, arg1, arg2, arg3, arg4)
    end
end

local function storage_enable()
    M.is_enabled = true
end

--
-- Disable can be used in case the storage entered a critical state in which
-- requests are not allowed. For instance, its config got broken or too old
-- compared to a centric config somewhere.
--
local function storage_disable()
    M.is_enabled = false
    M.api_call_cache = storage_api_call_unsafe
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------
--
-- There are 3 function types:
-- 1) public functions, returned by require('module') and are not
--    used inside module;
-- 2) functions, used inside or/and outside of module;
-- 3) infinite functions for background fibers.
--
-- To update these functions hot reload can be used, when lua code
-- is updated with no process restart.
-- Reload can be successful (an interpreter received 'return'
-- command) or unsuccessful (fail before 'return').
-- If a reload has been failed, then no functions must be updated.
-- The module must continue work like there was no reload. To
-- provide this atomicity, some functions must be saved in module
-- attributes. The main task is to determine, which functions
-- is enough to store in the module members to reach atomic
-- reload.
--
-- Functions of type 1 can be omitted in module members, because
-- they already are updated only on success reload by definition
-- (reload is success, if an interpreter came to the 'return'
-- command).
--
-- Functions of type 2 can be omitted, because outside of a module
-- they are updated only in a case of successful reload, and
-- inside of the module they are used only inside functions of the
-- type 3.
--
-- Functions of type 3 MUST be saved in a module attributes,
-- because they actually contain pointers to all other functions.
--
-- For example:
--
-- local function func1()
--     ...
-- end
--
-- local function func2()
--     ...
-- end
--
-- local function background_f()
--     while module_version == M.module_version do
--         func1()
--         func2()
--    end
-- end
-- M.background_f = background_f
--
-- On the first module load background_f is created with pointers
-- to func1 and func2. After the module is reloaded (or failed to
-- reload) func1 and func2 are updated, but background_f still
-- uses old func1 and func2 until it stops.
--
-- If the module reload was unsuccessful, then background_f is not
-- restarted (or is restarted from M.background_f, which is not
-- changed) and continues use old func1 and func2.
--

if not rawget(_G, MODULE_INTERNALS) then
    rawset(_G, MODULE_INTERNALS, M)
else
    reload_evolution.upgrade(M)
    if M.current_cfg then
        storage_cfg(M.current_cfg, M.this_replica.id or M.this_replica.uuid,
                    true)
    end
    M.module_version = M.module_version + 1
    -- Background fibers could sleep waiting for bucket changes.
    -- Let them know it is time to reload.
    bucket_generation_increment()
end

M.recovery_f = recovery_f
M.rebalancer_f = rebalancer_f
M.gc_bucket_f = gc_bucket_f
M.instance_watch_f = instance_watch_f
M.conn_manager_f = conn_manager_f

--
-- These functions are saved in M not for atomic reload, but for
-- unit testing.
--
M.gc_bucket_drop = gc_bucket_drop
M.rebalancer_build_routes = rebalancer_build_routes
M.rebalancer_calculate_metrics = rebalancer_calculate_metrics
M.route_dispenser = {
    create = route_dispenser_create,
    put = route_dispenser_put,
    throttle = route_dispenser_throttle,
    skip = route_dispenser_skip,
    pop = route_dispenser_pop,
    sent = route_dispenser_sent,
}
M.bucket_state_edges = bucket_state_edges

M.bucket_are_all_rw = bucket_are_all_rw_public
M.bucket_generation_wait = bucket_generation_wait
lregistry.storage = M

--
-- Not all methods are public here. Private methods should not be exposed if
-- possible. At least not without notable difference in naming.
--
return {
    --
    -- Bucket methods.
    --
    bucket_force_create = storage_make_api(bucket_force_create),
    bucket_force_drop = storage_make_api(bucket_force_drop),
    bucket_collect = storage_make_api(bucket_collect),
    bucket_recv = storage_make_api(bucket_recv),
    bucket_send = storage_make_api(bucket_send),
    bucket_stat = storage_make_api(bucket_stat),
    bucket_pin = storage_make_api(bucket_pin),
    bucket_unpin = storage_make_api(bucket_unpin),
    bucket_ref = storage_make_api(bucket_ref),
    bucket_unref = storage_make_api(bucket_unref),
    bucket_refro = storage_make_api(bucket_refro),
    bucket_refrw = storage_make_api(bucket_refrw),
    bucket_unrefro = storage_make_api(bucket_unrefro),
    bucket_unrefrw = storage_make_api(bucket_unrefrw),
    bucket_delete_garbage = storage_make_api(bucket_delete_garbage),
    _bucket_delete_garbage = bucket_delete_garbage,
    buckets_info = storage_make_api(storage_buckets_info),
    buckets_count = storage_make_api(bucket_count_public),
    buckets_discovery = storage_make_api(buckets_discovery),
    --
    -- Garbage collector.
    --
    garbage_collector_wakeup = storage_make_api(garbage_collector_wakeup),
    --
    -- Rebalancer.
    --
    rebalancer_wakeup = storage_make_api(rebalancer_wakeup),
    rebalancer_apply_routes = storage_make_api(rebalancer_apply_routes),
    rebalancer_disable = storage_make_api(rebalancer_disable),
    rebalancer_enable = storage_make_api(rebalancer_enable),
    rebalancing_is_in_progress = storage_make_api(rebalancing_is_in_progress),
    rebalancer_request_state = storage_make_api(rebalancer_request_state),
    _rebalancer_request_state = rebalancer_request_state,
    --
    -- Recovery.
    --
    recovery_wakeup = storage_make_api(recovery_wakeup),
    --
    -- Instance info.
    --
    is_locked = storage_make_api(is_this_replicaset_locked),
    info = storage_make_api(storage_info),
    sharded_spaces = storage_make_api(storage_sharded_spaces),
    _sharded_spaces = storage_sharded_spaces,
    module_version = function() return M.module_version end,
    --
    -- Miscellaneous.
    --
    call = storage_make_api(storage_call),
    _call = storage_make_api(service_call),
    sync = storage_make_api(sync),
    cfg = function(cfg, id) return storage_cfg(cfg, id, false) end,
    on_master_enable = on_master_enable,
    on_master_disable = on_master_disable,
    on_bucket_event = on_bucket_event,
    enable = storage_enable,
    disable = storage_disable,
    internal = M,
}
