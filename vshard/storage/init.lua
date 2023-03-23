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
        -- Index which is a trigger to shard its space by numbers in
        -- this index. It must have at first part either unsigned,
        -- or integer or number type and be not nullable. Values in
        -- this part are considered as bucket identifiers.
        shard_index = nil,
        -- Bucket count stored on all replicasets.
        total_bucket_count = 0,
        errinj = {
            ERRINJ_CFG = false,
            ERRINJ_RELOAD = false,
            ERRINJ_CFG_DELAY = false,
            ERRINJ_LONG_RECEIVE = false,
            ERRINJ_LAST_RECEIVE_DELAY = false,
            ERRINJ_RECEIVE_PARTIALLY = false,
            ERRINJ_RECOVERY_PAUSE = false,
            ERRINJ_UPGRADE = false,
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
        -- Reference to the function-proxy to most of the public functions. It
        -- allows to avoid 'if's in each function by adding expensive
        -- conditional checks in one rarely used version of the wrapper and no
        -- checks into the other almost always used wrapper.
        api_call_cache = nil,

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
end

--
-- Invoke a function on this instance. Arguments are unpacked into the function
-- as arguments.
-- The function returns pcall() as is, because is used from places where
-- exceptions are not allowed.
--
local function local_call(func_name, args)
    return pcall(netbox_self_call, netbox_self, func_name, args)
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

local schema_version_mt = {
    __tostring = function(self)
        return string.format('{%s}', table.concat(self, '.'))
    end,
    __serialize = function(self)
        return tostring(self)
    end,
    __eq = function(l, r)
        return l[1] == r[1] and l[2] == r[2] and l[3] == r[3] and l[4] == r[4]
    end,
    __lt = function(l, r)
        for i = 1, 4 do
            local diff = l[i] - r[i]
            if diff < 0 then
                return true
            elseif diff > 0 then
                return false
            end
        end
        return false;
    end,
}

local function schema_version_make(ver)
    return setmetatable(ver, schema_version_mt)
end

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

-- VShard versioning works in 4 numbers: major, minor, patch, and
-- a last helper number incremented on every schema change, if
-- first 3 numbers stay not changed. That happens when users take
-- the latest master version not having a tag yet. They couldn't
-- upgrade if not the 4th number changed inside one tag.

-- The schema first time appeared with 0.1.16. So this function
-- describes schema before that - 0.1.15.
local function schema_init_0_1_15_0(username, password)
    log.info("Initializing schema %s", schema_version_make({0, 1, 15, 0}))
    box.schema.user.create(username, {
        password = password,
        if_not_exists = true,
    })
    -- Replication may has not been granted, if user exists.
    box.schema.user.grant(username, 'replication', nil, nil,
                          {if_not_exists = true})

    local bucket = box.schema.space.create('_bucket')
    bucket:format({
        {'id', 'unsigned'},
        {'status', 'string'},
        {'destination', 'string', is_nullable = true}
    })
    bucket:create_index('pk', {parts = {'id'}})
    bucket:create_index('status', {parts = {'status'}, unique = false})

    local storage_api = {
        'vshard.storage.sync',
        'vshard.storage.call',
        'vshard.storage.bucket_force_create',
        'vshard.storage.bucket_force_drop',
        'vshard.storage.bucket_collect',
        'vshard.storage.bucket_send',
        'vshard.storage.bucket_recv',
        'vshard.storage.bucket_stat',
        'vshard.storage.buckets_count',
        'vshard.storage.buckets_info',
        'vshard.storage.buckets_discovery',
        'vshard.storage.rebalancer_request_state',
        'vshard.storage.rebalancer_apply_routes',
    }

    for _, name in ipairs(storage_api) do
        box.schema.func.create(name, {setuid = true})
        box.schema.user.grant(username, 'execute', 'function', name)
    end

    box.space._schema:replace({'vshard_version', 0, 1, 15, 0})
end

local function schema_upgrade_to_0_1_16_0(username)
    -- Since 0.1.16.0 the old versioning by
    -- 'oncevshard:storage:<number>' is dropped because it is not
    -- really extendible nor understandable.
    log.info("Insert 'vshard_version' into _schema")
    box.space._schema:replace({'vshard_version', 0, 1, 16, 0})
    box.space._schema:delete({'oncevshard:storage:1'})

    -- vshard.storage._call() is supposed to replace some internal
    -- functions exposed in _func; to allow introduction of new
    -- functions on replicas; to allow change of internal
    -- functions without touching the schema.
    local func = 'vshard.storage._call'
    log.info('Create function %s()', func)
    box.schema.func.create(func, {setuid = true})
    box.schema.user.grant(username, 'execute', 'function', func)
    -- Don't drop old functions in the same version. Removal can
    -- happen only after 0.1.16. Or there should appear support of
    -- rebalancing from too old versions. Drop of these functions
    -- now would immediately make it impossible to rebalance from
    -- old instances.
end

local function schema_downgrade_from_0_1_16_0()
    log.info("Remove 'vshard_version' from _schema")
    box.space._schema:replace({'oncevshard:storage:1'})
    box.space._schema:delete({'vshard_version'})

    local func = 'vshard.storage._call'
    log.info('Remove function %s()', func)
    box.schema.func.drop(func, {if_exists = true})
end

--
-- Make vshard.storage.bucket_recv() understand raw args as msgpack object. It
-- doesn't necessarily make it faster, but is essential to preserve the original
-- tuples as is, without their contortion through Lua tables. It would break
-- field types like MP_BIN (varbinary).
--
local function schema_upgrade_bucket_recv_raw()
    if not util.feature.msgpack_object then
        return
    end
    if box.func == nil then
        return
    end
    local name = 'vshard.storage.bucket_recv'
    local func = box.func[name]
    if func == nil then
        return
    end
    if func.takes_raw_args then
        return
    end
    log.info("Upgrade %s to accept args as msgpack", name)
    box.begin()
    local ok, err = pcall(function()
        -- Preserve all the possible grants which could be given to the
        -- function. Don't just drop and re-create.
        local priv_space = box.space._priv
        local func_space = box.space._func
        local privs = priv_space.index.object:select{'function', func.id}
        local priv_pk_parts = priv_space.index.primary.parts

        -- Extract the old function.
        for _, p in pairs(privs) do
            priv_space:delete(util.tuple_extract_key(p, priv_pk_parts))
        end
        local tuple = func_space:delete(func.id)

        -- Insert the updated version. Can't update it in-place, _func doesn't
        -- support that.
        tuple = tuple:update({{'=', 'opts.takes_raw_args', true}})
        func_space:insert(tuple)
        for _, p in pairs(privs) do
            priv_space:insert(p)
        end
    end)
    if ok then
        ok, err = pcall(box.commit)
        if ok then
            return
        end
    end
    box.rollback()
    log.error("Couldn't upgrade %s to accept args as msgpack: %s", name, err)
end

--
-- Upgrade schema features which depend on Tarantool version and are not bound
-- to a certain vshard version.
--
local function schema_upgrade_core_features()
    -- In future it could make sense to store core feature set version in
    -- addition to vshard own schema version, but it would be an overkill while
    -- the feature is just one.
    schema_upgrade_bucket_recv_raw()
end

local function schema_current_version()
    local version = box.space._schema:get({'vshard_version'})
    if version == nil then
        return schema_version_make({0, 1, 15, 0})
    else
        return schema_version_make(version:totable(2))
    end
end

local schema_latest_version = schema_version_make({0, 1, 16, 0})

local function schema_upgrade_replica()
    local version = schema_current_version()
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
    if version ~= schema_latest_version then
        log.info('Replica\' vshard schema version is not latest - current '..
                 '%s vs latest %s, but the replica still can work', version,
                 schema_latest_version)
    end
    -- In future for hard changes the replica may be suspended
    -- until its schema is synced with master. Or it may
    -- reject to upgrade in case of incompatible changes. Now
    -- there are too few versions so as such problems could
    -- appear.
end

-- Every handler should be atomic. It is either applied whole, or
-- not applied at all. Atomic upgrade helps to downgrade in case
-- something goes wrong. At least by doing restart with the latest
-- successfully applied version. However, atomicity does not
-- prohibit yields, in case the upgrade, for example, affects huge
-- number of tuples (_bucket records, maybe).
local schema_upgrade_handlers = {
    {
        version = schema_version_make({0, 1, 16, 0}),
        upgrade = schema_upgrade_to_0_1_16_0,
        downgrade = schema_downgrade_from_0_1_16_0
    },
}

local function schema_upgrade_master(target_version, username, password)
    local _schema = box.space._schema
    local is_old_versioning = _schema:get({'oncevshard:storage:1'}) ~= nil
    local version = schema_current_version()
    local is_bootstrap = not box.space._bucket

    if is_bootstrap then
        schema_init_0_1_15_0(username, password)
    elseif is_old_versioning then
        log.info("The instance does not have 'vshard_version' record. "..
                 "It is 0.1.15.0.")
    end
    assert(schema_upgrade_handlers[#schema_upgrade_handlers].version ==
           schema_latest_version)
    local prev_version = version
    local ok, err1, err2
    local errinj = M.errinj.ERRINJ_UPGRADE
    for _, handler in pairs(schema_upgrade_handlers) do
        local next_version = handler.version
        if next_version > target_version then
            break
        end
        if next_version > version then
            log.info("Upgrade vshard schema to %s", next_version)
            if errinj == 'begin' then
                ok, err1 = false, 'Errinj in begin'
            else
                ok, err1 = pcall(handler.upgrade, username)
                if ok and errinj == 'end' then
                    ok, err1 = false, 'Errinj in end'
                end
            end
            if not ok then
                -- Rollback in case the handler started a
                -- transaction before the exception.
                box.rollback()
                log.info("Couldn't upgrade schema to %s: '%s'. Revert to %s",
                         next_version, err1, prev_version)
                ok, err2 = pcall(handler.downgrade)
                if not ok then
                    log.info("Couldn't downgrade schema to %s - fatal error: "..
                             "'%s'", prev_version, err2)
                    os.exit(-1)
                end
                error(err1)
            end
            ok, err1 = pcall(_schema.replace, _schema,
                             {'vshard_version', unpack(next_version)})
            if not ok then
                log.info("Upgraded schema to %s but couldn't update _schema "..
                         "'vshard_version' - fatal error: '%s'", next_version,
                         err1)
                os.exit(-1)
            end
            log.info("Successful vshard schema upgrade to %s", next_version)
        end
        prev_version = next_version
    end
end

local function schema_upgrade(is_first_cfg, is_master, username, password)
    if is_master then
        schema_upgrade_master(schema_latest_version, username, password)
        -- Core features can only change with Tarantool exe update, which means
        -- it won't happen on reconfig nor on hot reload. Enough to do it when
        -- configured first time.
        if is_first_cfg then
            schema_upgrade_core_features()
        end
    else
        return schema_upgrade_replica()
    end
end

local function this_is_master()
    return M.this_replicaset and M.this_replicaset.master and
           M.this_replica == M.this_replicaset.master
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
local function recovery_local_bucket_is_active(_, remote_bucket)
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
        total = total + 1
        local bucket_id = bucket.id
        if M.rebalancer_transfering_buckets[bucket_id] then
            goto continue
        end
        assert(bucket_is_transfer_in_progress(bucket))
        local peer_uuid = bucket.destination
        local destination = M.replicasets[peer_uuid]
        if not destination or not destination.master then
            -- No replicaset master for a bucket. Wait until it
            -- appears.
            if is_step_empty then
                log.info(start_format, type)
                log.warn('Can not find for bucket %s its peer %s', bucket_id,
                         peer_uuid)
                is_step_empty = false
            end
            goto continue
        end
        local remote_bucket, err =
            destination:callrw('vshard.storage.bucket_stat', {bucket_id})
        -- Check if it is not a bucket error, and this result can
        -- not be used to recovery anything. Try later.
        if not remote_bucket and (not err or err.type ~= 'ShardingError' or
                                  err.code ~= lerror.code.WRONG_BUCKET) then
            if is_step_empty then
                if err == nil then
                    err = 'unknown'
                end
                log.info(start_format, type)
                log.error('Error during recovery of bucket %s on replicaset '..
                          '%s: %s', bucket_id, peer_uuid, err)
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
                     bucket_id, bucket.status, remote_bucket.status, peer_uuid)
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
            lfiber.sleep(0.01)
            goto continue
        end
        is_all_recovered = true
        if bucket_generation_recovered == bucket_generation_current then
            goto sleep
        end

        service:set_activity('recovering sending')
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
            M.bucket_generation_cond:wait(sleep_time)
        end
    ::continue::
    end
end

local function recovery_f()
    assert(not M.recovery_service)
    local service = lservice_info.new('recovery')
    M.recovery_service = service
    pcall(recovery_service_f, service)
    assert(M.recovery_service == service)
    M.recovery_service = nil
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
    elseif M.this_replicaset.master ~= M.this_replica then
        local master_uuid = M.this_replicaset.master
        if master_uuid then
            master_uuid = master_uuid.uuid
        end
        return bucket, lerror.vshard(lerror.code.NON_MASTER,
                                     M.this_replica.uuid,
                                     M.this_replicaset.uuid, master_uuid)
    else
        return bucket
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
-- @param from Source UUID.
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
    M.rebalancer_transfering_buckets[bucket_id] = true
    local status, ret, err = pcall(bucket_recv_xc, bucket_id, from, data, opts)
    M.rebalancer_transfering_buckets[bucket_id] = nil
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
-- Find spaces with index having the specified (in cfg) name.
-- The function result is cached using `schema_version`.
-- @retval Map of type {space_id = <space object>}.
--
local sharded_spaces_cache_schema_version = nil
local sharded_spaces_cache = nil
local function find_sharded_spaces()
    local schema_version = util.schema_version()
    if sharded_spaces_cache_schema_version == schema_version then
        return sharded_spaces_cache
    end
    local spaces = {}
    local idx = M.shard_index
    for k, space in pairs(box.space) do
        if type(k) == 'number' and space.index[idx] ~= nil then
            local parts = space.index[idx].parts
            local p = parts[1].type
            if p == 'unsigned' or p == 'integer' or p == 'number' then
                spaces[k] = space
            end
        end
    end
    sharded_spaces_cache_schema_version = schema_version
    sharded_spaces_cache = spaces
    return spaces
end

--
-- Public wrapper for sharded spaces list getter.
--
local function storage_sharded_spaces()
    return table.deepcopy(find_sharded_spaces())
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
    local spaces = find_sharded_spaces()
    local idx = M.shard_index
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
-- The only thing, that must be done to abort a master demote is
-- a reset of read_only.
--
local function local_on_master_disable_abort()
    if not M.current_cfg or M.current_cfg.read_only == nil then
        box.cfg{read_only = false}
    end
end

--
-- Prepare to a master demotion. Before it, a master must stop
-- accept writes, and try to wait until all of its data is
-- replicated to each slave.
--
local function local_on_master_disable_prepare()
    log.info("Resigning from the replicaset master role...")
    if not M.current_cfg or M.current_cfg.read_only == nil then
        box.cfg({read_only = true})
        sync(M.sync_timeout)
    end
end

--
-- This function executes when a master role is removed from local
-- instance during configuration
--
local function local_on_master_disable()
    M._on_master_disable:run()
    -- Stop garbage collecting
    if M.collect_bucket_garbage_fiber ~= nil then
        M.collect_bucket_garbage_fiber:cancel()
        M.collect_bucket_garbage_fiber = nil
        log.info("GC stopped")
    end
    if M.recovery_fiber ~= nil then
        M.recovery_fiber:cancel()
        M.recovery_fiber = nil
        log.info('Recovery stopped')
    end
    log.info("Resigned from the replicaset master role")
end

--
-- The only thing, that must be done to abort a master promotion
-- is a set read_only back to true.
--
local function local_on_master_enable_abort()
    if not M.current_cfg or M.current_cfg.read_only == nil then
        box.cfg({read_only = true})
    end
end

--
-- Promote does not require sync, because a replica can not have a
-- data, that is not on a current master - the replica is read
-- only. But read_only can not be set to false here, because
-- until box.cfg is called, it can not be guaranteed, that the
-- promotion will be successful.
--
local function local_on_master_enable_prepare()
    log.info("Taking on replicaset master role...")
end
--
-- This function executes whan a master role is added to local
-- instance during configuration
--
local function local_on_master_enable()
    if not M.current_cfg or M.current_cfg.read_only == nil then
        box.cfg({read_only = false})
    end
    M._on_master_enable:run()
    -- Start background process to collect garbage.
    M.collect_bucket_garbage_fiber =
        util.reloadable_fiber_create('vshard.gc', M, 'gc_bucket_f')
    M.recovery_fiber =
        util.reloadable_fiber_create('vshard.recovery', M, 'recovery_f')
    -- TODO: check current status
    log.info("Took on replicaset master role")
end

--
-- Send a bucket to other replicaset.
--
local function bucket_send_xc(bucket_id, destination, opts, exception_guard)
    local uuid = box.info.cluster.uuid
    local status, ok
    local ref, err = bucket_refrw_touch(bucket_id)
    if not ref then
        return nil, err
    end
    ref.rw_lock = true
    exception_guard.ref = ref
    exception_guard.drop_rw_lock = true
    local timeout = opts and opts.timeout or consts.DEFAULT_BUCKET_SEND_TIMEOUT
    local deadline = fiber_clock() + timeout
    while ref.rw ~= 0 do
        timeout = deadline - fiber_clock()
        if not M.bucket_rw_lock_is_ready_cond:wait(timeout) then
            return nil, lerror.timeout()
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
    if destination == uuid then
        return nil, lerror.vshard(lerror.code.MOVE_TO_SELF, bucket_id, uuid)
    end
    local data = {}
    local spaces = find_sharded_spaces()
    local limit = consts.BUCKET_CHUNK_SIZE
    local idx = M.shard_index
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
                status, err = replicaset:callrw('vshard.storage.bucket_recv',
                                                {bucket_id, uuid, data}, opts)
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
    status, err = replicaset:callrw('vshard.storage.bucket_recv',
                                    {bucket_id, uuid, data}, opts)
    if not status then
        return status, lerror.make(err)
    end
    -- Always send at least two messages to prevent the case, when
    -- a bucket is sent, hung in the network. Then it is recovered
    -- to active on the source, and then the message arrives and
    -- the same bucket is activated on the destination.
    status, err = replicaset:callrw('vshard.storage.bucket_recv',
                                    {bucket_id, uuid, {}, {is_last = true}},
                                    opts)
    if not status then
        return status, lerror.make(err)
    end
    _bucket:replace({bucket_id, consts.BUCKET.SENT, destination})
    return true
end

--
-- Exception and recovery safe version of bucket_send_xc.
--
local function bucket_send(bucket_id, destination, opts)
    if type(bucket_id) ~= 'number' or type(destination) ~= 'string' then
        error('Usage: bucket_send(bucket_id, destination)')
    end
    M.rebalancer_transfering_buckets[bucket_id] = true
    local exception_guard = {}
    local status, ret, err = pcall(bucket_send_xc, bucket_id, destination, opts,
                                   exception_guard)
    if exception_guard.drop_rw_lock then
        exception_guard.ref.rw_lock = false
    end
    M.rebalancer_transfering_buckets[bucket_id] = nil
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
    local bucket_index = space.index[M.shard_index]
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
    local sharded_spaces = find_sharded_spaces()
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
        except = M.this_replica.uuid,
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
    for _, b in _bucket.index.status:pairs(consts.BUCKET.SENT) do
        i = i + 1
        local bid = b.id
        local ref = M.bucket_refs[bid]
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
            lfiber.sleep(0.01)
            goto continue
        end
        if bucket_generation_collected ~= bucket_generation_current then
            service:set_activity('gc garbage')
            status, err = gc_bucket_drop(consts.BUCKET.GARBAGE, route_map)
            if status then
                service:set_activity('gc sent')
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
            M.bucket_generation_cond:wait(sleep_time)
        end
    ::continue::
    end
end

local function gc_bucket_f()
    assert(not M.gc_service)
    local service = lservice_info.new('gc')
    M.gc_service = service
    pcall(gc_bucket_service_f, service)
    assert(M.gc_service == service)
    M.gc_service = nil
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
    local sharded_spaces = find_sharded_spaces()
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
--     uuid = {bucket_count = number, weight = number},
--     ...
-- }
--
-- @retval Maximal disbalance over all replicasets, and UUID of
--        a replicaset having it.
--
local function rebalancer_calculate_metrics(replicasets)
    local max_disbalance = 0
    local max_disbalance_uuid
    for uuid, replicaset in pairs(replicasets) do
        local needed = replicaset.etalon_bucket_count - replicaset.bucket_count
        if replicaset.etalon_bucket_count ~= 0 then
            local disbalance =
                math.abs(needed) / replicaset.etalon_bucket_count * 100
            if disbalance > max_disbalance then
                max_disbalance = disbalance
                max_disbalance_uuid = uuid
            end
        elseif replicaset.bucket_count ~= 0 then
            max_disbalance = math.huge
            max_disbalance_uuid = uuid
        end
        assert(needed >= 0 or -needed <= replicaset.bucket_count)
        replicaset.needed = needed
    end
    return max_disbalance, max_disbalance_uuid
end

--
-- Move @a needed bucket count from a pool to @a dst_uuid and
-- remember the route in @a bucket_routes table.
--
local function rebalancer_take_buckets_from_pool(bucket_pool, bucket_routes,
                                                 dst_uuid, needed)
    local to_remove_from_pool = {}
    for src_uuid, bucket_count in pairs(bucket_pool) do
        local count = math.min(bucket_count, needed)
        local src = bucket_routes[src_uuid]
        if src == nil then
            bucket_routes[src_uuid] = {[dst_uuid] = count}
        else
            local dst = src[dst_uuid]
            if dst == nil then
                src[dst_uuid] = count
            else
                src[dst_uuid] = dst + count
            end
        end
        local new_count = bucket_pool[src_uuid] - count
        needed = needed - count
        bucket_pool[src_uuid] = new_count
        if new_count == 0 then
            table.insert(to_remove_from_pool, src_uuid)
        end
        if needed == 0 then
            break
        end
    end
    for _, src_uuid in pairs(to_remove_from_pool) do
        bucket_pool[src_uuid] = nil
    end
end

--
-- Build a table with routes defining from which node to which one
-- how many buckets should be moved to reach the best balance in
-- a cluster.
-- @param replicasets Map of type: {
--     uuid = {bucket_count = number, weight = number,
--             needed = number},
--     ...
-- }      This parameter is a result of
--        rebalancer_calculate_metrics().
--
-- @retval Bucket routes. It is a map of type: {
--     src_uuid = {
--         dst_uuid = number, -- Bucket count to move from
--                               src to dst.
--         ...
--     },
--     ...
-- }
--
local function rebalancer_build_routes(replicasets)
    -- Map of type: {
    --     uuid = number, -- free buckets of uuid.
    -- }
    local bucket_pool = {}
    for uuid, replicaset in pairs(replicasets) do
        if replicaset.needed < 0 then
            bucket_pool[uuid] = -replicaset.needed
            replicaset.needed = 0
        end
    end
    local bucket_routes = {}
    for uuid, replicaset in pairs(replicasets) do
        if replicaset.needed > 0 then
            rebalancer_take_buckets_from_pool(bucket_pool, bucket_routes, uuid,
                                              replicaset.needed)
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
    for uuid, bucket_count in pairs(routes) do
        local new = {
            -- Receiver's UUID.
            uuid = uuid,
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
        map[uuid] = new
        rlist:add_tail(new)
    end
    return {
        rlist = rlist,
        map = map,
        -- Error, which occurred in `bucket_send` and led to
        -- skipping of the above-mentioned uuid from dispenser.
        error = nil,
    }
end

--
-- Put one bucket back to the dispenser. It happens, if the worker
-- receives a throttle error. This is the only error that can be
-- tolerated.
--
local function route_dispenser_put(dispenser, uuid)
    local dst = dispenser.map[uuid]
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
local function route_dispenser_skip(dispenser, uuid)
    local map = dispenser.map
    local dst = map[uuid]
    if dst then
        map[uuid] = nil
        dispenser.rlist:remove(dst)
    end
end

--
-- Set that the receiver @a uuid was throttled. When it happens
-- first time it is logged.
--
local function route_dispenser_throttle(dispenser, uuid)
    local dst = dispenser.map[uuid]
    if dst then
        local old_value = dst.is_throttle_warned
        dst.is_throttle_warned = true
        return not old_value
    end
    return false
end

--
-- Notify the dispenser that a bucket was successfully sent to
-- @a uuid. It has no any functional purpose except tracking
-- progress.
--
local function route_dispenser_sent(dispenser, uuid)
    local dst = dispenser.map[uuid]
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
        return dst.uuid
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
    local uuid = route_dispenser_pop(dispenser)
    local worker_throttle_count = 0
    local bucket_id, is_found
    while uuid do
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
        local ret, err = bucket_send(bucket_id, uuid, opts)
        if ret then
            worker_throttle_count = 0
            local finished, total = route_dispenser_sent(dispenser, uuid)
            if finished then
                log.info('%d buckets were successfully sent to %s', total, uuid)
            end
            goto continue
        end
        route_dispenser_put(dispenser, uuid)
        if err.type ~= 'ShardingError' or
           err.code ~= lerror.code.TOO_MANY_RECEIVING then
            log.error('Error during rebalancer routes applying: receiver %s, '..
                      'error %s', uuid, err)
            log.info('Can not finish transfers to %s, skip to next round', uuid)
            worker_throttle_count = 0
            dispenser.error = dispenser.error or err
            route_dispenser_skip(dispenser, uuid)
            goto continue
        end
        worker_throttle_count = worker_throttle_count + 1
        if route_dispenser_throttle(dispenser, uuid) then
            log.error('Too many buckets is being sent to %s', uuid)
        end
        if worker_throttle_count < dispenser.rlist.count then
            goto continue
        end
        log.info('The worker was asked for throttle %d times in a row. '..
                 'Sleep for %d seconds', worker_throttle_count,
                 consts.REBALANCER_WORK_INTERVAL)
        worker_throttle_count = 0
        if not quit_cond:wait(consts.REBALANCER_WORK_INTERVAL) then
            log.info('The worker is back')
        end
::continue::
        uuid = route_dispenser_pop(dispenser)
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
    for uuid, dst in pairs(dispenser.map) do
        if dst.is_throttle_warned then
            table.insert(throttled, uuid)
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
    pcall(rebalancer_service_apply_routes_f, service, routes)
    -- Delay service destruction in order to check states and errors
    while M.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY do
        lfiber.sleep(0.001)
    end
    assert(M.routes_applier_service == service)
    M.routes_applier_service = nil
end

--
-- Apply routes table of type: {
--     dst_uuid = number, -- Bucket count to send.
--     ...
-- }. Is used by a rebalancer.
--
local function rebalancer_apply_routes(routes)
    if is_this_replicaset_locked() then
        return false, lerror.vshard(lerror.code.REPLICASET_IS_LOCKED);
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
    for uuid, replicaset in pairs(M.replicasets) do
        local state =
            replicaset:callrw('vshard.storage.rebalancer_request_state', {})
        if state == nil then
            return
        end
        local bucket_count = state.bucket_active_count +
                             state.bucket_pinned_count
        if replicaset.lock then
            total_bucket_locked_count = total_bucket_locked_count + bucket_count
        else
            total_bucket_active_count = total_bucket_active_count + bucket_count
            replicasets[uuid] = {bucket_count = bucket_count,
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
-- smallest replicaset uuid and which is master.
--
local function rebalancer_service_f(service)
    local module_version = M.module_version
    while module_version == M.module_version do
        service:next_iter()
        while not M.is_rebalancer_active do
            log.info('Rebalancer is disabled. Sleep')
            M.rebalancer_service:set_activity('disabled')
            lfiber.sleep(consts.REBALANCER_IDLE_INTERVAL)
        end
        service:set_activity('downloading states')
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
            lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
            goto continue
        end
        lreplicaset.calculate_etalon_balance(replicasets,
                                             total_bucket_active_count)
        local max_disbalance, max_disbalance_uuid =
            rebalancer_calculate_metrics(replicasets)
        local threshold = M.rebalancer_disbalance_threshold
        if max_disbalance <= threshold then
            local balance_msg
            if max_disbalance > 0 then
                local rep = replicasets[max_disbalance_uuid]
                balance_msg = string.format(
                    'The cluster is balanced ok with max disbalance %f%% at '..
                    '"%s": etalon bucket count is %d, stored count is %d. '..
                    'The disbalance is smaller than your threshold %f%%, '..
                    'nothing to do.', max_disbalance, max_disbalance_uuid,
                    rep.etalon_bucket_count, rep.bucket_count, threshold)
            else
                balance_msg = 'The cluster is balanced ok.'
            end
            log.info('%s Schedule next rebalancing after %f seconds',
                     balance_msg, consts.REBALANCER_IDLE_INTERVAL)
            service:set_status_ok()
            service:set_activity('idling')
            lfiber.sleep(consts.REBALANCER_IDLE_INTERVAL)
            goto continue
        end
        local routes = rebalancer_build_routes(replicasets)
        -- Routes table can not be empty. If it had been empty,
        -- then max_disbalance would have been calculated
        -- incorrectly.
        assert(next(routes) ~= nil)
        for src_uuid, src_routes in pairs(routes) do
            service:set_activity('applying routes')
            local rs = M.replicasets[src_uuid]
            local status, err =
                rs:callrw('vshard.storage.rebalancer_apply_routes',
                          {src_routes})
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
        lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
::continue::
    end
end

local function rebalancer_f()
    assert(not M.rebalancer_service)
    local service = lservice_info.new('rebalancer')
    M.rebalancer_service = service
    pcall(rebalancer_service_f, service)
    assert(M.rebalancer_service == service)
    M.rebalancer_service = nil
end

--
-- Check all buckets of the host storage to have SENT or ACTIVE
-- state, return active bucket count.
-- @retval not nil Count of active buckets.
-- @retval     nil Not SENT or not ACTIVE buckets were found.
--
local function rebalancer_request_state()
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
-- Configuration
--------------------------------------------------------------------------------

local function storage_cfg(cfg, this_replica_uuid, is_reload)
    if this_replica_uuid == nil then
        error('Usage: cfg(configuration, this_replica_uuid)')
    end
    local is_first_cfg = not M.current_cfg
    cfg = lcfg.check(cfg, M.current_cfg)
    local vshard_cfg, box_cfg = lcfg.split(cfg)
    if vshard_cfg.weights or vshard_cfg.zone then
        error('Weights and zone are not allowed for storage configuration')
    end
    if M.replicasets then
        log.info("Starting reconfiguration of replica %s", this_replica_uuid)
    else
        log.info("Starting configuration of replica %s", this_replica_uuid)
    end

    local was_master = M.this_replicaset ~= nil and
                       M.this_replicaset.master == M.this_replica

    local this_replicaset
    local this_replica
    local new_replicasets = lreplicaset.buildall(vshard_cfg)
    local min_master
    for _, rs in pairs(new_replicasets) do
        for replica_uuid, replica in pairs(rs.replicas) do
            if (min_master == nil or replica_uuid < min_master.uuid) and
               rs.master == replica then
                min_master = replica
            end
            if replica_uuid == this_replica_uuid then
                assert(this_replicaset == nil)
                this_replicaset = rs
                this_replica = replica
            end
        end
    end
    if this_replicaset == nil then
        error(string.format("Local replica %s wasn't found in config",
                            this_replica_uuid))
    end
    local is_master = this_replicaset.master == this_replica
    if is_master then
        log.info('I am master')
    end

    -- It is considered that all possible errors during cfg
    -- process occur only before this place.
    -- This check should be placed as late as possible.
    if M.errinj.ERRINJ_CFG then
        error('Error injection: cfg')
    end

    --
    -- Sync timeout is a special case - it must be updated before
    -- all other options to allow a user to demote a master with
    -- a new sync timeout.
    --
    local old_sync_timeout = M.sync_timeout
    M.sync_timeout = vshard_cfg.sync_timeout

    if was_master and not is_master then
        local_on_master_disable_prepare()
    end
    if not was_master and is_master then
        local_on_master_enable_prepare()
    end

    if not is_reload then
        -- Do not change 'read_only' option here - if a master is
        -- disabled and there are triggers on master disable, then
        -- they would not be able to modify anything, if 'read_only'
        -- had been set here. 'Read_only' is set in
        -- local_on_master_disable after triggers and is unset in
        -- local_on_master_enable before triggers.
        --
        -- If a master role of the replica is not changed, then
        -- 'read_only' can be set right here.
        local this_replicaset_cfg = vshard_cfg.sharding[this_replicaset.uuid]
        local this_replica_cfg = this_replicaset_cfg.replicas[this_replica.uuid]

        if box_cfg.listen == nil then
            box_cfg.listen = this_replica_cfg.listen
            if box_cfg.listen == nil then
                box_cfg.listen = this_replica_cfg.uri
            end
        end
        if not box_cfg.replication then
            box_cfg.replication = {}
            for _, replica in pairs(this_replicaset.replicas) do
                table.insert(box_cfg.replication, replica.uri)
            end
        end
        if was_master == is_master and box_cfg.read_only == nil then
            box_cfg.read_only = not is_master
        end
        if type(box.cfg) == 'function' then
            box_cfg.instance_uuid = box_cfg.instance_uuid or this_replica.uuid
            box_cfg.replicaset_uuid = box_cfg.replicaset_uuid or
                                      this_replicaset.uuid
        else
            local info = box.info
            if this_replica_uuid ~= info.uuid then
                error(string.format('Instance UUID mismatch: already set ' ..
                                    '"%s" but "%s" in arguments', info.uuid,
                                    this_replica_uuid))
            end
            if this_replicaset.uuid ~= info.cluster.uuid then
                error(string.format('Replicaset UUID mismatch: already set ' ..
                                    '"%s" but "%s" in vshard config',
                                    info.cluster.uuid, this_replicaset.uuid))
            end
        end
        local ok, err = pcall(box.cfg, box_cfg)
        while M.errinj.ERRINJ_CFG_DELAY do
            lfiber.sleep(0.01)
        end
        if not ok then
            M.sync_timeout = old_sync_timeout
            if was_master and not is_master then
                local_on_master_disable_abort()
            end
            if not was_master and is_master then
                local_on_master_enable_abort()
            end
            error(err)
        end
        log.info("Box has been configured")
    end

    local uri = luri.parse(this_replica.uri)
    schema_upgrade(is_first_cfg, is_master, uri.login, uri.password)

    -- Check for master specifically. On master _bucket space must exist.
    -- Because it should have done the schema bootstrap. Shall not ever try to
    -- do anything delayed.
    if is_master or box.space._bucket then
        schema_install_triggers()
    else
        schema_install_triggers_delayed()
    end

    lref.cfg()
    lsched.cfg(vshard_cfg)

    lreplicaset.rebind_replicasets(new_replicasets, M.replicasets)
    lreplicaset.outdate_replicasets(M.replicasets)
    M.replicasets = new_replicasets
    M.this_replicaset = this_replicaset
    M.this_replica = this_replica
    M.total_bucket_count = vshard_cfg.bucket_count
    M.rebalancer_disbalance_threshold =
        vshard_cfg.rebalancer_disbalance_threshold
    M.rebalancer_receiving_quota = vshard_cfg.rebalancer_max_receiving
    M.shard_index = vshard_cfg.shard_index
    M.rebalancer_worker_count = vshard_cfg.rebalancer_max_sending
    M.current_cfg = cfg

    if was_master and not is_master then
        local_on_master_disable()
    end

    if not was_master and is_master then
        local_on_master_enable()
    end

    if min_master == this_replica then
        if not M.rebalancer_fiber then
            M.rebalancer_fiber =
                util.reloadable_fiber_create('vshard.rebalancer', M,
                                             'rebalancer_f')
        else
            log.info('Wakeup rebalancer')
            -- Configuration had changed. Time to rebalance.
            M.rebalancer_fiber:wakeup()
        end
    elseif M.rebalancer_fiber then
        log.info('Rebalancer location has changed to %s', min_master)
        M.rebalancer_fiber:cancel()
        M.rebalancer_fiber = nil
    end
    M.is_configured = true
    -- Destroy connections, not used in a new configuration.
    collectgarbage()
end

local function storage_cfg_fiber_safe(cfg, this_replica_uuid, is_reload)
    if M.is_cfg_in_progress then
        return error(lerror.vshard(lerror.code.STORAGE_CFG_IS_IN_PROGRESS))
    end

    M.is_cfg_in_progress = true
    local ok, err = pcall(storage_cfg, cfg, this_replica_uuid, is_reload)
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
    local this_uuid = M.this_replicaset.uuid
    local this_master = M.this_replicaset.master
    if this_master == nil then
        table.insert(state.alerts, alert(code.MISSING_MASTER, this_uuid))
        state.status = math.max(state.status, consts.STATUS.ORANGE)
    end
    if this_master and this_master ~= M.this_replica then
        for _, replica in pairs(box.info.replication) do
            if replica.uuid ~= this_master.uuid then
                goto cont
            end
            state.replication.status = replica.upstream.status
            if replica.upstream.status ~= 'follow' then
                state.replication.idle = replica.upstream.idle
                table.insert(state.alerts, alert(code.UNREACHABLE_MASTER,
                                                 this_uuid,
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
    elseif this_master then
        state.replication.status = 'master'
        local replica_count = 0
        local not_available_replicas = 0
        for _, replica in pairs(box.info.replication) do
            if replica.uuid ~= M.this_replica.uuid then
                replica_count = replica_count + 1
                if replica.downstream == nil or
                   replica.downstream.vclock == nil then
                    table.insert(state.alerts, alert(code.UNREACHABLE_REPLICA,
                                                     replica.uuid))
                    state.status = math.max(state.status, consts.STATUS.YELLOW)
                    not_available_replicas = not_available_replicas + 1
                end
            end
        end
        local available_replicas = replica_count - not_available_replicas
        if replica_count > 0 and available_replicas == 0 then
            table.insert(state.alerts, alert(code.UNREACHABLE_REPLICASET,
                                             this_uuid))
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
    for uuid, replicaset in pairs(M.replicasets) do
        local master = replicaset.master
        if not master then
            ireplicasets[uuid] = {uuid = uuid, master = 'missing'}
        else
            local uri = master:safe_uri()
            local conn = master.conn
            ireplicasets[uuid] = {
                uuid = uuid;
                master = {
                    uri = uri, uuid = conn and conn.peer_uuid,
                    state = conn and conn.state, error = conn and conn.error,
                };
            };
        end
    end
    state.replicasets = ireplicasets
    if opts and opts.with_services then
        state.services = {
            gc = M.gc_service and M.gc_service:info(),
            rebalancer = M.rebalancer_service and M.rebalancer_service:info(),
            recovery = M.recovery_service and M.recovery_service:info(),
            routes_applier = M.routes_applier_service and
                M.routes_applier_service:info(),
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
        storage_cfg_fiber_safe(M.current_cfg, M.this_replica.uuid, true)
    end
    M.module_version = M.module_version + 1
    -- Background fibers could sleep waiting for bucket changes.
    -- Let them know it is time to reload.
    bucket_generation_increment()
end

M.recovery_f = recovery_f
M.rebalancer_f = rebalancer_f
M.gc_bucket_f = gc_bucket_f

--
-- These functions are saved in M not for atomic reload, but for
-- unit testing.
--
M.gc_bucket_drop = gc_bucket_drop
M.rebalancer_build_routes = rebalancer_build_routes
M.rebalancer_calculate_metrics = rebalancer_calculate_metrics
M.cached_find_sharded_spaces = find_sharded_spaces
M.route_dispenser = {
    create = route_dispenser_create,
    put = route_dispenser_put,
    throttle = route_dispenser_throttle,
    skip = route_dispenser_skip,
    pop = route_dispenser_pop,
    sent = route_dispenser_sent,
}
M.schema_latest_version = schema_latest_version
M.schema_current_version = schema_current_version
M.schema_upgrade_master = schema_upgrade_master
M.schema_upgrade_handlers = schema_upgrade_handlers
M.schema_version_make = schema_version_make
M.schema_bootstrap = schema_init_0_1_15_0
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
    cfg = function(cfg, uuid) return storage_cfg_fiber_safe(cfg, uuid, false) end,
    on_master_enable = on_master_enable,
    on_master_disable = on_master_disable,
    on_bucket_event = on_bucket_event,
    enable = storage_enable,
    disable = storage_disable,
    internal = M,
}
