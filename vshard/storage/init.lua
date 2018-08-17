local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local netbox = require('net.box') -- for net.box:self()
local trigger = require('internal.trigger')
local ffi = require('ffi')
local table_new = require('table.new')

local MODULE_INTERNALS = '__module_vshard_storage'
-- Reload requirements, in case this module is reloaded manually.
if rawget(_G, MODULE_INTERNALS) then
    local vshard_modules = {
        'vshard.consts', 'vshard.error', 'vshard.cfg',
        'vshard.replicaset', 'vshard.util',
        'vshard.storage.reload_evolution',
        'vshard.lua_gc',
    }
    for _, module in pairs(vshard_modules) do
        package.loaded[module] = nil
    end
end
local consts = require('vshard.consts')
local lerror = require('vshard.error')
local lcfg = require('vshard.cfg')
local lreplicaset = require('vshard.replicaset')
local util = require('vshard.util')
local lua_gc = require('vshard.lua_gc')
local reload_evolution = require('vshard.storage.reload_evolution')

ffi.cdef[[
    typedef struct box_key_def_t box_key_def_t;

    struct key_def *
    space_index_cmp_def(struct space *space, uint32_t id);

    struct space *
    space_by_id(uint32_t id);

    int
    box_tuple_compare(const box_tuple_t *tuple_a, const box_tuple_t *tuple_b,
                      const box_key_def_t *key_def);
]]

local builtin = ffi.C

local M = rawget(_G, MODULE_INTERNALS)
if not M then
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
        -- Index which is a trigger to shard its space by numbers in
        -- this index. It must have at first part either unsigned,
        -- or integer or number type and be not nullable. Values in
        -- this part are considered as bucket identifiers.
        shard_index = nil,
        -- Bucket count stored on all replicasets.
        total_bucket_count = 0,
        errinj = {
            ERRINJ_CFG = false,
            ERRINJ_BUCKET_FIND_GARBAGE_DELAY = false,
            ERRINJ_RELOAD = false,
            ERRINJ_CFG_DELAY = false,
            ERRINJ_LONG_RECEIVE = false,
            ERRINJ_RECEIVE_PARTIALLY = false,
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
        --
        -- Incremental generation of the _bucket space. It is
        -- incremented on each _bucket change and is used to
        -- detect that _bucket was not changed between yields.
        --
        bucket_generation = 0,

        ------------------- Garbage collection -------------------
        -- Fiber to remove garbage buckets data.
        collect_bucket_garbage_fiber = nil,
        -- Do buckets garbage collection once per this time.
        collect_bucket_garbage_interval = nil,
        -- Boolean lua_gc state (create periodic gc task).
        collect_lua_garbage = nil,

        -------------------- Bucket recovery ---------------------
        -- Bucket identifiers which are not active and are not being
        -- sent - their status is unknown. Their state must be checked
        -- periodically in recovery fiber.
        buckets_to_recovery = {},
        recovery_fiber = nil,

        ----------------------- Rebalancer -----------------------
        -- Fiber to rebalance a cluster.
        rebalancer_fiber = nil,
        -- Fiber which applies routes one by one. Its presense and
        -- active status means that the rebalancing is in progress
        -- now on the current node.
        rebalancer_applier_fiber = nil,
        -- Internal flag to activate and deactivate rebalancer. Mostly
        -- for tests.
        is_rebalancer_active = true,
        -- Maximal allowed percent deviation of bucket count on a
        -- replicaset from etalon bucket count.
        rebalancer_disbalance_threshold = 0,
        -- Maximal bucket count that can be received by a single
        -- replicaset simultaneously.
        rebalancer_max_receiving = 0,
        -- Identifier of a bucket that rebalancer is sending or
        -- receiving now. If a bucket has state SENDING/RECEIVING,
        -- but its id is not stored here, it means, that its
        -- transfer has failed.
        rebalancer_transfering_buckets = {},

        ------------------------- Reload -------------------------
        -- Version of the loaded module. This number is used on
        -- reload to determine which upgrade scripts to run.
        reload_version = reload_evolution.version,
    }
end

--
-- Trigger for on replace into _bucket to update its generation.
--
local function bucket_generation_increment()
    M.bucket_generation = M.bucket_generation + 1
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
           bucket.status == consts.BUCKET.PINNED or
           (bucket.status == consts.BUCKET.SENDING and
            M.rebalancer_transfering_buckets[bucket.id])
end

--
-- Check if @a bucket can accept 'read' requests.
--
local function bucket_is_readable(bucket)
    return bucket.status == consts.BUCKET.ACTIVE or
           bucket.status == consts.BUCKET.PINNED or
           bucket.status == consts.BUCKET.SENDING
end

--
-- Check if a bucket is sending or receiving.
--
local function bucket_is_transfer_in_progress(bucket)
    return bucket.status == consts.BUCKET.SENDING or
           bucket.status == consts.BUCKET.RECEIVING
end

--
-- Check if @a bucket is garbage. It is true for
-- * sent buckets;
-- * buckets explicitly marked to be a garbage.
--
local function bucket_is_garbage(bucket)
    return bucket.status == consts.BUCKET.SENT or
           bucket.status == consts.BUCKET.GARBAGE
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
                              status, b.status)
            error(lerror.vshard(lerror.code.WRONG_BUCKET, bucket_id, msg, nil))
        end
        return M.bucket_generation
    end
    return bucket_generation
end

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------
local function storage_schema_v1(username, password)
    log.info("Initializing schema")
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

    box.snapshot()
end

local function this_is_master()
    return M.this_replicaset and M.this_replicaset.master and
           M.this_replica == M.this_replicaset.master
end

local function on_master_disable(...)
    M._on_master_disable(...)
    -- If a trigger is set after storage.cfg(), then notify an
    -- user, that the current instance is not master.
    if select('#', ...) == 1 and not this_is_master() then
        M._on_master_disable:run()
    end
end

local function on_master_enable(...)
    M._on_master_enable(...)
    -- Same as above, but notify, that the instance is master.
    if select('#', ...) == 1 and this_is_master() then
        M._on_master_enable:run()
    end
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
-- Check if a local bucket can be deleted.
--
local function recovery_local_bucket_is_garbage(local_bucket, remote_bucket)
    return remote_bucket and bucket_is_writable(remote_bucket)
end

--
-- Check if a local bucket can become active.
--
local function recovery_local_bucket_is_active(local_bucket, remote_bucket)
    if not remote_bucket or bucket_is_garbage(remote_bucket) then
        return true
    end
    if remote_bucket.status == consts.BUCKET.RECEIVING and
       not remote_bucket.is_transfering and
       local_bucket.status == consts.BUCKET.SENDING then
        assert(not M.rebalancer_transfering_buckets[local_bucket.id])
        return true
    end
    return false
end

--
-- Check status of each bucket scheduled for recovery. Resolve
-- status where possible.
--
local function recovery_step()
    -- Buckets to became garbage. It is such buckets, that they
    -- are in 'sending' state here, but 'active' on another
    -- replicaset.
    local garbage = {}
    -- Bucket becames active, if it is sending here and is not
    -- active on destination replicaset.
    local active = {}
    -- If a bucket status is resolved, it is deleted from
    -- buckets_to_recovery map.
    local recovered = {}
    local _bucket = box.space._bucket
    local is_empty = true
    for bucket_id, _ in pairs(M.buckets_to_recovery) do
        if M.rebalancer_transfering_buckets[bucket_id] then
            goto continue
        end
        if is_empty then
            log.info('Starting buckets recovery step')
        end
        is_empty = false
        local bucket = _bucket:get{bucket_id}
        if not bucket or not bucket_is_transfer_in_progress(bucket) then
            -- Possibly, a bucket was deleted or recovered by
            -- an admin. Or recovery_f started not after
            -- bootstrap, but after master change - in such a case
            -- there can be receiving buckets, which are ok.
            table.insert(recovered, bucket_id)
            goto continue
        end
        local destination = M.replicasets[bucket.destination]
        if not destination or not destination.master then
            -- No replicaset master for a bucket. Wait until it
            -- appears.
            goto continue
        end
        local remote_bucket, err =
            destination:callrw('vshard.storage.bucket_stat', {bucket_id})
        -- Check if it is not a bucket error, and this result can
        -- not be used to recovery anything. Try later.
        if not remote_bucket and (not err or err.type ~= 'ShardingError' or
                                  err.code ~= lerror.code.WRONG_BUCKET) then
            goto continue
        end
        if recovery_local_bucket_is_garbage(bucket, remote_bucket) then
            table.insert(recovered, bucket_id)
            table.insert(garbage, bucket_id)
        elseif recovery_local_bucket_is_active(bucket, remote_bucket) then
            table.insert(recovered, bucket_id)
            table.insert(active, bucket_id)
        end
::continue::
    end
    if not is_empty then
        log.info('Finish bucket recovery step')
    end
    if #active > 0 or #garbage > 0 then
        box.begin()
        for _, id in pairs(active) do
            _bucket:update({id}, {{'=', 2, consts.BUCKET.ACTIVE}})
        end
        for _, id in pairs(garbage) do
            _bucket:update({id}, {{'=', 2, consts.BUCKET.GARBAGE}})
        end
        box.commit()
    end
    for _, id in pairs(recovered) do
        M.buckets_to_recovery[id] = nil
    end
end

--
-- Infinite function to resolve status of buckets, whose 'sending'
-- has failed due to tarantool or network problems. Restarts on
-- reload.
-- @param module_version Module version, on which the current
--        function had been started. If the actual module version
--        appears to be changed, then stop recovery. It is
--        restarted in reloadable_fiber.
--
local function recovery_f()
    local module_version = M.module_version
    local _bucket = box.space._bucket
    M.buckets_to_recovery = {}
    for _, bucket in _bucket.index.status:pairs({consts.BUCKET.SENDING}) do
        M.buckets_to_recovery[bucket.id] = true
    end
    for _, bucket in _bucket.index.status:pairs({consts.BUCKET.RECEIVING}) do
        M.buckets_to_recovery[bucket.id] = true
    end
    -- Interrupt recovery if a module has been reloaded. Perhaps,
    -- there was found a bug, and reload fixes it.
    while module_version == M.module_version do
        local ok, err = pcall(recovery_step)
        if not ok then
            box.rollback()
            log.error('Error during buckets recovery: %s', err)
        end
        lfiber.sleep(consts.RECOVERY_INTERVAL)
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

local function sync(timeout)
    if timeout ~= nil and type(timeout) ~= 'number' then
        error('Usage: vshard.storage.sync([timeout: number])')
    end

    log.debug("Synchronizing replicaset...")
    timeout = timeout or M.sync_timeout
    local vclock = box.info.vclock
    local tstart = lfiber.time()
    repeat
        local done = true
        for _, replica in ipairs(box.info.replication) do
            if replica.downstream and replica.downstream.vclock and
               not vclock_lesseq(vclock, replica.downstream.vclock) then
                done = false
            end
        end
        if done then
            log.debug("Replicaset has been synchronized")
            return true
        end
        lfiber.sleep(0.001)
    until not (lfiber.time() <= tstart + timeout)
    log.warn("Timed out during synchronizing replicaset")
    return false
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
    local reason = nil
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
        return bucket, lerror.vshard(lerror.code.NON_MASTER,
                                     M.this_replica.uuid,
                                     M.this_replicaset.uuid)
    else
        return bucket
    end
    return bucket, lerror.vshard(lerror.code.WRONG_BUCKET, bucket_id, reason,
                                 bucket and bucket.destination)
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
    for i = first_bucket_id, first_bucket_id + count - 1 do
        _bucket:insert({i, consts.BUCKET.ACTIVE})
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
--        [{space_id, [space_tuples]}, ...].
-- @param opts Options. Now the only possible option is 'is_last'.
--        It is set to true when the data portion is last and the
--        bucket can be activated here.
--
-- @retval nil, error Error occured.
-- @retval true The data is received ok.
--
local function bucket_recv_xc(bucket_id, from, data, updates, opts)
    if not from or not M.replicasets[from] then
        return nil, lerror.vshard(lerror.code.NO_SUCH_REPLICASET, from)
    end
    local _bucket = box.space._bucket
    local b = _bucket:get{bucket_id}
    local recvg = consts.BUCKET.RECEIVING
    if not b then
        _bucket:insert({bucket_id, recvg, from})
    elseif b.status ~= recvg then
        local msg = string.format("bucket state is changed: was receiving, "..
                                  "became %s", b.status)
        return nil, lerror.vshard(lerror.code.WRONG_BUCKET, bucket_id, msg,
                                  from)
    end
    local bucket_generation = M.bucket_generation
    local limit = consts.BUCKET_CHUNK_SIZE
    for _, row in ipairs(data) do
        local space_id, space_data = row[1], row[2]
        local space = box.space[space_id]
        if space == nil then
            -- Tarantool doesn't provide API to create box.error
            -- objects before 1.10.
            local _, boxerror = pcall(box.error, box.error.NO_SUCH_SPACE,
                                      space_id)
            return nil, lerror.box(boxerror)
        end
        box.begin()
        for _, tuple in ipairs(space_data) do
            space:insert(tuple)
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
                limit = consts.BUCKET_CHUNK_SIZE
            end
        end
        box.commit()
        bucket_generation = bucket_guard_xc(bucket_generation, bucket_id, recvg)
    end
    for _, txn in pairs(updates) do
        box.begin()
        for space_id, space_data in pairs(txn) do
            local space = box.space[space_id]
            for _, stmt in pairs(space_data) do
                if not stmt[2] then
                    space:replace(stmt[1])
                else
                    space:delete(stmt[1])
                end
            end
        end
        box.commit()
    end
    if opts and opts.is_last then
        _bucket:replace({bucket_id, consts.BUCKET.ACTIVE})
        if M.errinj.ERRINJ_LONG_RECEIVE then
            box.error(box.error.TIMEOUT)
        end
    end
    return true
end

--
-- Exception safe version of bucket_recv_xc.
--
local function bucket_recv(bucket_id, from, data, updates, opts)
    M.buckets_to_recovery[bucket_id] = true
    M.rebalancer_transfering_buckets[bucket_id] = true
    local status, ret, err = pcall(bucket_recv_xc, bucket_id, from, data,
                                   updates, opts)
    M.rebalancer_transfering_buckets[bucket_id] = nil
    if status then
        if ret then
            if opts and opts.is_last then
                M.buckets_to_recovery[bucket_id] = nil
            end
            return ret
        end
    else
        err = ret
        ret = status
    end
    box.rollback()
    return nil, err
end

--
-- Find spaces with index having the specified (in cfg) name.
-- The function result is cached using `schema_version`.
-- @retval Map of type {space_id = <space object>}.
--
local sharded_spaces_cache_schema_version = nil
local sharded_spaces_cache = nil
local function find_sharded_spaces()
    if sharded_spaces_cache_schema_version == box.internal.schema_version() then
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
    sharded_spaces_cache_schema_version = box.internal.schema_version()
    sharded_spaces_cache = spaces
    return spaces
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
    local status, err = bucket_check_state(bucket_id, 'read')
    if err then
        return nil, err
    end
    local data = {}
    local spaces = find_sharded_spaces()
    local idx = M.shard_index
    for k, space in pairs(spaces) do
        assert(space.index[idx] ~= nil)
        local space_data = space.index[idx]:select({bucket_id})
        table.insert(data, {space.id, space_data})
    end
    return data
end

--
-- Collect array of active bucket identifiers for discovery.
--
local function buckets_discovery()
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
    box.cfg{read_only = false}
end

--
-- Prepare to a master demotion. Before it, a master must stop
-- accept writes, and try to wait until all of its data is
-- replicated to each slave.
--
local function local_on_master_disable_prepare()
    log.info("Resigning from the replicaset master role...")
    box.cfg({read_only = true})
    sync(M.sync_timeout)
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
    box.cfg({read_only = true})
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
    box.cfg({read_only = false})
    M._on_master_enable:run()
    -- Start background process to collect garbage.
    M.collect_bucket_garbage_fiber =
        util.reloadable_fiber_create('vshard.gc', M, 'gc_bucket_f')
    M.recovery_fiber =
        util.reloadable_fiber_create('vshard.recovery', M, 'recovery_f')
    -- TODO: check current status
    log.info("Took on replicaset master role")
end

------------------------------------------------------------------
-- Bucket sending
------------------------------------------------------------------
--[[
Bucket sending is a complex procedure consisting of several
stages. First stage - mark the bucket as SENDING and schedule its
recovery if the transfer will fail.

Second stage - send the bucket data of each space iterating over
its shard index in chunks. For transfer duration on the space a
trigger is hanged which tracks each update of the space bucket
data. Then these updates are sent together with a next chunk. Once
the space is completely scanned, all its updates are being
collected and sent while other spaces are being scanned.

Third stage - lock the bucket updating until its already collected
updates are sent and the bucket is activated on a destination.

Each space has its own transfer state, created for each
transferring bucket:
{
    collect_all = <boolean, is set when the space is scanned
                   completely and all updates are needed>,
    cmp_def = <cdata key_def, compare key definition used to check
               whether a tuple crossed the transfer front or not>,
    schema_version = <number, schema version to refetch cmp_def on
                      schema updates>,
    space = <box.space object, transferring space>,
    current_tuple = <cdata tuple, transfer front. Each update of a
                     tuple < this one should be resent>,
    is_started = <boolean, true if a transfer of this space is
                  started>,
    bucket_field = <number, number of the field, containing
                    bucket_id>,
    on_replace = <function, on replace trigger which schedules
                  on_commit trigger to save txn data of the
                  bucket>,
    transfer = <global bucket transfer state object, reference to
                the global bucket transfer state.>,
}

And the entire bucket transfer process has a global state:
{
    bucket_id = <number, identifier of the transferring bucket>,
    destination = <string, UUID of the destination>,
    is_locked = <boolean, is set when the final update packets are
                 in fly>,
    spaces = <map of the format: space_id => space_transfer
              object>,
    queue = <queue of updates> {
        txns = <array of maps of the format space_id => array of
                statements of the format: [tuple, is_deleted],
                list of transactions, updating the bucket>,
        first = <number, index of the first txn in the queue>,
        last = <number, index of the last txn in the queue>,
        stmt_count = <number, number of statements in the whole
                      queue>,
    },
    error = <error object set from on_commit trigger. If it is not
             nil, then an on_commit trigger failed to save
             updates>
}
--]]

--
-- Check if the tuple has a key that was transferred already. For
-- this the tuple is compared with the last transferred tuple by
-- shard_index key.
-- @param space_transfer Space transfer object.
-- @param tuple Tuple to check.
--
-- @retval True, if the tuple is behind. False otherwise.
--
local function space_transfer_is_tuple_behind(space_transfer, tuple)
    -- Collect all when the space is sent completely and only
    -- updates arrive.
    if space_transfer.collect_all then
        return true
    end
    if space_transfer.schema_version ~= box.internal.schema_version() then
        local iid = space_transfer.space.index[M.shard_index]
        space_transfer.cmp_def =
            builtin.space_index_cmp_def(builtin.space_by_id(space.id), iid)
    end
    return builtin.box_tuple_compare(space_transfer.current_tuple, tuple,
                                     space_transfer.cmp_def) >= 0
end

--
-- Collect updates of the transferring bucket and store into the
-- transfer queue.
-- @param txn Transaction iterator.
-- @param transfer Bucket transfer state.
--
local function transfer_on_commit_xc(txn, transfer)
    local bucket_data = {}
    local total = 0
    for _, old, new, space_id in txn() do
        local space_transfer = transfer.spaces[space_id]
        if not space_transfer.is_started then
            goto continue
        end
        local to_insert
        -- Collect all updates of the bucket. It is not possible
        -- now to check if the tuple is behind a transfer front
        -- since a vinyl iterator can be yielding right now on
        -- exactly this tuple and did not manage to set
        -- current_tuple yet. So this update could be erroneously
        -- skipped.
        if new then
            if new[space_transfer.bucket_field] ~= transfer.bucket_id then
                goto continue
            end
            to_insert = {new}
        else
            if old[space_transfer.bucket_field] ~= transfer.bucket_id then
                goto continue
            end
            to_insert = {old, true}
        end
        local space_data = bucket_data[space_id]
        if space_data then
            table.insert(space_data, to_insert)
        else
            bucket_data[space_id] = {to_insert}
        end
        total = total + 1
::continue::
    end
    if total > 0 then
        local q = transfer.queue
        table.insert(q.txns, bucket_data)
        q.last = #q.txns
        q.stmt_count = q.stmt_count + total
    end
end

--
-- Exception safe version of transfer_on_commit_xc. Sets
-- transfer.error instead.
--
local function transfer_on_commit(txn, transfer)
    local ok, err = pcall(transfer_on_commit_xc, txn, transfer)
    if not ok then
        transfer.error = err
    end
end

--
-- On replace trigger working during a bucket transfer to forbid
-- some replaces, and to schedule on_commit triggers.
-- @param old Old tuple or nil.
-- @param new New tuple or nil.
-- @param space_transfer Space transfer state.
--
local function space_transfer_on_replace(old, new, space_transfer)
    local transfer = space_transfer.transfer
    local bucket_id = transfer.bucket_id
    if not M.buckets_to_recovery[bucket_id] then
        -- Remove self from triggers list. It can be still here if
        -- bucket_send failed before cleaning on_replaces. So this
        -- trigger is irrelevant.
        space_transfer.space:on_replace(nil, space_transfer.on_replace)
        return
    end
    local field = space_transfer.bucket_field
    if ((new and new[field]) or (old and old[field])) ~= bucket_id then
        -- Not linked with this bucket.
        return
    end
    if transfer.is_locked or
       transfer.queue.stmt_count > REBALANCER_MAX_BUCKET_QUEUE then
        error(lerror.vshard(lerror.code.TRANSFER_IS_IN_PROGRESS, bucket_id,
                            transfer.destination))
    end
    local storage = lfiber.self().storage
    if not storage.__vshard_is_commit_set then
        box.on_commit(function(txn) transfer_on_commit(txn, transfer) end)
        storage.__vshard_is_commit_set = true
    end
end

--
-- Pop at least BUCKET_CHUNK_SIZE update statements from the
-- queue.
-- @param transfer Transfer object from which to pop.
-- @retval Updates as an array of maps of the format space_id =>
--         array of pairs [tuple/key, is_deleted].
--
local function transfer_pop_updates(transfer)
    local limit = consts.BUCKET_CHUNK_SIZE
    local res = {}
    local queue = transfer.queue
    local txns = queue.txns
    local total = 0
    local i = queue.first
    local last = queue.last
    -- Hack to collect tuple_extract_key results later.
    box.begin()
    while i < last and limit > total do
        local filtered_txn = {}
        for space_id, space_data in pairs(txns[i]) do
            local space_transfer = transfer.spaces[space_id]
            if not space_transfer.is_started then
                goto continue
            end
            local filtered_space_data = table_new(#space_data, 0)
            for _, stmt in pairs(space_data) do
                if space_transfer_is_tuple_behind(space_transfer, stmt[1]) then
                    if stmt[2] then
                        stmt[1] = util.tuple_extract_key(stmt[1], space_id, 0)
                    end
                    table.insert(filtered_space_data, stmt)
                    total = total + 1
                end
            end
            filtered_txn[space_id] = filtered_space_data
::continue::
        end
        table.insert(res, filtered_txn)
        txns[i] = nil
        i = i + 1
    end
    box.rollback()
    queue.stmt_count = queue.stmt_count - total
    queue.first = i
    return res
end

local function bucket_transfer_open(bucket_id)
    local transfer = table_new(0, 10)
    transfer.queue = { first = 0, last = 0, txns = {}, stmt_count = 0 }
    transfer.bucket_id = bucket_id
    transfer.destination = destination
    transfer.spaces = table.copy(find_sharded_spaces())
    transfer.current_space_id = nil
end

local function bucket_transfer_close(transfer)
    for _, space_transfer in pairs(transfer.spaces) do
        if space_transfer.space then
            space_transfer.space:on_replace(nil, space_transfer.on_replace)
        end
    end
end

local function bucket_transfer_lock(transfer)
    transfer.is_locked = true
end

local function bucket_transfer_is_empty(transfer)
    return transfer.queue.stmt_count == 0
end

local function bucket_transfer_next(transfer)
    local limit = consts.BUCKET_CHUNK_SIZE
    local data = {}
    while true do
        local space
        local space_id = transfer.current_space_id
        if not space_id then
            goto next_space
        end
        local space_transfer = transfer.spaces[space_id]
        local itr = space_transfer.itr
        local itr_key = space_transfer.itr_key
        local itr_state = space_transfer.itr_state
        local space_data = {}
        while true do
            _, tuple = itr(itr_key, itr_state)
            if tuple == nil then
                goto next_space
            end
            table.insert(space_data, t)
            space_transfer.current_tuple = t
            limit = limit - 1
            if limit == 0 then
                goto exit
            end
        end
        space_transfer.collect_all = true
        if #space_data > 0 then
            table.insert(data, {space_id, space_data})
        end

::next_space::
        transfer.current_space_id, space = next(transfer.spaces, space_id)
        if not transfer.current_space_id then
            goto exit
        end
        local shard_index = space.index[M.shard_index]
        space_transfer = table_new(0, 9)
        -- Number of tuple field storing bucket_id.
        space_transfer.bucket_field = shard_index.parts[1].fieldno
        -- On replace trigger to track updates.
        space_transfer.on_replace = function(old, new)
            space_transfer_on_replace(old, new, space_transfer)
        end
        space_transfer.transfer = transfer
        space_transfer.is_started = true
        space_transfer.space = space
        space_transfer.itr, space_transfer.itr_key, space_transfer.itr_state =
            shard_index:pairs({bucket_id})
        transfer.spaces[space_id] = space_transfer
        space:on_replace(space_transfer.on_replace)
    end
::exit::
    if transfer.error then
        return nil, lerror.make(transfer.error)
    end
    table.insert(data, {space_id, space_data})
    return data, transfer_pop_updates(transfer)
end

--
-- Send a bucket to other replicaset.
--
local function bucket_send_xc(bucket_id, destination, opts)
    local uuid = box.info.cluster.uuid
    local status, err = bucket_check_state(bucket_id, 'write')
    if err then
        return nil, err
    end
    local replicaset = M.replicasets[destination]
    if replicaset == nil then
        return nil, lerror.vshard(lerror.code.NO_SUCH_REPLICASET, destination)
    end
    if destination == uuid then
        return nil, lerror.vshard(lerror.code.MOVE_TO_SELF, bucket_id,
                                  replicaset_uuid)
    end
    local sendg = consts.BUCKET.SENDING
    local status, err, recv_opts
    local bucket_generation = M.bucket_generation
    local transfer = bucket_transfer_open(bucket_id)
    _bucket:replace({bucket_id, sendg, destination})
    while true do
        local data, updates = bucket_transfer_next(transfer)
        if not data then
            status = data
            err = updates
            goto finish
        end
        if #data == 0 then
            break
        end
        status, err = replicaset:callrw('vshard.storage.bucket_recv',
                                        {bucket_id, uuid, data, updates}, opts)
        if not status then
            _bucket:replace({bucket_id, consts.BUCKET.ACTIVE})
            goto finish
        end
        bucket_generation = bucket_guard_xc(bucket_generation, bucket_id, sendg)
    end
    bucket_transfer_lock(transfer)
    recv_opts = {is_last = false}
    repeat
        local data, updates = bucket_transfer_next(transfer)
        assert(#data == 0)
        if not data then
            status = data
            err = updates
            goto finish
        end
        recv_opts.is_last = bucket_transfer_is_empty(transfer)
        status, err =
            replicaset:callrw('vshard.storage.bucket_recv',
                              {bucket_id, uuid, data, updates, recv_opts}, opts)
        if not status then
            if err.type == 'ShardingError' then
                _bucket:replace({bucket_id, consts.BUCKET.ACTIVE})
            end
            goto finish
        end
        bucket_generation = bucket_guard_xc(bucket_generation, bucket_id, sendg)
    until recv_opts.is_last
    _bucket:replace({bucket_id, consts.BUCKET.SENT, destination})
::finish::
    bucket_transfer_close(transfer)
    return status, err
end

--
-- Exception and recovery safe version of bucket_send_xc.
--
local function bucket_send(bucket_id, destination, opts)
    if type(bucket_id) ~= 'number' or type(destination) ~= 'string' then
        error('Usage: bucket_send(bucket_id, destination)')
    end
    M.buckets_to_recovery[bucket_id] = true
    M.rebalancer_transfering_buckets[bucket_id] = true
    local status, ret, err = pcall(bucket_send_xc, bucket_id, destination,
                                   opts)
    M.rebalancer_transfering_buckets[bucket_id] = nil
    if status then
        if ret then
            M.buckets_to_recovery[bucket_id] = nil
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
    local space_id = space.id
::restart::
    local limit = consts.BUCKET_CHUNK_SIZE
    box.begin()
    for _, tuple in bucket_index:pairs({bucket_id}) do
        space:delete(util.tuple_extract_key(tuple, space_id, 0))
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
-- Remove tuples from buckets of a specified type.
-- @param type Type of buckets to gc.
-- @retval List of ids of empty buckets of the type.
--
local function gc_bucket_step_by_type(type)
    local sharded_spaces = find_sharded_spaces()
    local empty_buckets = {}
    local limit = consts.BUCKET_CHUNK_SIZE
    for _, bucket in box.space._bucket.index.status:pairs(type) do
        for _, space in pairs(sharded_spaces) do
            gc_bucket_in_space_xc(space, bucket.id, type)
            limit = limit - 1
            if limit == 0 then
                lfiber.sleep(0)
                limit = consts.BUCKET_CHUNK_SIZE
            end
        end
        table.insert(empty_buckets, bucket.id)
    end
    return empty_buckets
end

--
-- Drop buckets with ids in the list.
-- @param bucket_ids Bucket ids to drop.
-- @param status Expected bucket status.
--
local function gc_bucket_drop_xc(bucket_ids, status)
    if #bucket_ids == 0 then
        return
    end
    local limit = consts.BUCKET_CHUNK_SIZE
    local bucket_generation = M.bucket_generation
    box.begin()
    local _bucket = box.space._bucket
    for _, id in pairs(bucket_ids) do
        bucket_guard_xc(bucket_generation, id, status)
        _bucket:delete{id}
        limit = limit - 1
        if limit == 0 then
            box.commit()
            box.begin()
            limit = consts.BUCKET_CHUNK_SIZE
        end
    end
    box.commit()
end

--
-- Exception safe version of gc_bucket_drop_xc.
--
local function gc_bucket_drop(bucket_ids, status)
    local status, err = pcall(gc_bucket_drop_xc, bucket_ids, status)
    if not status then
        box.rollback()
    end
    return status, err
end

--
-- Garbage collector. Works on masters. The garbage collector
-- wakes up once per specified time.
-- After wakeup it follows the plan:
-- 1) Check if _bucket has changed. If not, then sleep again;
-- 2) Scan user spaces for sent and garbage buckets, delete
--    garbage data in batches of limited size;
-- 3) Delete GARBAGE buckets from _bucket immediately, and
--    schedule SENT buckets for deletion after a timeout;
-- 4) Sleep, go to (1).
-- For each step details see comments in the code.
--
function gc_bucket_f()
    local module_version = M.module_version
    -- Changes of _bucket increments bucket generation. Garbage
    -- collector has its own bucket generation which is <= actual.
    -- Garbage collection is finished, when collector's
    -- generation == bucket generation. In such a case the fiber
    -- does nothing until next _bucket change.
    local bucket_generation_collected = -1
    -- Empty sent buckets are collected into an array. After a
    -- specified time interval the buckets are deleted both from
    -- this array and from _bucket space.
    local buckets_for_redirect = {}
    local buckets_for_redirect_ts = lfiber.time()
    -- Empty sent buckets, updated after each step, and when
    -- buckets_for_redirect is deleted, it gets empty_sent_buckets
    -- for next deletion.
    local empty_garbage_buckets, empty_sent_buckets, status, err
    while M.module_version == module_version do
        -- Check if no changes in buckets configuration.
        if bucket_generation_collected ~= M.bucket_generation then
            local bucket_generation = M.bucket_generation
            status, empty_garbage_buckets =
                pcall(gc_bucket_step_by_type, consts.BUCKET.GARBAGE)
            if not status then
                err = empty_garbage_buckets
                goto check_error
            end
            status, empty_sent_buckets =
                pcall(gc_bucket_step_by_type, consts.BUCKET.SENT)
            if not status then
                err = empty_sent_buckets
                goto check_error
            end
            status, err = gc_bucket_drop(empty_garbage_buckets,
                                         consts.BUCKET.GARBAGE)
::check_error::
            if not status then
                box.rollback()
                log.error('Error during garbage collection step: %s', err)
                goto continue
            end
            bucket_generation_collected = bucket_generation
        end

        if lfiber.time() - buckets_for_redirect_ts >=
           consts.BUCKET_SENT_GARBAGE_DELAY then
            status, err = gc_bucket_drop(buckets_for_redirect,
                                         consts.BUCKET.SENT)
            if not status then
                buckets_for_redirect = {}
                empty_sent_buckets = {}
                bucket_generation_collected = -1
                log.error('Error during deletion of empty sent buckets: %s',
                          err)
            elseif M.module_version ~= module_version then
                return
            else
                buckets_for_redirect = empty_sent_buckets or {}
                empty_sent_buckets = nil
                buckets_for_redirect_ts = lfiber.time()
            end
        end
::continue::
        lfiber.sleep(M.collect_bucket_garbage_interval)
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
    if bucket ~= nil and not bucket_is_garbage(bucket) and not opts.force then
        error('Can not delete not garbage bucket. Use "{force=true}" to '..
              'ignore this attention')
    end
    local sharded_spaces = find_sharded_spaces()
    local bucket_status = bucket and bucket.status
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
-- @param max_receiving Maximal bucket count that can be received
--        in parallel by a single master.
--
-- @retval Maximal disbalance over all replicasets.
--
local function rebalancer_calculate_metrics(replicasets, max_receiving)
    local max_disbalance = 0
    for _, replicaset in pairs(replicasets) do
        local needed = replicaset.etalon_bucket_count - replicaset.bucket_count
        if replicaset.etalon_bucket_count ~= 0 then
            local disbalance =
                math.abs(needed) / replicaset.etalon_bucket_count * 100
            if disbalance > max_disbalance then
                max_disbalance = disbalance
            end
        elseif replicaset.bucket_count ~= 0 then
            max_disbalance = math.huge
        end
        assert(needed >= 0 or -needed <= replicaset.bucket_count)
        replicaset.needed = math.min(max_receiving, needed)
    end
    return max_disbalance
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
-- Fiber function to apply routes as described below.
--
local function rebalancer_apply_routes_f(routes)
    lfiber.name('vshard.rebalancer_applier')
    log.info('Apply rebalancer routes')
    local _status = box.space._bucket.index.status
    assert(_status:count({consts.BUCKET.SENDING}) == 0)
    assert(_status:count({consts.BUCKET.RECEIVING}) == 0)
    -- Can not assing it on fiber.create() like
    -- var = fiber.create(), because when it yields, we have no
    -- guarantee that an event loop does not contain events
    -- between this fiber and its creator.
    M.rebalancer_applier_fiber = lfiber.self()
    local active_buckets = _status:select{consts.BUCKET.ACTIVE}
    local i = 1
    local opts = {timeout = consts.REBALANCER_CHUNK_TIMEOUT}
    for dst_uuid, bucket_count in pairs(routes) do
        assert(i + bucket_count - 1 <= #active_buckets)
        log.info('Send %d buckets to %s', bucket_count, M.replicasets[dst_uuid])
        for j = i, i + bucket_count - 1 do
            local bucket_id = active_buckets[j].id
            local ret, err = bucket_send(bucket_id, dst_uuid, opts)
            if not ret then
                log.error('Error during rebalancer routes applying: %s', err)
                log.info('Can not apply routes')
                return
            end
        end
        log.info('%s buckets are sent ok', bucket_count)
        i = i + bucket_count
    end
    log.info('Rebalancer routes are applied')
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
local function rebalancer_f()
    local module_version = M.module_version
    while module_version == M.module_version do
        while not M.is_rebalancer_active do
            log.info('Rebalancer is disabled. Sleep')
            lfiber.sleep(consts.REBALANCER_IDLE_INTERVAL)
        end
        local status, replicasets, total_bucket_active_count =
            pcall(rebalancer_download_states)
        if M.module_version ~= module_version then
            return
        end
        if not status or replicasets == nil then
            if not status then
                log.error('Error during downloading rebalancer states: %s',
                          replicasets)
            end
            log.info('Some buckets are not active, retry rebalancing later')
            lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
            goto continue
        end
        lreplicaset.calculate_etalon_balance(replicasets,
                                             total_bucket_active_count)
        local max_disbalance =
            rebalancer_calculate_metrics(replicasets,
                                         M.rebalancer_max_receiving)
        if max_disbalance <= M.rebalancer_disbalance_threshold then
            log.info('The cluster is balanced ok. Schedule next rebalancing '..
                     'after %f seconds', consts.REBALANCER_IDLE_INTERVAL)
            lfiber.sleep(consts.REBALANCER_IDLE_INTERVAL)
            goto continue
        end
        local routes = rebalancer_build_routes(replicasets)
        -- Routes table can not be empty. If it had been empty,
        -- then max_disbalance would have been calculated
        -- incorrectly.
        local is_empty = true
        for _,__ in pairs(routes) do
            is_empty = false
            break
        end
        assert(not is_empty)
        for src_uuid, src_routes in pairs(routes) do
            local rs = M.replicasets[src_uuid]
            local status, err =
                rs:callrw('vshard.storage.rebalancer_apply_routes',
                          {src_routes})
            if not status then
                log.error('Error during routes appying on "%s": %s. '..
                          'Try rebalance later', rs, lerror.make(err))
                lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
                goto continue
            end
        end
        log.info('Rebalance routes are sent. Schedule next wakeup after '..
                 '%f seconds', consts.REBALANCER_WORK_INTERVAL)
        lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
::continue::
    end
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
    local bucket_count = _bucket:count()
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
    if mode ~= 'write' and mode ~= 'read' then
        error('Unknown mode: '..tostring(mode))
    end

    local status, err = bucket_check_state(bucket_id, mode)
    if err then
        return nil, err
    end
    -- TODO: implement box.call()
    return true, netbox.self:call(name, args)
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function storage_cfg(cfg, this_replica_uuid, is_reload)
    if this_replica_uuid == nil then
        error('Usage: cfg(configuration, this_replica_uuid)')
    end
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
    for rs_uuid, rs in pairs(new_replicasets) do
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
        box_cfg.listen = box_cfg.listen or this_replica.uri
        if box_cfg.replication == nil and this_replicaset.master
           and not is_master then
            box_cfg.replication = {this_replicaset.master.uri}
        else
            box_cfg.replication = {}
        end
        if was_master == is_master then
            box_cfg.read_only = not is_master
        end
        if type(box.cfg) == 'function' then
            box_cfg.instance_uuid = this_replica.uuid
            box_cfg.replicaset_uuid = this_replicaset.uuid
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
        local uri = luri.parse(this_replica.uri)
        box.once("vshard:storage:1", storage_schema_v1, uri.login, uri.password)
        box.space._bucket:on_replace(bucket_generation_increment)
    else
        local old = box.space._bucket:on_replace()[1]
        box.space._bucket:on_replace(bucket_generation_increment, old)
    end

    lreplicaset.rebind_replicasets(new_replicasets, M.replicasets)
    lreplicaset.outdate_replicasets(M.replicasets)
    M.replicasets = new_replicasets
    M.this_replicaset = this_replicaset
    M.this_replica = this_replica
    M.total_bucket_count = vshard_cfg.bucket_count
    M.rebalancer_disbalance_threshold =
        vshard_cfg.rebalancer_disbalance_threshold
    M.rebalancer_max_receiving = vshard_cfg.rebalancer_max_receiving
    M.shard_index = vshard_cfg.shard_index
    M.collect_bucket_garbage_interval =
        vshard_cfg.collect_bucket_garbage_interval
    M.collect_lua_garbage = vshard_cfg.collect_lua_garbage
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
    lua_gc.set_state(M.collect_lua_garbage, consts.COLLECT_LUA_GARBAGE_INTERVAL)
    -- Destroy connections, not used in a new configuration.
    collectgarbage()
end

--------------------------------------------------------------------------------
-- Monitoring
--------------------------------------------------------------------------------

local function storage_buckets_count()
    return  box.space._bucket.index.pk:count()
end

local function storage_buckets_info()
    local ibuckets = setmetatable({}, { __serialize = 'mapping' })

    for _, bucket in box.space._bucket:pairs() do
        ibuckets[bucket.id] = {
            id = bucket.id;
            status = bucket.status;
            destination = bucket.destination;
        }
    end

    return ibuckets
end

local function storage_info()
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
        for id, replica in pairs(box.info.replication) do
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
        for id, replica in pairs(box.info.replication) do
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
    return state
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
-- Functions of type 2 can be omited, because outside of a module
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
    storage_cfg(M.current_cfg, M.this_replica.uuid, true)
    M.module_version = M.module_version + 1
end

M.recovery_f = recovery_f
M.rebalancer_f = rebalancer_f
M.gc_bucket_f = gc_bucket_f

--
-- These functions are saved in M not for atomic reload, but for
-- unit testing.
--
M.gc_bucket_step_by_type = gc_bucket_step_by_type
M.rebalancer_build_routes = rebalancer_build_routes
M.rebalancer_calculate_metrics = rebalancer_calculate_metrics
M.cached_find_sharded_spaces = find_sharded_spaces

return {
    sync = sync,
    bucket_force_create = bucket_force_create,
    bucket_force_drop = bucket_force_drop,
    bucket_collect = bucket_collect,
    bucket_recv = bucket_recv,
    bucket_send = bucket_send,
    bucket_stat = bucket_stat,
    bucket_pin = bucket_pin,
    bucket_unpin = bucket_unpin,
    bucket_delete_garbage = bucket_delete_garbage,
    garbage_collector_wakeup = garbage_collector_wakeup,
    rebalancer_wakeup = rebalancer_wakeup,
    rebalancer_apply_routes = rebalancer_apply_routes,
    rebalancer_disable = rebalancer_disable,
    rebalancer_enable = rebalancer_enable,
    is_locked = is_this_replicaset_locked,
    rebalancing_is_in_progress = rebalancing_is_in_progress,
    recovery_wakeup = recovery_wakeup,
    call = storage_call,
    cfg = function(cfg, uuid) return storage_cfg(cfg, uuid, false) end,
    info = storage_info,
    buckets_info = storage_buckets_info,
    buckets_count = storage_buckets_count,
    buckets_discovery = buckets_discovery,
    rebalancer_request_state = rebalancer_request_state,
    internal = M,
    on_master_enable = on_master_enable,
    on_master_disable = on_master_disable,
    sharded_spaces = function()
        return table.deepcopy(find_sharded_spaces())
    end,
    module_version = function() return M.module_version end,
}
