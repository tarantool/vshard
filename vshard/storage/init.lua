local log = require('log')
local luri = require('uri')
local lfiber = require('fiber')
local netbox = require('net.box') -- for net.box:self()
local consts = require('vshard.consts')
local lerror = require('vshard.error')
local util = require('vshard.util')
local lcfg = require('vshard.cfg')
local lreplicaset = require('vshard.replicaset')

-- Internal state
local self = {
    --
    -- All known replicasets used for bucket re-balancing.
    -- See format in replicaset.lua.
    --
    replicasets = nil,
    -- Fiber to remove garbage buckets data.
    garbage_collect_fiber = nil,
    -- Fiber to rebalance a cluster.
    rebalancer_fiber = nil,
    -- Fiber which applies routes one by one. Its presense and
    -- active status means that the rebalancing is in progress
    -- now on the current node.
    rebalancer_applier_fiber = nil,
    -- Internal flag to activate and deactivate rebalancer. Mostly
    -- for tests.
    is_rebalancer_active = true,
    errinj = {
        ERRINJ_BUCKET_FIND_GARBAGE_DELAY = false,
    }
}

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------

local function storage_schema_v1(username, password)
    log.info("Initializing schema")
    box.schema.user.create(username, {password = password})
    box.schema.user.grant(username, 'replication')

    local bucket = box.schema.space.create('_bucket')
    bucket:format({
        {'id', 'unsigned'},
        {'status', 'string'},
        {'destination', 'string', is_nullable = true}
    })
    bucket:create_index('pk', {parts = {'id'}})
    bucket:create_index('status', {parts = {'status'}, unique = false})

    local storage_api = {
        'vshard.storage.call',
        'vshard.storage.bucket_force_create',
        'vshard.storage.bucket_force_drop',
        'vshard.storage.bucket_collect',
        'vshard.storage.bucket_send',
        'vshard.storage.bucket_recv',
        'vshard.storage.bucket_stat',
        'vshard.storage.buckets_count',
        'vshard.storage.buckets_info',
        'vshard.storage.rebalancer_request_state',
        'vshard.storage.rebalancer_apply_routes',
    }

    for _, name in ipairs(storage_api) do
        box.schema.func.create(name, {setuid = true})
        box.schema.user.grant(username, 'execute', 'function', name)
    end

    box.snapshot()
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
    log.verbose("Synchronizing replicaset...")
    timeout = timeout or consts.SYNC_TIMEOUT
    local vclock = box.info.vclock
    local tstart = lfiber.time()
    repeat
        local done = true
        for _, replica in ipairs(box.info.replication) do
            if replica.downstream and
               not vclock_lesseq(vclock, replica.downstream.vclock) then
                done = false
            end
        end
        if done then
            log.info("Replicaset has been synchronized")
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
-- Check if @a bucket is garbage. It is true for
-- * sent buckets;
-- * buckets explicitly marked to be a garbage.
--
local function bucket_is_garbage(bucket)
    return bucket.status == consts.BUCKET.SENT or
           bucket.status == consts.BUCKET.GARBAGE
end

--
-- Check that an action of a specified mode can be applied to a
-- bucket.
-- @param bucket_id Bucket identifier.
-- @param mode 'Read' or 'write' mode.
--
-- @retval true Bucket can accept an action of a specified mode.
-- @retval nil, error object Bucket can not accept the action.
--
local function bucket_check_state(bucket_id, mode)
    assert(type(bucket_id) == 'number')
    assert(mode == 'read' or mode == 'write')
    local bucket = box.space._bucket:get({bucket_id})
    local errcode = nil
    if bucket == nil or bucket_is_garbage(bucket) then
        errcode = lerror.code.WRONG_BUCKET
    elseif (bucket.status == consts.BUCKET.SENDING and mode ~= 'read') then
        errcode = lerror.code.TRANSFER_IS_IN_PROGRESS
    elseif bucket.status == consts.BUCKET.ACTIVE and mode ~= 'read' and
           self.this_replicaset.master ~= self.this_replica then
        errcode = lerror.code.NON_MASTER
    end
    if errcode ~= nil then
        local dest = bucket and bucket.destination or nil
        return nil, lerror.vshard(errcode, {bucket_id = bucket_id,
                                            destination = dest})
    end

    assert(bucket.status == consts.BUCKET.ACTIVE or
           bucket.status == consts.BUCKET.SENDING and mode == 'read')
    return true
end

--
-- Return information about bucket
--
local function bucket_stat(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_stat(bucket_id)')
    end
    local bucket = box.space._bucket:get({bucket_id})

    if not bucket or bucket_is_garbage(bucket) then
        return nil, lerror.vshard(lerror.code.WRONG_BUCKET,
                                  {bucket_id = bucket_id})
    else
        return {
            id = bucket.id;
            status = bucket.status;
            destination = bucket.destination;
        }
    end
end

--
-- Create bucket manually for initial bootstrap, tests or
-- emergency cases
--
local function bucket_force_create(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_force_create(bucket_id)')
    end

    box.space._bucket:insert({bucket_id, consts.BUCKET.ACTIVE})
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
-- Receive bucket with its data
--
local function bucket_recv(bucket_id, from, data)
    if type(bucket_id) ~= 'number' or type(data) ~= 'table' then
        error('Usage: bucket_recv(bucket_id, data)')
    end

    local bucket = box.space._bucket:get({bucket_id})
    if bucket ~= nil then
        return nil, lerror.vshard(lerror.code.BUCKET_ALREADY_EXISTS,
                                  {bucket_id = bucket_id})
    end

    box.begin()
    bucket = box.space._bucket:insert({bucket_id, consts.BUCKET.RECEIVING,
                                       from})
    -- Fill spaces with data
    for _, row in ipairs(data) do
        local space_id, space_data = row[1], row[2]
        local space = box.space[space_id]
        if space == nil then
            -- Tarantool doesn't provide API to create box.error objects
            -- https://github.com/tarantool/tarantool/issues/3031
            local _, boxerror = pcall(box.error, box.error.NO_SUCH_SPACE,
                                      space_id)
            return nil, lerror.box(boxerror)
        end
        for _, tuple in ipairs(space_data) do
            space:insert(tuple)
        end
    end

    -- Activate bucket
    bucket = box.space._bucket:replace({bucket_id, consts.BUCKET.ACTIVE})

    box.commit()
    return true
end

--
-- Find spaces with indexed bucket_id fields.
-- @retval Map of type {space_id = <space object>}.
--
local function find_sharded_spaces()
    local spaces = {}
    for k, space in pairs(box.space) do
        if type(k) == 'number' and space.index.bucket_id ~= nil then
            local parts = space.index.bucket_id.parts
            if #parts == 1 and parts[1].type == 'unsigned' then
                spaces[k] = space
            end
        end
    end
    return spaces
end

--
-- Collect bucket data from all spaces.
-- @retval In a case of success, bucket data in
--         array of pairs: {space_id, <array of tuples>}.
--
local function bucket_collect_internal(bucket_id)
    local data = {}
    local spaces = find_sharded_spaces()
    for k, space in pairs(spaces) do
        assert(space.index.bucket_id ~= nil)
        local space_data = space.index.bucket_id:select({bucket_id})
        table.insert(data, {space.id, space_data})
    end
    return data
end

--
-- Collect content of ACTIVE bucket.
--
local function bucket_collect(bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: bucket_collect(bucket_id)')
    end

    local status, err = bucket_check_state(bucket_id, 'read')
    if not status then
        return nil, err
    end
    return bucket_collect_internal(bucket_id)
end

--
-- This function executes when a master role is removed from local
-- instance during configuration
--
local function local_master_disable()
    log.verbose("Resigning from the replicaset master role...")
    -- Stop garbage collecting
    if self.garbage_collect_fiber ~= nil then
        self.garbage_collect_fiber:cancel()
        self.garbage_collect_fiber = nil
        log.info("GC stopped")
    end
    -- Wait until replicas are synchronized before one another become a new master
    sync(consts.SYNC_TIMEOUT)
    log.info("Resigned from the replicaset master role")
end

local collect_garbage_f

--
-- This function executes whan a master role is added to local
-- instance during configuration
--
local function local_master_enable()
    log.verbose("Taking on replicaset master role...")
    -- Start background process to collect garbage.
    self.garbage_collect_fiber = lfiber.create(collect_garbage_f)
    log.info("GC started")
    -- TODO: check current status
    log.info("Took on replicaset master role")
end

--
-- Send a bucket to other replicaset
--
local function bucket_send(bucket_id, destination)
    if type(bucket_id) ~= 'number' or type(destination) ~= 'string' then
        error('Usage: bucket_send(bucket_id, destination)')
    end

    local status, err = bucket_check_state(bucket_id, 'write')
    if not status then
        return nil, err
    end
    local replicaset = self.replicasets[destination]
    if replicaset == nil then
        return nil, lerror.vshard(lerror.code.NO_SUCH_REPLICASET,
                                  {replicaset_uuid = destination})
    end

    if destination == box.info.cluster.uuid then
        return nil, lerror.vshard(lerror.code.MOVE_TO_SELF,
                                  {bucket_id = bucket_id,
                                   replicaset_uuid = replicaset_uuid})
    end

    local data = bucket_collect_internal(bucket_id)
    box.space._bucket:replace({bucket_id, consts.BUCKET.SENDING, destination})

    local status, err =
        replicaset:callrw('vshard.storage.bucket_recv',
                           {bucket_id, box.info.cluster.uuid, data})
    if not status then
        -- Rollback bucket state.
        box.space._bucket:replace({bucket_id, consts.BUCKET.ACTIVE})
        return status, err
    end

    box.space._bucket:replace({bucket_id, consts.BUCKET.SENT, destination})

    return true
end

--------------------------------------------------------------------------------
-- Garbage collector
--------------------------------------------------------------------------------
--
-- Find a bucket which has data in a space, but is not stored
-- in _bucket space or is garbage bucket.
-- @param bucket_index Index of the space with bucket_id part.
-- @param control GC controller. If buckets generation is
--        increased then interrupt the search.
-- @retval Not nil Garbage bucket id.
-- @retval     nil No garbage.
--
local function find_garbage_bucket(bucket_index, control)
    local curr_bucket = 0
    local iterations = 0
    local bucket_generation = control.bucket_generation
    while true do
        -- Get next bucket id from a space.
        local t = bucket_index:select({curr_bucket}, {iterator='GE', limit=1})
        if #t == 0 then
            return
        end
        assert(#t == 1)
        local bucket_id = util.tuple_extract_key(t[1], bucket_index.parts)[1]
        t = box.space._bucket:get({bucket_id})
        -- If a bucket is stored in _bucket and is garbage - the
        -- result is found.
        if t == nil or bucket_is_garbage(t) then
            return bucket_id
        end
        -- The found bucket is not garbage - continue search
        -- starting from the next one.
        curr_bucket = bucket_id + 1
        iterations = iterations + 1
        if iterations % 1000 == 0 or
           self.errinj.ERRINJ_BUCKET_FIND_GARBAGE_DELAY then
            while self.ERRINJ_BUCKET_FIND_GARBAGE_DELAY do
                lfiber.sleep(0.1)
            end
            -- Do not occupy 100% CPU.
            lfiber.yield()
            -- If during yield _bucket space has changed, then
            -- interrupt garbage collection step. It is restarted
            -- in the main function (collect_garbage_f).
            if bucket_generation ~= control.bucket_generation then
                return
            end
        end
    end
end

--
-- Delete from a space some garbage tuples with a specified bucket
-- id.
-- @param space Space to cleanup.
-- @param bucket_index Index containing bucket_id in a first part.
-- @param bucket_id Garbage bucket identifier.
--
local function collect_garbage_bucket_in_space(space, bucket_index, bucket_id)
    local pk_parts = space.index[0].parts
    box.begin()
    for _, tuple in bucket_index:pairs({bucket_id}) do
        space:delete(util.tuple_extract_key(tuple, pk_parts))
    end
    box.commit()
end

--
-- Make one garbage collection step. Go over sharded spaces,
-- search garbage in each one and delete part by part. If _bucket
-- has changed during a step, then the step is interrupted and
-- restarted.
-- @param control GC controller. If buckets generation is
--        increased then interrupt the step.
--
local function collect_garbage_step(control)
    log.info('Start next garbage collection step')
    local bucket_generation = control.bucket_generation
    -- If spaces are added later, they are not participate in
    -- garbage collection on this step and they must not do,
    -- because garbage buckets can be in only old spaces. If
    -- during garbage collection some buckets are removed and
    -- their tuples are in new spaces, then bucket_generation is
    -- incremented and such spaces are cleaned up on a next step.
    local sharded_spaces = find_sharded_spaces()
    -- For each space:
    -- 1) Find garbage bucket. If not found, go to a next space;
    -- 2) Delete all its tuples;
    -- 3) Go to 1.
    for _, space in pairs(sharded_spaces) do
        local bucket_index = space.index.bucket_id
        while true do
            local garbage_bucket = find_garbage_bucket(bucket_index, control)
            -- Stop the step, if a generation has changed.
            if bucket_generation ~= control.bucket_generation then
                log.info('Interrupt garbage collection step')
                return
            end
            if garbage_bucket == nil then
                break
            end
            collect_garbage_bucket_in_space(space, bucket_index, garbage_bucket)
            if bucket_generation ~= control.bucket_generation then
                log.info('Interrupt garbage collection step')
                return
            end
        end
    end
    assert(bucket_generation == control.bucket_generation)
    control.bucket_generation_collected = bucket_generation
    log.info('Finish garbage collection step')
end

--
-- Drop empty sent buckets with finished timeout of redirection.
-- @param redirect_buckets Array of empty sent buckets to delete.
--
local function collect_garbage_drop_redirects(redirect_buckets)
    if #redirect_buckets == 0 then
        return
    end
    local _bucket = box.space._bucket
    box.begin()
    for _, id in pairs(redirect_buckets) do
        local old = _bucket:get{id}
        -- Bucket can change status, if an admin manualy had
        -- changed it.
        if old ~= nil and old.status == consts.BUCKET.SENT then
            _bucket:delete{id}
        end
    end
    box.commit()
end

--
-- Delete garbage buckets from _bucket and get identifiers of
-- empty sent buckets.
-- @retval Array of empty sent bucket identifiers.
--
local function collect_garbage_update_bucket()
    local _bucket = box.space._bucket
    local status_index = _bucket.index.status
    -- Sent buckets are not deleted immediately after cleaning.
    -- They are used to redirect requests for a while. The table
    -- below collects such empty sent buckets to delete them after
    -- a specified timeout.
    local empty_buckets_for_redirect = {}
    local sent_buckets = status_index:select{consts.BUCKET.SENT}
    for _, bucket in pairs(sent_buckets) do
        table.insert(empty_buckets_for_redirect, bucket.id)
    end
    -- A receiving bucket can be garbage if it was found in
    -- _bucket right after bootstrap. It means, that it was
    -- unsuccessfully and partialy sent by another replicaset.
    -- In such a case it changes its status to GARBAGE, and GC
    -- deletes it here.
    local empty_buckets_to_delete = {}
    local garbage_buckets = status_index:select{consts.BUCKET.GARBAGE}
    for _, bucket in pairs(garbage_buckets) do
        table.insert(empty_buckets_to_delete, bucket.id)
    end
    if #empty_buckets_to_delete ~= 0 then
        box.begin()
        for _, id in pairs(empty_buckets_to_delete) do
            _bucket:delete{id}
        end
        box.commit()
    end
    return empty_buckets_for_redirect
end

--
-- Background garbage collector. Works on masters. The garbage
-- collector wakeups once per GARBAGE_COLLECT_INTERVAL seconds.
-- After wakeup it checks follows the plan:
-- 1) Check if _bucket has changed. If not, then sleep again;
-- 2) Scan user spaces for not existing, sent and garbage buckets,
--    delete garbage data in a single transaction;
-- 3) Restart GC, if _bucket has changed during data deletion;
-- 4) Delete GARBAGE buckets from _bucket immediately, and
--    schedule SENT buckets for deletion after a timeout;
-- 5) Sleep, go to (1).
-- For each step detains see comments in code.
--
function collect_garbage_f()
    -- Collector controller. Changes of _bucket increments
    -- bucket generation. Garbage collector has its own bucket
    -- generation which is <= actual. Garbage collection is
    -- finished, when collector's generation == bucket generation.
    -- In such a case the fiber does nothing until next _bucket
    -- change.
    local control = {
        bucket_generation = 1,
        bucket_generation_collected = 0
    }
    -- Function to trigger buckets garbage collection.
    local on_bucket_replace = function(old_tuple)
        if old_tuple ~= nil then
            control.bucket_generation = control.bucket_generation + 1
        end
    end
    box.space._bucket:on_replace(on_bucket_replace)
    -- Empty sent buckets are collected into an array. After a
    -- specified time interval the buckets are deleted both from
    -- this array and from _bucket space.
    local buckets_for_redirect = {}
    local buckets_for_redirect_ts = lfiber.time()
    -- Empty sent buckets, updated after each step, and when
    -- buckets_for_redirect is deleted, it gets empty_sent_buckets
    -- for next deletion.
    local empty_sent_buckets = {}

    while true do
::continue::
        -- Check if no changes in buckets configuration.
        if control.bucket_generation_collected ~= control.bucket_generation then
            local status, err = pcall(collect_garbage_step, control)
            if not status then
                log.error('Error during garbage collection step: %s', err)
                lfiber.sleep(consts.GARBAGE_COLLECT_INTERVAL)
                goto continue
            end
            status, empty_sent_buckets = pcall(collect_garbage_update_bucket)
            if not status then
                log.error('Error during empty buckets processing: %s',
                          empty_sent_buckets)
                control.bucket_generation = control.bucket_generation + 1
                lfiber.sleep(consts.GARBAGE_COLLECT_INTERVAL)
                goto continue
            end
        end
        local duration = lfiber.time() - buckets_for_redirect_ts
        if duration >= consts.BUCKET_SENT_GARBAGE_DELAY then
            local status, err = pcall(collect_garbage_drop_redirects,
                                      buckets_for_redirect)
            if not status then
                log.error('Error during deletion of empty sent buckets: %s',
                          err)
            else
                buckets_for_redirect = empty_sent_buckets
                empty_sent_buckets = {}
                buckets_for_redirect_ts = lfiber.time()
            end
        end
        lfiber.sleep(consts.GARBAGE_COLLECT_INTERVAL)
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
    for _, space in pairs(sharded_spaces) do
        collect_garbage_bucket_in_space(space, space.index.bucket_id, bucket_id)
    end
end

--------------------------------------------------------------------------------
-- Rebalancer
--------------------------------------------------------------------------------
--
-- Calculate a set of metrics:
-- * bucket_count per weight unit;
-- * maximal disbalance over all replicasets;
-- * needed buckets for each replicaset.
-- @param replicasets Map of type: {
--     uuid = {bucket_count = number, weight = number},
--     ...
-- }
-- @retval Maximal disbalance over all replicasets.
--
local function rebalancer_calculate_metrics(replicasets)
    local weight_sum = 0
    for _, replicaset in pairs(replicasets) do
        weight_sum = weight_sum + replicaset.weight
    end
    assert(weight_sum ~= 0)
    local bucket_per_weight = consts.BUCKET_COUNT / weight_sum
    local max_disbalance = 0
    for _, replicaset in pairs(replicasets) do
        local ethalon_bucket_count = replicaset.weight * bucket_per_weight
        local needed = ethalon_bucket_count - replicaset.bucket_count
        needed = math.ceil(needed)
        if ethalon_bucket_count ~= 0 then
            local disbalance = math.abs(needed) / ethalon_bucket_count * 100
            if disbalance > max_disbalance then
                max_disbalance = disbalance
            end
        elseif replicaset.bucket_count ~= 0 then
            max_disbalance = math.huge
        end
        assert(needed >= 0 or -needed <= replicaset.bucket_count)
        replicaset.needed = math.min(consts.REBALANCER_MAX_RECEIVING, needed)
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
    log.info('Apply rebalancer routes')
    -- Can not assing it on fiber.create() like
    -- var = fiber.create(), because when it yields, we have no
    -- guarantee that an event loop does not contain events
    -- between this fiber and its creator.
    self.rebalancer_applier_fiber = lfiber.self()
    local active_buckets =
        box.space._bucket.index.status:select{consts.BUCKET.ACTIVE}
    local i = 1
    for dst_uuid, bucket_count in pairs(routes) do
        assert(i + bucket_count - 1 <= #active_buckets)
        log.info('Send %d buckets to "%s"', bucket_count, dst_uuid)
        for j = i, i + bucket_count - 1 do
            local status, ret = pcall(bucket_send, active_buckets[j].id,
                                      dst_uuid)
            if not status or ret ~= true then
                if not status then
                    log.error('Error during rebalancer routes applying: %s', ret)
                end
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
-- Check if a rebalancing is in progress. It is true, if the node
-- applies routes received from a rebalancer node in the special
-- fiber.
--
local function rebalancing_is_in_progress()
    local f = self.rebalancer_applier_fiber
    return f ~= nil and f:status() ~= 'dead'
end

--
-- Apply routes table of type: {
--     dst_uuid = number, -- Bucket count to send.
--     ...
-- }. Is used by a rebalancer.
--
local function rebalancer_apply_routes(routes)
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
    local total_bucket_active_count = 0
    for uuid, replicaset in pairs(self.replicasets) do
        local bucket_active_count =
            replicaset:callrw('vshard.storage.rebalancer_request_state', {})
        if bucket_active_count == nil then
            return
        end
        total_bucket_active_count = total_bucket_active_count +
                                    bucket_active_count
        replicasets[uuid] = {bucket_count = bucket_active_count,
                             weight = replicaset.weight or 1}
    end
    if total_bucket_active_count == consts.BUCKET_COUNT then
        return replicasets
    else
        log.info('Total active bucket count is not equal to BUCKET_COUNT. '..
                 'Possibly a boostrap is not finished yet.')
    end
end

--
-- Background rebalancer. Works on a storage which has the
-- smallest replicaset uuid and which is master.
--
local function rebalancer_f()
    while true do
::continue::
        while not self.is_rebalancer_active do
            log.info('Rebalancer is disabled. Sleep')
            lfiber.sleep(consts.REBALANCER_IDLE_INTERVAL)
        end
        local status, replicasets = pcall(rebalancer_download_states)
        if not status or replicasets == nil then
            if not status then
                log.error('Error during downloading rebalancer states: %s',
                          replicasets)
            end
            log.info('Some buckets are not active, retry rebalancing later')
            lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
            goto continue
        end
        local max_disbalance = rebalancer_calculate_metrics(replicasets)
        if max_disbalance <= consts.REBALANCER_DISBALANCE_THRESHOLD then
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
            local rs = self.replicasets[src_uuid]
            local status, err =
                rs:callrw('vshard.storage.rebalancer_apply_routes',
                          {src_routes})
            if not status then
                log.error('Error during routes appying on "%s": %s. '..
                          'Try rebalance later', src_uuid, err)
                lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
                goto continue
            end
        end
        log.info('Rebalance routes are sent. Schedule next wakeup after '..
                 '%f seconds', consts.REBALANCER_WORK_INTERVAL)
        lfiber.sleep(consts.REBALANCER_WORK_INTERVAL)
    end
end

--
-- Check all buckets of the host storage to have SENT or ACTIVE
-- state, return active bucket count.
-- @retval not nil Count of active buckets.
-- @retval     nil Not SENT or not ACTIVE buckets were found.
--
local function rebalancer_request_state()
    if not self.is_rebalancer_active or rebalancing_is_in_progress() then
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
    return status_index:count({consts.BUCKET.ACTIVE})
end

--
-- Immediately wakeup rebalancer, if it exists on the current
-- node.
--
local function rebalancer_wakeup()
    if self.rebalancer_fiber ~= nil then
        self.rebalancer_fiber:wakeup()
    end
end

--
-- Disable/enable rebalancing. Disabled rebalancer sleeps until it
-- is enabled back. If not a rebalancer node is disabled, it does
-- not sends its state to rebalancer.
--
local function rebalancer_disable()
    self.is_rebalancer_active = false
end
local function rebalancer_enable()
    self.is_rebalancer_active = true
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

    local ok, err = bucket_check_state(bucket_id, mode)
    if not ok then
        return nil, err
    end
    -- TODO: implement box.call()
    return true, netbox.self:call(name, args)
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------
local function storage_cfg(cfg, this_replica_uuid)
    cfg = table.deepcopy(cfg)
    if this_replica_uuid == nil then
        error('Usage: cfg(configuration, this_replica_uuid)')
    end
    lcfg.check(cfg)
    if cfg.weights or cfg.zone then
        error('Weights and zone are not allowed for storage configuration')
    end
    if self.replicasets ~= nil then
        log.info("Starting reconfiguration of replica %s", this_replica_uuid)
    else
        log.info("Starting configuration of replica %s", this_replica_uuid)
    end

    local was_master = self.this_replicaset and
                       self.this_replicaset.master == self.this_replica

    local this_replicaset
    local this_replica
    local new_replicasets = lreplicaset.buildall(cfg, self.replicasets or {})
    local min_master_uuid
    for rs_uuid, rs in pairs(new_replicasets) do
        for replica_uuid, replica in pairs(rs.replicas) do
            if (min_master_uuid == nil or replica_uuid < min_master_uuid) and
               rs.master == replica then
                min_master_uuid = replica_uuid
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

    cfg.listen = cfg.listen or this_replica.uri
    if cfg.replication == nil then
        cfg.replication = {}
        for uuid, replica in pairs(this_replicaset.replicas) do
            table.insert(cfg.replication, replica.uri)
         end
    end
    cfg.instance_uuid = this_replica.uuid
    cfg.replicaset_uuid = this_replicaset.uuid
    cfg.sharding = nil

    local is_master = this_replicaset.master == this_replica
    if is_master then
        log.info('I am master')
    end
    box.cfg(cfg)
    log.info("Box has been configured")
    self.replicasets = new_replicasets
    self.this_replicaset = this_replicaset
    self.this_replica = this_replica
    local uri = luri.parse(this_replica.uri)
    box.once("vshard:storage:1", storage_schema_v1, uri.login, uri.password)

    local is_master = self.this_replicaset and
                      self.this_replicaset.master == self.this_replica
    if was_master and not is_master then
        local_master_disable()
    end

    if not was_master and is_master then
        local_master_enable()
    end

    -- Collect old net.box connections
    collectgarbage('collect')
    if min_master_uuid == this_replica.uuid then
        if not self.rebalancer_fiber then
            log.info('Run rebalancer')
            self.rebalancer_fiber = lfiber.create(rebalancer_f)
        else
            log.info('Wakeup rebalancer')
            -- Configuration had changed. Time to rebalance.
            self.rebalancer_fiber:wakeup()
        end
    elseif self.rebalancer_fiber then
        log.info('Rebalancer location has changed to "%s"', min_master_uuid)
        self.rebalancer_fiber:cancel()
        self.rebalancer_fiber = nil
    end
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
            is_dirty = bucket.is_dirty;
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
    local this_uuid = self.this_replicaset.uuid
    if self.this_replicaset.master == nil then
        table.insert(state.alerts, alert(code.MISSING_MASTER, this_uuid))
        state.status = math.max(state.status, consts.STATUS.ORANGE)
    end
    if self.this_replicaset.master ~= self.this_replica then
        for id, replica in pairs(box.info.replication) do
            if replica.uuid ~= self.this_replicaset.master.uuid then
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
    else
        state.replication.status = 'master'
        local redundancy = 0
        for id, replica in pairs(box.info.replication) do
            if replica.uuid ~= self.this_replica.uuid then
                if replica.downstream == nil then
                    table.insert(state.alerts, alert(code.REPLICA_IS_DOWN,
                                                     replica.uuid))
                    state.status = math.max(state.status, consts.STATUS.YELLOW)
                else
                    redundancy = redundancy + 1
                end
            end
        end
        if redundancy == 0 then
            table.insert(state.alerts, alert(code.REPLICASET_IS_UNREACHABLE,
                                             this_uuid))
            state.status = math.max(state.status, consts.STATUS.RED)
        elseif redundancy == 1 then
            table.insert(state.alerts, alert(code.LOW_REDUNDANCY))
            state.status = math.max(state.status, consts.STATUS.ORANGE)
        end
    end

    state.bucket.count = box.space._bucket.index.pk:count()
    state.bucket.active = box.space._bucket.index.status:count({consts.BUCKET.ACTIVE})
    state.bucket.garbage = box.space._bucket.index.status:count({consts.BUCKET.SENT})
    state.bucket.receiving = box.space._bucket.index.status:count({consts.BUCKET.RECEIVING})
    state.bucket.sending = box.space._bucket.index.status:count({consts.BUCKET.SENDING})
    if state.bucket.receiving ~= 0 and state.bucket.sending ~= 0 then
        --
        --Some buckets are receiving and some buckets are sending at same time,
        --this may be a balancer issue, alert it.
        --
        table.insert(state.alerts, alert(lerror.code.INVALID_REBALANCING))
        state.status = math.max(state.status, consts.STATUS.YELLOW)
    end

    local ireplicasets = {}
    for uuid, replicaset in pairs(self.replicasets) do
        ireplicasets[uuid] = {
            uuid = uuid;
            master = {
                uri = replicaset.master.uri;
                uuid = replicaset.master.conn and replicaset.master.conn.peer_uuid;
                state = replicaset.master.conn and replicaset.master.conn.state;
                error = replicaset.master.conn and replicaset.master.conn.error;
            };
        };
    end
    state.replicasets = ireplicasets
    return state
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------

self.sync = sync
self.find_sharded_spaces = find_sharded_spaces
self.find_garbage_bucket = find_garbage_bucket
self.collect_garbage_step = collect_garbage_step
self.collect_garbage_f = collect_garbage_f

self.rebalancer_build_routes = rebalancer_build_routes
self.rebalancer_calculate_metrics = rebalancer_calculate_metrics

return {
    bucket_force_create = bucket_force_create;
    bucket_force_drop = bucket_force_drop;
    bucket_collect = bucket_collect;
    bucket_recv = bucket_recv;
    bucket_send = bucket_send;
    bucket_stat = bucket_stat;
    bucket_delete_garbage = bucket_delete_garbage;
    rebalancer_request_state = rebalancer_request_state;
    rebalancer_wakeup = rebalancer_wakeup;
    rebalancer_apply_routes = rebalancer_apply_routes;
    rebalancer_disable = rebalancer_disable;
    rebalancer_enable = rebalancer_enable;
    call = storage_call;
    cfg = storage_cfg;
    info = storage_info;
    buckets_info = storage_buckets_info;
    buckets_count = storage_buckets_count;
    internal = self;
}
