local log = require('log')
local lfiber = require('fiber')
local lmsgpack = require('msgpack')
local table_new = require('table.new')
local fiber_clock = lfiber.clock

local MODULE_INTERNALS = '__module_vshard_router'
-- Reload requirements, in case this module is reloaded manually.
if rawget(_G, MODULE_INTERNALS) then
    local vshard_modules = {
        'vshard.consts', 'vshard.error', 'vshard.cfg', 'vshard.version',
        'vshard.hash', 'vshard.replicaset', 'vshard.util',
        'vshard.service_info', 'vshard.log_ratelimit',
    }
    for _, module in pairs(vshard_modules) do
        package.loaded[module] = nil
    end
end
local consts = require('vshard.consts')
local lerror = require('vshard.error')
local lcfg = require('vshard.cfg')
local lhash = require('vshard.hash')
local lreplicaset = require('vshard.replicaset')
local lservice_info = require('vshard.service_info')
local lratelimit = require('vshard.log_ratelimit')
local util = require('vshard.util')
local seq_serializer = { __serialize = 'seq' }
local map_serializer = { __serialize = 'map' }
local future_wait = util.future_wait

local msgpack_is_object = lmsgpack.is_object

if not util.feature.msgpack_object then
    local msg = 'Msgpack object feature is not supported by current '..
                'Tarantool version'
    msgpack_is_object = function()
        error(msg)
    end
end

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        ---------------- Common module attributes ----------------
        errinj = {
            ERRINJ_CFG = false,
            ERRINJ_CFG_DELAY = false,
            ERRINJ_FAILOVER_DELAY = false,
            ERRINJ_RELOAD = false,
            ERRINJ_LONG_DISCOVERY = false,
        },
        -- Dictionary, key is router name, value is a router.
        routers = {},
        -- Router object which can be accessed by old api:
        -- e.g. vshard.router.call(...)
        static_router = nil,
        -- This counter is used to restart background fibers with
        -- new reloaded code.
        module_version = 0,

        ----------------------- Map-Reduce -----------------------
        -- Storage Ref ID. It must be unique for each ref request
        -- and therefore is global and monotonically growing.
        ref_id = 0,
    }
end

--
-- Router object attributes.
--
local ROUTER_TEMPLATE = {
        -- Name of router.
        name = nil,
        -- The last passed configuration.
        current_cfg = nil,
        -- Time to outdate old objects on reload.
        connection_outdate_delay = nil,
        -- Bucket map cache.
        route_map = {},
        -- All known replicasets used for bucket re-balancing
        replicasets = nil,
        -- Fiber to discovery buckets in background.
        discovery_fiber = nil,
        -- Save statuses and errors for the discovery fiber
        discovery_service = nil,
        -- How discovery works. On - work infinitely. Off - no
        -- discovery.
        discovery_mode = nil,
        -- Bucket count stored on all replicasets.
        total_bucket_count = 0,
        known_bucket_count = 0,
        --
        -- Timeout to wait sync on storages. Used by sync() call
        -- when no timeout is specified.
        --
        sync_timeout = consts.DEFAULT_SYNC_TIMEOUT,
        -- Flag whether router_cfg() is in progress.
        is_cfg_in_progress = false,
        -- Flag whether router_cfg() is finished.
        is_configured = false,
        -- Flag whether the instance is enabled manually. It is true by default
        -- for backward compatibility with old vshard.
        is_enabled = true,
        -- Reference to the function-proxy to most of the public functions. It
        -- allows to avoid 'if's in each function by adding expensive
        -- conditional checks in one rarely used version of the wrapper and no
        -- checks into the other almost always used wrapper.
        api_call_cache = nil,
}

local STATIC_ROUTER_NAME = '_static_router'

-- Set a bucket to a replicaset.
local function bucket_set(router, bucket_id, rs_id)
    local replicaset = router.replicasets[rs_id]
    -- It is technically possible to delete a replicaset at the
    -- same time when route to the bucket is discovered.
    if not replicaset then
        return nil, lerror.vshard(lerror.code.NO_ROUTE_TO_BUCKET, bucket_id)
    end
    local old_replicaset = router.route_map[bucket_id]
    if old_replicaset ~= replicaset then
        if old_replicaset then
            old_replicaset.bucket_count = old_replicaset.bucket_count - 1
        else
            router.known_bucket_count = router.known_bucket_count + 1
        end
        replicaset.bucket_count = replicaset.bucket_count + 1
    end
    router.route_map[bucket_id] = replicaset
    return replicaset
end

-- Remove a bucket from the cache.
local function bucket_reset(router, bucket_id)
    local replicaset = router.route_map[bucket_id]
    if replicaset then
        replicaset.bucket_count = replicaset.bucket_count - 1
        router.known_bucket_count = router.known_bucket_count - 1
    end
    router.route_map[bucket_id] = nil
end

local function route_map_clear(router)
    router.route_map = {}
    router.known_bucket_count = 0
    for _, rs in pairs(router.replicasets) do
        rs.bucket_count = 0
    end
end

--------------------------------------------------------------------------------
-- Discovery
--------------------------------------------------------------------------------

-- Resolve bucket id to replicaset
local function bucket_resolve(router, bucket_id)
    local replicaset = router.route_map[bucket_id]
    if replicaset ~= nil then
        return replicaset
    end

    log.verbose("Discovering bucket %d", bucket_id)
    local last_err = nil
    local unreachable_id = nil
    for id, replicaset in pairs(router.replicasets) do
        local _, err =
            replicaset:callrw('vshard.storage.bucket_stat', {bucket_id})
        if err == nil then
            return bucket_set(router, bucket_id, replicaset.id)
        elseif err.code ~= lerror.code.WRONG_BUCKET and
               err.code ~= lerror.code.REPLICASET_IN_BACKOFF then
            last_err = err
            unreachable_id = id
        end
    end
    local err
    if last_err then
        if last_err.type == 'ClientError' and
           last_err.code == box.error.NO_CONNECTION then
            err = lerror.vshard(lerror.code.UNREACHABLE_REPLICASET,
                                unreachable_id, bucket_id)
        else
            err = lerror.make(last_err)
        end
    else
        -- All replicasets were scanned, but a bucket was not
        -- found anywhere, so most likely it does not exist. It
        -- can be wrong, if rebalancing is in progress, and a
        -- bucket was found to be RECEIVING on one replicaset, and
        -- was not found on other replicasets (it was sent during
        -- discovery).
        err = lerror.vshard(lerror.code.NO_ROUTE_TO_BUCKET, bucket_id)
    end

    return nil, err
end

-- Group bucket ids by replicasets according to the router cache.
local function buckets_group(router, bucket_ids, timeout)
    local deadline = fiber_clock() + timeout
    local replicaset, err
    local replicaset_buckets = {}
    local buckets, id
    local bucket_map = {}
    for _, bucket_id in pairs(bucket_ids) do
        if bucket_map[bucket_id] then
            goto continue
        end
        if type(bucket_id) ~= 'number' then
            local msg = string.format("Bucket '%s' is not a number", bucket_id)
            return nil, lerror.make(msg)
        end
        -- Avoid duplicates.
        bucket_map[bucket_id] = true
        if fiber_clock() > deadline then
            return nil, lerror.timeout()
        end
        replicaset, err = bucket_resolve(router, bucket_id)
        if err ~= nil then
            return nil, err
        end
        id = replicaset.id
        buckets = replicaset_buckets[id]
        if buckets then
            table.insert(buckets, bucket_id)
        else
            replicaset_buckets[id] = {bucket_id}
        end
        ::continue::
    end

    return replicaset_buckets
end

--
-- Arrange downloaded buckets to the route map so as they
-- reference a given replicaset.
--
local function discovery_handle_buckets(router, replicaset, buckets)
    local count = replicaset.bucket_count
    local affected = {}
    for _, bucket_id in pairs(buckets) do
        local old_rs = router.route_map[bucket_id]
        if old_rs ~= replicaset then
            count = count + 1
            if old_rs then
                local bc = old_rs.bucket_count
                if not affected[old_rs] then
                    affected[old_rs] = bc
                end
                old_rs.bucket_count = bc - 1
            else
                router.known_bucket_count = router.known_bucket_count + 1
            end
            router.route_map[bucket_id] = replicaset
        end
    end
    if count ~= replicaset.bucket_count then
        log.info('Updated %s buckets: was %d, became %d', replicaset,
                 replicaset.bucket_count, count)
    end
    replicaset.bucket_count = count
    for rs, old_bucket_count in pairs(affected) do
        log.info('Affected buckets of %s: was %d, became %d', rs,
                 old_bucket_count, rs.bucket_count)
    end
end

--
-- Bucket discovery main loop.
--
local function discovery_service_f(router, service, limiter)
    local module_version = M.module_version
    assert(router.discovery_mode == 'on' or router.discovery_mode == 'once')
    local iterators = {}
    local opts = {is_async = true}
    local mode
    while module_version == M.module_version do
        service:next_iter()
        -- Just typical map reduce - send request to each
        -- replicaset in parallel, and collect responses. Many
        -- requests probably will be needed for each replicaset.
        --
        -- Step 1: create missing iterators, in case this is a
        -- first discovery iteration, or some replicasets were
        -- added after the router is started.
        for rs_id in pairs(router.replicasets) do
            local iter = iterators[rs_id]
            if not iter then
                iterators[rs_id] = {
                    args = {{from = 1}},
                    future = nil,
                }
            end
        end
        -- Step 2: map stage - send parallel requests for every
        -- iterator, prune orphan iterators whose replicasets were
        -- removed.
        service:set_activity('sending requests')
        for rs_id, iter in pairs(iterators) do
            local replicaset = router.replicasets[rs_id]
            if not replicaset then
                log.warn('Replicaset %s was removed during discovery', rs_id)
                iterators[rs_id] = nil
                goto continue
            end
            local future, err =
                replicaset:callro('vshard.storage.buckets_discovery', iter.args,
                                  opts)
            if not future then
                limiter:log_warn(err, service:set_status_error(
                    'Error during discovery %s, retry will be done later: %s',
                    rs_id, err))
                goto continue
            end
            iter.future = future
            -- Don't spam many requests at once. Give
            -- storages time to handle them and other
            -- requests.
            lfiber.sleep(consts.DISCOVERY_WORK_STEP)
            if module_version ~= M.module_version then
                return
            end
            ::continue::
        end
        -- Step 3: reduce stage - collect responses, restart
        -- iterators which reached the end.
        service:set_activity('collecting responses and updating route map')
        for rs_id, iter in pairs(iterators) do
            lfiber.yield()
            local future = iter.future
            if not future then
                goto continue
            end
            local result, err = future_wait(future, consts.DISCOVERY_TIMEOUT)
            if module_version ~= M.module_version then
                return
            end
            if not result then
                future:discard()
                limiter:log_warn(err, service:set_status_error(
                          'Error during discovery %s, retry will be done '..
                          'later: %s', rs_id, err))
                goto continue
            end
            local replicaset = router.replicasets[rs_id]
            if not replicaset then
                iterators[rs_id] = nil
                log.warn('Replicaset %s was removed during discovery', rs_id)
                goto continue
            end
            result = result[1]
            -- Buckets are returned as plain array by storages
            -- using old vshard version. But if .buckets is set,
            -- this is a new storage.
            discovery_handle_buckets(router, replicaset,
                                     result.buckets or result)
            local discovery_args = iter.args[1]
            discovery_args.from = result.next_from
            if not result.next_from then
                -- Nil next_from means no more buckets to get.
                -- Restart the iterator.
                iterators[rs_id] = nil
            end
            ::continue::
        end
        service:set_activity('idling')
        local unknown_bucket_count
        repeat
            unknown_bucket_count =
                router.total_bucket_count - router.known_bucket_count
            if unknown_bucket_count == 0 then
                service:set_status_ok()
                if router.discovery_mode == 'once' then
                    log.info("Discovery mode is 'once', and all is "..
                             "discovered - shut down the discovery process")
                    router.discovery_fiber = nil
                    lfiber.self():cancel()
                    return
                end
                if mode ~= 'idle' then
                    log.info('Discovery enters idle mode, all buckets are '..
                             'known. Discovery works with %s seconds '..
                             'interval now', consts.DISCOVERY_IDLE_INTERVAL)
                    mode = 'idle'
                end
                lfiber.sleep(consts.DISCOVERY_IDLE_INTERVAL)
            elseif not next(router.replicasets) then
                if mode ~= 'idle' then
                    log.info('Discovery enters idle mode because '..
                             'configuration does not have replicasets. '..
                             'Retries will happen with %s seconds interval',
                             consts.DISCOVERY_IDLE_INTERVAL)
                    mode = 'idle'
                end
                lfiber.sleep(consts.DISCOVERY_IDLE_INTERVAL)
            else
                if mode ~= 'aggressive' then
                    log.info('Start aggressive discovery, %s buckets are '..
                             'unknown. Discovery works with %s seconds '..
                             'interval', unknown_bucket_count,
                             consts.DISCOVERY_WORK_INTERVAL)
                    mode = 'aggressive'
                end
                lfiber.sleep(consts.DISCOVERY_WORK_INTERVAL)
                break
            end
            while M.errinj.ERRINJ_LONG_DISCOVERY do
                M.errinj.ERRINJ_LONG_DISCOVERY = 'waiting'
                -- Note, that the errinj won't throw FiberIsCancelled error.
                pcall(lfiber.sleep, 0.01)
            end
        until next(router.replicasets)
    end
end

local function discovery_f(router)
    local name = 'discovery'
    local service = lservice_info.new(name)
    router.discovery_service = service
    local ratelimit = lratelimit.create{name = name}
    local ok, err = pcall(discovery_service_f, router, service, ratelimit)
    if router.discovery_service == service then
        router.discovery_service = nil
    end
    if not ok then
        error(err)
    end
end

--
-- Immediately wakeup discovery fiber if exists.
--
local function discovery_wakeup(router)
    if router.discovery_fiber then
        router.discovery_fiber:wakeup()
    end
end

local function discovery_set(router, new_mode)
    local current_mode = router.discovery_mode
    if current_mode == new_mode then
        return
    end
    router.discovery_mode = new_mode
    if router.discovery_fiber ~= nil then
        pcall(router.discovery_fiber.cancel, router.discovery_fiber)
        router.discovery_fiber = nil
    end
    if new_mode == 'off' then
        return
    end
    if new_mode == 'once' and
       router.total_bucket_count == router.known_bucket_count then
        -- 'Once' discovery is supposed to stop working when all
        -- is found. But it is the case already. So nothing to do.
        return
    end
    router.discovery_fiber = util.reloadable_fiber_new(
        'vshard.discovery.' .. router.name, M, 'discovery_f', router)
end

--------------------------------------------------------------------------------
-- API
--------------------------------------------------------------------------------

local function vshard_future_tostring(self)
    return 'vshard.net.box.request'
end

local function vshard_future_serialize(self)
    -- Drop the metatable. It is also copied and if returned as is leads to
    -- recursive serialization.
    local s = setmetatable(table.deepcopy(self), {})
    s._base = nil
    return s
end

local function vshard_future_is_ready(self)
    return self._base:is_ready()
end

local function vshard_future_wrap_result(res)
    local storage_ok, err
    storage_ok, res, err = res[1], res[2], res[3]
    if storage_ok then
        if res == nil and err ~= nil then
            return nil, lerror.make(err)
        end
        return setmetatable({res}, seq_serializer)
    end
    return nil, lerror.make(res)
end

local function vshard_future_result(self)
    local res, err = self._base:result()
    if res == nil then
        return nil, lerror.make(err)
    end
    return vshard_future_wrap_result(res)
end

local function vshard_future_wait_result(self, timeout)
    local res, err = future_wait(self._base, timeout)
    if res == nil then
        return nil, lerror.make(err)
    end
    return vshard_future_wrap_result(res)
end

local function vshard_future_discard(self)
    return self._base:discard()
end

local function vshard_future_iter_next(iter, i)
    local res, err
    local base_next = iter.base_next
    local base_req = iter.base_req
    local base = iter.base
    -- Need to distinguish the last response from the pushes. Because the former
    -- has metadata returned by vshard.storage.call().
    -- At the same time there is no way to check if the base pairs() did its
    -- last iteration except calling its next() function again.
    -- This, in turn, might lead to a block if the result is not ready yet.
    i, res = base_next(base, i)
    -- To avoid that there is a 2-phase check.
    -- If the request isn't finished after first next(), it means the result is
    -- not received. This is a push. Return as is.
    -- If the request is finished, it is safe to call next() again to check if
    -- it ended. It won't block.
    local is_done = base_req:is_ready()

    if not is_done then
        -- Definitely a push. It would be finished if the final result was
        -- received.
        if i == nil then
            return nil, lerror.make(res)
        end
        return i, res
    end
    if i == nil then
        if res ~= nil then
            return i, lerror.make(res)
        end
        return nil, nil
    end
    -- Will not block because the request is already finished.
    if base_next(base, i) == nil then
        res, err = vshard_future_wrap_result(res)
        if res ~= nil then
            return i, res
        end
        return i, {nil, lerror.make(err)}
    end
    return i, res
end

local function vshard_future_pairs(self, timeout)
    local next_f, iter, i = self._base:pairs(timeout)
    return vshard_future_iter_next,
           {base = iter, base_req = self, base_next = next_f}, i
end

local vshard_future_mt = {
    __tostring = vshard_future_tostring,
    __serialize = vshard_future_serialize,
    __index = {
        is_ready = vshard_future_is_ready,
        result = vshard_future_result,
        wait_result = vshard_future_wait_result,
        discard = vshard_future_discard,
        pairs = vshard_future_pairs,
    }
}

--
-- Since 1.10 netbox supports flag 'is_async'. Given this flag, a
-- request result is returned immediately in a form of a future
-- object. Future of CALL request returns a result wrapped into an
-- array instead of unpacked values because unpacked values can
-- not be stored anywhere.
--
-- Vshard.router.call calls a user function not directly, but via
-- vshard.storage.call which returns true/false, result, errors.
-- So vshard.router.call should wrap a future object with its own
-- unpacker of result.
--
local function vshard_future_new(future)
    -- Use '_' as a prefix so as users could use all normal names.
    return setmetatable({_base = future}, vshard_future_mt)
end

-- Perform shard operation
-- Function will restart operation after wrong bucket response until timeout
-- is reached
--
local function router_call_impl(router, bucket_id, mode, prefer_replica,
                                balance, func, args, opts)
    local do_return_raw
    if opts then
        if type(opts) ~= 'table' or (opts.timeout and
           type(opts.timeout) ~= 'number') or (opts.request_timeout and
           type(opts.request_timeout) ~= 'number') then
            error('Usage: call(bucket_id, mode, func, args, opts)')
        end
        opts = table.copy(opts)
        do_return_raw = opts.return_raw
    else
        opts = {}
        do_return_raw = false
    end
    local timeout = opts.timeout or consts.CALL_TIMEOUT_MIN
    if opts.request_timeout and opts.request_timeout > timeout then
        error('request_timeout must be <= timeout')
    end
    local replicaset, err
    local tend = fiber_clock() + timeout
    if type(bucket_id) ~= 'number' then
        error('Usage: call(bucket_id, mode, func, args, opts)')
    end
    if bucket_id > router.total_bucket_count or bucket_id <= 0 then
        error('Bucket is unreachable: bucket id is out of range')
    end
    local call
    if mode == 'read' then
        if prefer_replica then
            if balance then
                call = 'callbre'
            else
                call = 'callre'
            end
        elseif balance then
            call = 'callbro'
        else
            call = 'callro'
        end
    else
        call = 'callrw'
    end
    repeat
        replicaset, err = bucket_resolve(router, bucket_id)
        if replicaset then
::replicaset_is_found::
            opts.timeout = tend - fiber_clock()
            local storage_call_status, call_status, call_error =
                replicaset[call](replicaset, 'vshard.storage.call',
                                 {bucket_id, mode, func, args}, opts)
            if do_return_raw and msgpack_is_object(storage_call_status) then
                -- Storage.call returns in the first value a flag whether user's
                -- function threw an exception or not. Need to extract it.
                -- Unfortunately, it forces to repack the rest of values into a
                -- new array. But the values themselves are not decoded.
                local it = storage_call_status:iterator()
                local count = it:decode_array_header()
                storage_call_status = it:decode()
                -- When no values, nil is not packed into msgpack object. Same
                -- as in raw netbox.
                if count > 1 then
                    call_status = it:take_array(count - 1)
                end
                call_error = nil
            end
            if storage_call_status then
                if call_status == nil and call_error ~= nil then
                    return call_status, call_error
                elseif not opts.is_async then
                    return call_status
                else
                    -- Vshard.storage.call(func) returns two
                    -- values: true/false and func result. But
                    -- async returns future object. No true/false
                    -- nor func result. So return the first value.
                    return vshard_future_new(storage_call_status)
                end
            end
            err = lerror.make(call_status)
            if err.code == lerror.code.WRONG_BUCKET or
               err.code == lerror.code.BUCKET_IS_LOCKED or
               err.code == lerror.code.TRANSFER_IS_IN_PROGRESS then
                bucket_reset(router, bucket_id)
                if err.destination then
                    replicaset = router.replicasets[err.destination]
                    if not replicaset then
                        log.warn('Replicaset "%s" was not found, but received'..
                                 ' from storage as destination - please '..
                                 'update configuration', err.destination)
                        -- Try to wait until the destination
                        -- appears. A destination can disappear,
                        -- if reconfiguration had been started,
                        -- and while is not executed on router,
                        -- but already is executed on storages.
                        while fiber_clock() <= tend do
                            lfiber.sleep(0.05)
                            replicaset = router.replicasets[err.destination]
                            if replicaset then
                                goto replicaset_is_found
                            end
                        end
                    else
                        replicaset = bucket_set(router, bucket_id,
                                                replicaset.id)
                        lfiber.yield()
                        -- Protect against infinite cycle in a
                        -- case of broken cluster, when a bucket
                        -- is sent on two replicasets to each
                        -- other.
                        if replicaset and fiber_clock() <= tend then
                            goto replicaset_is_found
                        end
                    end
                    return nil, err
                end
            elseif err.code == lerror.code.NON_MASTER then
                assert(mode == 'write')
                if not replicaset:update_master(err.replica, err.master) then
                    return nil, err
                end
            else
                return nil, err
            end
        end
        lfiber.yield()
    until fiber_clock() > tend
    if err then
        return nil, err
    else
        return nil, lerror.timeout()
    end
end

--
-- Wrappers for router_call with preset mode.
--
local function router_callro(router, bucket_id, ...)
    return router_call_impl(router, bucket_id, 'read', false, false, ...)
end

local function router_callbro(router, bucket_id, ...)
    return router_call_impl(router, bucket_id, 'read', false, true, ...)
end

local function router_callrw(router, bucket_id, ...)
    return router_call_impl(router, bucket_id, 'write', false, false, ...)
end

local function router_callre(router, bucket_id, ...)
    return router_call_impl(router, bucket_id, 'read', true, false, ...)
end

local function router_callbre(router, bucket_id, ...)
    return router_call_impl(router, bucket_id, 'read', true, true, ...)
end

local function router_call(router, bucket_id, opts, ...)
    local mode, prefer_replica, balance
    if opts then
        if type(opts) == 'string' then
            mode = opts
        elseif type(opts) == 'table' then
            mode = opts.mode or 'write'
            prefer_replica = opts.prefer_replica
            balance = opts.balance
        else
            error('Usage: router.call(bucket_id, shard_opts, func, args, opts)')
        end
    else
        mode = 'write'
    end
    return router_call_impl(router, bucket_id, mode, prefer_replica, balance,
                            ...)
end

--
-- Perform Ref stage of the Ref-Map-Reduce process on all the known replicasets.
--
local function router_ref_storage_all(router, timeout, rid)
    local replicasets = router.replicasets
    local deadline = fiber_clock() + timeout
    local err, err_id, res
    local futures = {}
    local bucket_count = 0
    local opts_async = {is_async = true}
    local rs_count = 0
    -- Nil checks are done explicitly here (== nil instead of 'not'), because
    -- netbox requests return box.NULL instead of nils.

    --
    -- Ref stage: send.
    --
    -- Netbox async requests work only with active connections. Need to wait
    -- for the connection explicitly.
    timeout, err, err_id = lreplicaset.wait_masters_connect(
        replicasets, timeout)
    if not timeout then
        goto fail
    end
    for id, rs in pairs(replicasets) do
        res, err = rs:callrw('vshard.storage._call',
                              {'storage_ref', rid, timeout}, opts_async)
        if res == nil then
            err_id = id
            goto fail
        end
        futures[id] = res
        rs_count = rs_count + 1
    end
    --
    -- Ref stage: collect.
    --
    for id, future in pairs(futures) do
        res, err = future_wait(future, timeout)
        -- Handle netbox error first.
        if res == nil then
            err_id = id
            goto fail
        end
        -- Ref returns nil,err or bucket count.
        res, err = res[1], res[2]
        if res == nil then
            err_id = id
            goto fail
        end
        bucket_count = bucket_count + res
        timeout = deadline - fiber_clock()
    end
    -- All refs are done but not all buckets are covered. This is odd and can
    -- mean many things. The most possible ones: 1) outdated configuration on
    -- the router and it does not see another replicaset with more buckets,
    -- 2) some buckets are simply lost or duplicated - could happen as a bug, or
    -- if the user does a maintenance of some kind by creating/deleting buckets.
    -- In both cases can't guarantee all the data would be covered by Map calls.
    if bucket_count ~= router.total_bucket_count then
        err = lerror.vshard(lerror.code.UNKNOWN_BUCKETS,
                            router.total_bucket_count - bucket_count)
        goto fail
    end
    do return timeout, nil, nil, replicasets end

    ::fail::
    for _, f in pairs(futures) do
        f:discard()
    end
    return nil, err, err_id, replicasets
end

--
-- Perform Ref stage of the Ref-Map-Reduce process on a subset of all the
-- replicasets, which contains all the listed bucket IDs.
--
local function router_ref_storage_by_buckets(router, bucket_ids, timeout, rid)
    local grouped_buckets
    local group_count
    local err, err_id, res
    local replicasets_all = router.replicasets
    local replicasets_to_map = {}
    local futures = {}
    local opts_async = {is_async = true}
    local deadline = fiber_clock() + timeout

    -- Nil checks are done explicitly here (== nil instead of 'not'), because
    -- netbox requests return box.NULL instead of nils.

    -- Ref stage.
    while next(bucket_ids) do
        -- Group the buckets by replicasets according to the router cache.
        grouped_buckets, err = buckets_group(router, bucket_ids, timeout)
        if grouped_buckets == nil then
            goto fail
        end
        timeout = deadline - fiber_clock()

        -- Netbox async requests work only with active connections.
        -- So, first need to wait for the master connection explicitly.
        local replicasets_to_check = {}
        group_count = 0
        for uuid, _ in pairs(grouped_buckets) do
            group_count = group_count + 1
            table.insert(replicasets_to_check, replicasets_all[uuid])
        end
        timeout, err, err_id = lreplicaset.wait_masters_connect(
            replicasets_to_check, timeout)
        if not timeout then
            goto fail
        end

        -- Send ref requests with timeouts to the replicasets.
        futures = table_new(0, group_count)
        for id, buckets in pairs(grouped_buckets) do
            if timeout == nil then
                err_id = id
                goto fail
            end
            local args_ref
            if replicasets_to_map[id] then
                -- Replicaset is already referenced on a previous iteration.
                -- Simply get the moved buckets without double referencing.
                args_ref = {
                    'storage_ref_check_with_buckets', rid, buckets}
            else
                args_ref = {
                    'storage_ref_make_with_buckets', rid, timeout, buckets}
            end
            res, err = replicasets_all[id]:callrw('vshard.storage._call',
                                                  args_ref, opts_async)
            if res == nil then
                err_id = id
                goto fail
            end
            futures[id] = res
        end

        -- Wait for the refs to be done and collect moved buckets.
        bucket_ids = {}
        for id, f in pairs(futures) do
            res, err = future_wait(f, timeout)
            -- Handle netbox error first.
            if res == nil then
                err_id = id
                goto fail
            end
            -- Ref returns nil,err or {is_done, moved}.
            res, err = res[1], res[2]
            if res == nil then
                err_id = id
                goto fail
            end
            for _, bucket in pairs(res.moved) do
                local bid = bucket.id
                local dst = bucket.dst
                -- 'Reset' regardless of 'set'. So as not to
                -- bother with 'set' errors. If it fails, then
                -- won't matter. It is a best-effort thing.
                bucket_reset(router, bid)
                if dst ~= nil then
                    bucket_set(router, bid, dst)
                end
                table.insert(bucket_ids, bid)
            end
            if res.is_done then
                assert(not replicasets_to_map[id])
                -- If there are no buckets on the replicaset, it would not be
                -- referenced.
                replicasets_to_map[id] = replicasets_all[id]
            end
            timeout = deadline - fiber_clock()
        end
    end
    do return timeout, nil, nil, replicasets_to_map end

    ::fail::
    for _, f in pairs(futures) do
        f:discard()
    end
    return nil, err, err_id, replicasets_to_map
end

--
-- Perform map-reduce stages on the given set of replicasets. The map expects a
-- valid ref ID which must be done beforehand.
--
local function replicasets_map_reduce(replicasets, rid, func, args,
                                      grouped_args, opts)
    assert(opts)
    local timeout = opts.timeout or consts.CALL_TIMEOUT_MIN
    local do_return_raw = opts.return_raw
    local deadline = fiber_clock() + timeout
    local opts_map = {is_async = true, return_raw = do_return_raw}
    -- Workaround for box.NULL in `args`.
    args = args == nil and {} or args
    local futures = {}
    local map = {}
    --
    -- Map stage: send.
    --
    local func_args = {'storage_map', rid, func, args}
    for id, rs in pairs(replicasets) do
        if grouped_args ~= nil then
            -- It's cheaper to push and then pop, rather then deepcopy
            -- arguments table for every call.
            table.insert(args, grouped_args[id])
        end
        local res, err = rs:callrw('vshard.storage._call', func_args, opts_map)
        if grouped_args ~= nil then
            table.remove(args)
        end
        if res == nil then
            return nil, err, rs.id
        end
        futures[rs.id] = res
    end
    --
    -- Map stage: collect.
    --
    if do_return_raw then
        for id, f in pairs(futures) do
            local res, err = future_wait(f, timeout)
            if res == nil then
                return nil, err, id
            end
            -- Map returns true,res or nil,err.
            res = res:iterator()
            local count = res:decode_array_header()
            local ok = res:decode()
            if ok == nil then
                return nil, res:decode(), id
            end
            if count > 1 then
                map[id] = res:take_array(count - 1)
            end
            timeout = deadline - fiber_clock()
        end
    else
        for id, f in pairs(futures) do
            local res, err = future_wait(f, timeout)
            if res == nil then
                return nil, err, id
            end
            local ok
            -- Map returns true,res or nil,err.
            ok, res = res[1], res[2]
            if ok == nil then
                return nil, res, id
            end
            if res ~= nil then
                -- Store as a table so in future it could be extended for
                -- multireturn.
                map[id] = {res}
            end
            timeout = deadline - fiber_clock()
        end
    end
    return map
end

--
-- Cancel whatever pending requests are still waiting for a response and free
-- the given ref ID on all the affected storages. This is helpful when
-- map-reduce breaks in the middle. Makes sense to let the refs go to unblock
-- the rebalancer.
--
local function replicasets_map_cancel_refs(replicasets, rid)
    local opts_async = {is_async = true}
    for _, rs in pairs(replicasets) do
        -- Best effort to remove the created refs before exiting. Can help if
        -- the timeout was big and the error happened early.
        local f = rs:callrw('vshard.storage._call',
                            {'storage_unref', rid}, opts_async)
        if f ~= nil then
            -- Don't care waiting for a result - no time for this. But it won't
            -- affect the request sending if the connection is still alive.
            f:discard()
        end
    end
end

--
-- Build table of form {<rs_id> = {<bid> = {<arguments}} from map_callrw's
-- split arguments `bucket_ids` option.
--
local function router_group_map_callrw_args(router, bucket_ids, bucket_args)
    if util.table_is_numeric(bucket_args) then
        return nil
    end

    local grouped_args = {}
    -- No need to worry about timeout here, since all buckets have already been
    -- resolved during ref stage and now the table is built only from the cache.
    local grouped_bids = buckets_group(router, bucket_ids, 0)
    assert(grouped_bids ~= nil)
    for id, bids in pairs(grouped_bids) do
        -- The arguments should be encoded as map in msgpack, not array.
        local rs_args = grouped_args[id] or setmetatable({}, map_serializer)
        for _, bid in ipairs(bids) do
            rs_args[bid] = bucket_args[bid]
        end
        grouped_args[id] = rs_args
    end
    return grouped_args
end

--
-- Consistent Map-Reduce. The given function is called on masters in the cluster
-- with a guarantee that in case of success it was executed with all buckets
-- being accessible for reads and writes.
--
-- The selection of masters depends on bucket_ids option. When specified, the
-- Map-Reduce is performed only on masters having at least one of these buckets.
-- Otherwise it is executed on all the masters in the cluster.
--
-- Consistency in scope of map-reduce means all the data was accessible, and
-- didn't move during map requests execution. To preserve the consistency there
-- is a third stage - Ref. So the algorithm is actually Ref-Map-Reduce.
--
-- Refs are broadcast before Map stage to pin the buckets to their storages, and
-- ensure they won't move until maps are done.
--
-- Map requests are broadcast in case all refs are done successfully. They
-- execute the user function + delete the refs to enable rebalancing again.
--
-- On the storages there are additional means to ensure map-reduces don't block
-- rebalancing forever and vice versa.
--
-- The function is not as slow as it may seem - it uses netbox's feature
-- is_async to send refs and maps in parallel. So cost of the function is about
-- 2 network exchanges to the most far storage in terms of time.
--
-- @param router Router instance to use.
-- @param func Name of the function to call.
-- @param args Function arguments passed in netbox style (as an array).
-- @param opts Options. See below:
--     - timeout - a number of seconds. Note that the refs may end up being kept
--         on the storages during this entire timeout if something goes wrong.
--         For instance, network issues appear. This means better not use a
--         value bigger than necessary. A stuck infinite ref can only be dropped
--         by this router restart/reconnect or the storage restart.
--     - return_raw - true/false. When specified, the returned values are not
--         decoded into Lua native objects and stay packed as a msgpack object
--         (see 'msgpack' module). By default all is decoded. That might be
--         undesirable when the returned values are going to be forwarded back
--         into the network anyway.
--     - bucket_ids - an array of bucket IDs which have to be covered by
--         Map-Reduce. By default the whole cluster is covered.
--
-- @return In case of success - a map with replicaset ID (UUID or name) keys and
--     values being what the function returned from the replicaset.
--
-- @return In case of an error - nil, error object, optional UUID or name of the
--     replicaset where the error happened. UUID or name may be not present if
--     it wasn't about concrete replicaset. For example, not all buckets were
--     found even though all replicasets were scanned.
--
local function router_map_callrw(router, func, args, opts)
    local replicasets_to_map, err, err_id, map, rid
    local timeout, do_return_raw, bucket_ids, plain_bucket_ids, grouped_args
    if opts then
        timeout = opts.timeout or consts.CALL_TIMEOUT_MIN
        do_return_raw = opts.return_raw
        bucket_ids = opts.bucket_ids
        plain_bucket_ids = util.table_is_numeric(bucket_ids) and bucket_ids or
            util.table_keys(bucket_ids)
    else
        timeout = consts.CALL_TIMEOUT_MIN
    end
    rid = M.ref_id
    M.ref_id = rid + 1
    if plain_bucket_ids then
        timeout, err, err_id, replicasets_to_map =
            router_ref_storage_by_buckets(router, plain_bucket_ids, timeout,
                                          rid)
        -- Grouped arguments are only possible with partial Map-Reduce.
        if timeout then
            grouped_args = router_group_map_callrw_args(
                router, plain_bucket_ids, bucket_ids)
        end
    else
        timeout, err, err_id, replicasets_to_map =
            router_ref_storage_all(router, timeout, rid)
    end
    if timeout then
        map, err, err_id = replicasets_map_reduce(replicasets_to_map, rid, func,
            args, grouped_args, {
                timeout = timeout, return_raw = do_return_raw
            })
        if map then
            return map
        end
    end
    replicasets_map_cancel_refs(replicasets_to_map, rid)
    err = lerror.make(err)
    return nil, err, err_id
end

--
-- Get replicaset object by bucket identifier.
-- @param bucket_id Bucket identifier.
-- @retval Netbox connection.
--
local function router_route(router, bucket_id)
    if type(bucket_id) ~= 'number' then
        error('Usage: router.route(bucket_id)')
    end
    return bucket_resolve(router, bucket_id)
end

--
-- Return map of all replicasets.
-- @retval See self.replicasets map.
--
local function router_routeall(router)
    return router.replicasets
end

--------------------------------------------------------------------------------
-- Master search
--------------------------------------------------------------------------------

local function master_search_set(router)
    local enable = false
    for _, rs in pairs(router.replicasets) do
        if rs.is_master_auto then
            enable = true
            break
        end
    end
    local service_name = 'replicaset_master_search'
    if enable then
        log.info('Master auto search is enabled')
        for _, replicaset in pairs(router.replicasets) do
            if not replicaset.worker.services[service_name] then
                replicaset.worker:add_service(service_name,
                                              {mode = 'aggressive'})
            end
        end
    else
        log.info('Master auto search is disabled')
    end
end

local function master_search_wakeup(router)
    for _, replicaset in pairs(router.replicasets) do
        replicaset.worker:wakeup_service('replicaset_master_search')
    end
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function router_cfg(router, cfg, is_reload)
    cfg = lcfg.check(cfg, router.current_cfg)
    local vshard_cfg = lcfg.extract_vshard(cfg)
    log.info('Starting router %sconfiguration at VShard %s',
             M.replicasets and 're' or '', consts.VERSION)
    if vshard_cfg.box_cfg_mode ~= 'manual' then
        local box_cfg = lcfg.extract_box(cfg, {})
        log.info("Calling box.cfg()...")
        for k, v in pairs(box_cfg) do
            log.info({[k] = v})
        end
        -- It is considered that all possible errors during cfg
        -- process occur only before this place.
        -- This check should be placed as late as possible.
        if M.errinj.ERRINJ_CFG then
            error('Error injection: cfg')
        end
        if not is_reload then
            box.cfg(box_cfg)
            log.info("Box has been configured")
            while M.errinj.ERRINJ_CFG_DELAY do
                lfiber.sleep(0.01)
            end
        end
    else
        log.info("Box configuration was skipped due to the 'manual' " ..
                 "box_cfg_mode")
    end
    local new_replicasets = lreplicaset.buildall(vshard_cfg)
    -- Move connections from an old configuration to a new one.
    -- It must be done with no yields to prevent usage both of not
    -- fully moved old replicasets, and not fully built new ones.
    lreplicaset.rebind_replicasets(new_replicasets, router.replicasets)
    -- Now the new replicasets are fully built. Can establish
    -- connections and yield.
    for _, replicaset in pairs(new_replicasets) do
        replicaset:connect_all()
    end
    lreplicaset.wait_masters_connect(new_replicasets, consts.RECONNECT_TIMEOUT)
    lreplicaset.outdate_replicasets(router.replicasets,
                                    vshard_cfg.connection_outdate_delay)
    lreplicaset.create_workers(new_replicasets)
    router.connection_outdate_delay = vshard_cfg.connection_outdate_delay
    router.total_bucket_count = vshard_cfg.bucket_count
    router.current_cfg = cfg
    router.replicasets = new_replicasets
    router.sync_timeout = vshard_cfg.sync_timeout
    local old_route_map = router.route_map
    local known_bucket_count = 0
    router.route_map = table_new(router.total_bucket_count, 0)
    for bucket, rs in pairs(old_route_map) do
        local new_rs = router.replicasets[rs.id]
        if new_rs then
            router.route_map[bucket] = new_rs
            new_rs.bucket_count = new_rs.bucket_count + 1
            known_bucket_count = known_bucket_count + 1
        end
    end
    router.known_bucket_count = known_bucket_count
    for _, replicaset in pairs(router.replicasets) do
        replicaset.worker:add_service('replicaset_failover')
        for _, replica in pairs(replicaset.replicas) do
            replica.worker:add_service('replica_failover')
        end
    end
    discovery_set(router, vshard_cfg.discovery_mode)
    master_search_set(router)
    router.is_configured = true
end

local function router_cfg_fiber_safe(router, cfg, is_reload)
    if router.is_cfg_in_progress then
        error(lerror.vshard(lerror.code.ROUTER_CFG_IS_IN_PROGRESS, router.name))
    end

    router.is_cfg_in_progress = true
    local ok, err = pcall(router_cfg, router, cfg, is_reload)
    router.is_cfg_in_progress = false
    if not ok then
        error(err)
    end
end


--------------------------------------------------------------------------------
-- Bootstrap
--------------------------------------------------------------------------------

local function cluster_bootstrap(router, opts)
    local replicasets = {}
    local count, err, last_err, ok, if_not_bootstrapped
    if opts then
        if type(opts) ~= 'table' then
            return error('Usage: vshard.router.bootstrap({<options>})')
        end
        if_not_bootstrapped = opts.if_not_bootstrapped
        opts = {timeout = opts.timeout}
        if if_not_bootstrapped == nil then
            if_not_bootstrapped = false
        end
    else
        if_not_bootstrapped = false
    end

    for _, replicaset in pairs(router.replicasets) do
        table.insert(replicasets, replicaset)
        count, err = replicaset:callrw('vshard.storage.buckets_count', {}, opts)
        if count == nil then
            -- If the client considers a bootstrapped cluster ok,
            -- then even one count > 0 is enough. So don't stop
            -- attempts after a first error. Return an error only
            -- if all replicasets responded with an error.
            if if_not_bootstrapped then
                last_err = err
            else
                return nil, err
            end
        elseif count > 0 then
            if if_not_bootstrapped then
                return true
            end
            return nil, lerror.vshard(lerror.code.NON_EMPTY)
        end
    end
    if last_err then
        return nil, err
    end
    lreplicaset.calculate_etalon_balance(router.replicasets,
                                         router.total_bucket_count)
    local bucket_id = 1
    for id, replicaset in pairs(router.replicasets) do
        if replicaset.etalon_bucket_count > 0 then
            ok, err =
                replicaset:callrw('vshard.storage.bucket_force_create',
                                  {bucket_id, replicaset.etalon_bucket_count},
                                  opts)
            if not ok then
                return nil, err
            end
            local next_bucket_id = bucket_id + replicaset.etalon_bucket_count
            log.info('Buckets from %d to %d are bootstrapped on "%s"',
                     bucket_id, next_bucket_id - 1, id)
            bucket_id = next_bucket_id
        end
    end
    return true
end

--------------------------------------------------------------------------------
-- Monitoring
--------------------------------------------------------------------------------

--
-- Collect info about a replicaset's replica with a specified
-- name. Found alerts are appended to @an alerts table, if a
-- replica does not exist or is unavailable. In a case of error
-- @a errcolor is returned, and GREEN else.
--
local function replicaset_instance_info(replicaset, name, alerts, errcolor,
                                        errcode_unreachable, params1,
                                        errcode_missing, params2)
    local info = {}
    local replica = replicaset[name]
    if replica then
        info.uri = replica:safe_uri()
        info.uuid = replica.uuid
        info.name = replica.id == replica.name and replica.name or nil
        info.network_timeout = replica.net_timeout
        if replica:is_connected() then
            info.status = 'available'
        else
            info.status = 'unreachable'
            if errcode_unreachable then
                table.insert(alerts, lerror.alert(errcode_unreachable,
                                                  unpack(params1)))
                return info, errcolor
            end
        end
    else
        info.status = 'missing'
        if errcode_missing then
            table.insert(alerts, lerror.alert(errcode_missing, unpack(params2)))
            return info, errcolor
        end
    end
    return info, consts.STATUS.GREEN
end

local function router_info(router, opts)
    local state = {
        replicasets = {},
        bucket = {
            available_ro = 0,
            available_rw = 0,
            unreachable = 0,
            unknown = 0,
        },
        alerts = {},
        status = consts.STATUS.GREEN,
    }
    local bucket_info = state.bucket
    for _, replicaset in pairs(router.replicasets) do
        -- Replicaset info parameters:
        -- * master instance info;
        -- * replica instance info;
        -- * replicaset uuid;
        -- * replicaset name (only for named identification).
        --
        -- Instance info parameters:
        -- * uri;
        -- * uuid;
        -- * name (only for named identification);
        -- * status - available, unreachable, missing;
        -- * network_timeout - timeout for requests, updated on
        --   each 10 success and 2 failed requests. The greater
        --   timeout, the worse network feels itself.
        local rs_info = {
            uuid = replicaset.uuid,
            name = replicaset.name,
            bucket = {}
        }
        state.replicasets[replicaset.id] = rs_info

        -- Build master info.
        local info, color =
            replicaset_instance_info(replicaset, 'master', state.alerts,
                                     consts.STATUS.ORANGE,
                                     -- Master exists, but not
                                     -- available.
                                     lerror.code.UNREACHABLE_MASTER,
                                     {replicaset.id, 'disconnected'},
                                     -- Master does not exists.
                                     lerror.code.MISSING_MASTER,
                                     {replicaset.id})
        state.status = math.max(state.status, color)
        rs_info.master = info

        -- Build replica info.
        if replicaset.replica ~= replicaset.master then
            info = replicaset_instance_info(replicaset, 'replica', state.alerts)
        end
        rs_info.replica = info
        if not replicaset.replica or
           (replicaset.replica and
            replicaset.replica ~= replicaset.priority_list[1]) then
            -- If the replica is not optimal, then some replicas
            -- possibly are down.
            local a = lerror.alert(lerror.code.SUBOPTIMAL_REPLICA,
                                   replicaset.id)
            table.insert(state.alerts, a)
            state.status = math.max(state.status, consts.STATUS.YELLOW)
        end

        if rs_info.replica.status ~= 'available' and
           rs_info.master.status ~= 'available' then
            local a = lerror.alert(lerror.code.UNREACHABLE_REPLICASET,
                                   replicaset.id)
            table.insert(state.alerts, a)
            state.status = consts.STATUS.RED
        end

        -- Bucket info consists of three parameters:
        -- * available_ro: how many buckets are known and
        --                 available for read requests;
        -- * available_rw: how many buckets are known and
        --                 available for both read and write
        --                 requests;
        -- * unreachable: how many buckets are known, but are not
        --                available for any requests;
        -- * unknown: how many buckets are unknown - a router
        --            doesn't know their replicasets.
        if rs_info.master.status ~= 'available' then
            if rs_info.replica.status ~= 'available' then
                rs_info.bucket.unreachable = replicaset.bucket_count
                bucket_info.unreachable = bucket_info.unreachable +
                                          replicaset.bucket_count
            else
                rs_info.bucket.available_ro = replicaset.bucket_count
                bucket_info.available_ro = bucket_info.available_ro +
                                           replicaset.bucket_count
            end
        else
            rs_info.bucket.available_rw = replicaset.bucket_count
            bucket_info.available_rw = bucket_info.available_rw +
                                       replicaset.bucket_count
        end
        -- Not necessary to update the color - it is done above
        -- during replicaset master and replica checking.
        -- If a bucket is unreachable, then replicaset is
        -- unreachable too and color already is red.

        -- Build info about working services.
        if opts and opts.with_services then
            rs_info.services = {
                failover = replicaset:service_info('failover'),
                master_search = replicaset:service_info('master_search'),
            }
        end
    end
    bucket_info.unknown = router.total_bucket_count - router.known_bucket_count
    if bucket_info.unknown > 0 then
        state.status = math.max(state.status, consts.STATUS.YELLOW)
        table.insert(state.alerts, lerror.alert(lerror.code.UNKNOWN_BUCKETS,
                                                bucket_info.unknown))
    elseif bucket_info.unknown < 0 then
        state.status = consts.STATUS.RED
        local msg = "probably router's cfg.bucket_count is different from "..
                    "storages' one, difference is "..(0 - bucket_info.unknown)
        bucket_info.unknown = '???'
        table.insert(state.alerts, lerror.alert(lerror.code.INVALID_CFG, msg))
    end
    state.identification_mode = router.current_cfg.identification_mode
    if opts and opts.with_services then
        state.services = {
            discovery = router.discovery_service and
                router.discovery_service:info(),
        }
    end
    return state
end

--
-- Build info about each bucket. Since a bucket map can be huge,
-- the function provides API to get not entire bucket map, but a
-- part.
-- @param offset Offset in a bucket map to select from.
-- @param limit Maximal bucket count in output.
-- @retval Map of type {bucket_id = 'unknown'/replicaset_id}.
--
local function router_buckets_info(router, offset, limit)
    if offset ~= nil and type(offset) ~= 'number' or
       limit ~= nil and type(limit) ~= 'number' then
        error('Usage: buckets_info(offset, limit)')
    end
    offset = offset or 0
    limit = limit or router.total_bucket_count
    local ret = {}
    -- Use one string memory for all unknown buckets.
    local available_rw = 'available_rw'
    local available_ro = 'available_ro'
    local unknown = 'unknown'
    local unreachable = 'unreachable'
    -- Collect limit.
    local first = math.max(1, offset + 1)
    local last = math.min(offset + limit, router.total_bucket_count)
    for bucket_id = first, last do
        local rs = router.route_map[bucket_id]
        if rs then
            if rs.master and rs.master:is_connected() then
                ret[bucket_id] = {uuid = rs.uuid, name = rs.name,
                                  status = available_rw}
            elseif rs.replica and rs.replica:is_connected() then
                ret[bucket_id] = {uuid = rs.uuid, name = rs.name,
                                  status = available_ro}
            else
                ret[bucket_id] = {uuid = rs.uuid, name = rs.name,
                                  status = unreachable}
            end
        else
            ret[bucket_id] = {status = unknown}
        end
    end
    return ret
end

--------------------------------------------------------------------------------
-- Other
--------------------------------------------------------------------------------

local router_bucket_id_deprecated_warn = true
local function router_bucket_id(router, key)
    if key == nil then
        error("Usage: vshard.router.bucket_id(key)")
    end
    if router_bucket_id_deprecated_warn then
        router_bucket_id_deprecated_warn = false
        log.warn('vshard.router.bucket_id() is deprecated, use '..
                 'vshard.router.bucket_id_strcrc32() or '..
                 'vshard.router.bucket_id_mpcrc32()')
    end
    return lhash.strcrc32(key) % router.total_bucket_count + 1
end

local function router_bucket_id_strcrc32(router, key)
    if key == nil then
        error("Usage: vshard.router.bucket_id_strcrc32(key)")
    end
    return lhash.strcrc32(key) % router.total_bucket_count + 1
end

local function router_bucket_id_mpcrc32(router, key)
    if key == nil then
        error("Usage: vshard.router.bucket_id_mpcrc32(key)")
    end
    return lhash.mpcrc32(key) % router.total_bucket_count + 1
end

local function router_bucket_count(router)
    return router.total_bucket_count
end

local function router_sync(router, timeout)
    if timeout ~= nil then
        if type(timeout) ~= 'number' then
            error('Usage: vshard.router.sync([timeout: number])')
        end
    else
        timeout = router.sync_timeout
    end
    local arg = {timeout}
    local deadline = timeout and (fiber_clock() + timeout)
    local opts = {timeout = timeout}
    for rs_id, replicaset in pairs(router.replicasets) do
        if timeout < 0 then
            return nil, lerror.timeout()
        end
        local status, err = replicaset:callrw('vshard.storage.sync', arg, opts)
        if not status then
            -- Add information about replicaset
            err.replicaset = rs_id
            return nil, err
        end
        timeout = deadline - fiber_clock()
        arg[1] = timeout
        opts.timeout = timeout
    end
    return true
end

--------------------------------------------------------------------------------
-- Public API protection
--------------------------------------------------------------------------------

local function router_api_call_safe(func, router, ...)
    return func(router, ...)
end

--
-- Unsafe proxy is loaded with protections. But it is used rarely and only in
-- the beginning of instance's lifetime.
--
local function router_api_call_unsafe(func, router, ...)
    -- Router can be started on instance with unconfigured box.cfg.
    if not router.is_configured then
        local msg = 'router is not configured'
        return error(lerror.vshard(lerror.code.ROUTER_IS_DISABLED, msg))
    end
    if not router.is_enabled then
        local msg = 'router is disabled explicitly'
        return error(lerror.vshard(lerror.code.ROUTER_IS_DISABLED, msg))
    end
    router.api_call_cache = router_api_call_safe
    return func(router, ...)
end

local function router_make_api(func)
    return function(router, ...)
        return router.api_call_cache(func, router, ...)
    end
end

local function router_enable(router)
    router.is_enabled = true
end

local function router_disable(router)
    router.is_enabled = false
    router.api_call_cache = router_api_call_unsafe
end

if M.errinj.ERRINJ_RELOAD then
    error('Error injection: reload')
end

--------------------------------------------------------------------------------
-- Managing router instances
--------------------------------------------------------------------------------

local router_mt = {
    __index = {
        cfg = function(router, cfg) return router_cfg_fiber_safe(router, cfg, false) end,
        info = router_make_api(router_info),
        buckets_info = router_make_api(router_buckets_info),
        call = router_make_api(router_call),
        callro = router_make_api(router_callro),
        callbro = router_make_api(router_callbro),
        callrw = router_make_api(router_callrw),
        callre = router_make_api(router_callre),
        callbre = router_make_api(router_callbre),
        map_callrw = router_make_api(router_map_callrw),
        route = router_make_api(router_route),
        routeall = router_make_api(router_routeall),
        bucket_id = router_make_api(router_bucket_id),
        bucket_id_strcrc32 = router_make_api(router_bucket_id_strcrc32),
        bucket_id_mpcrc32 = router_make_api(router_bucket_id_mpcrc32),
        bucket_count = router_make_api(router_bucket_count),
        sync = router_make_api(router_sync),
        bootstrap = router_make_api(cluster_bootstrap),
        discovery_wakeup = router_make_api(discovery_wakeup),
        master_search_wakeup = router_make_api(master_search_wakeup),
        discovery_set = router_make_api(discovery_set),
        _route_map_clear = router_make_api(route_map_clear),
        _bucket_reset = router_make_api(bucket_reset),
        _buckets_group = router_make_api(buckets_group),
        disable = router_disable,
        enable = router_enable,
    }
}

-- Table which represents this module.
local module = {}

-- This metatable bypasses calls to a module to the static_router.
local module_mt = {__index = {}}
for method_name, method in pairs(router_mt.__index) do
    module_mt.__index[method_name] = function(...)
        return method(M.static_router, ...)
    end
end

--
-- Wrap self methods with a sanity checker.
--
local mt_index = {}
for name, func in pairs(router_mt.__index) do
    mt_index[name] = util.generate_self_checker("router", name, router_mt, func)
end
router_mt.__index = mt_index

local function export_static_router_attributes()
    setmetatable(module, module_mt)
end

--
-- Create a new instance of router.
-- @param name Name of a new router.
-- @param cfg Configuration for `router_cfg`.
-- @retval Router instance.
-- @retval Nil and error object.
--
local function router_new(name, cfg)
    if type(name) ~= 'string' or type(cfg) ~= 'table' then
           error('Wrong argument type. Usage: vshard.router.new(name, cfg).')
    end
    if M.routers[name] then
        return nil, lerror.vshard(lerror.code.ROUTER_ALREADY_EXISTS, name)
    end
    local router = table.deepcopy(ROUTER_TEMPLATE)
    setmetatable(router, router_mt)
    router.api_call_cache = router_api_call_unsafe
    router.name = name
    M.routers[name] = router
    local ok, err = pcall(router_cfg_fiber_safe, router, cfg)
    if not ok then
        M.routers[name] = nil
        error(err)
    end
    return router
end

--
-- Wrapper around a `router_new` API, which allow to use old
-- static `vshard.router.cfg()` API.
--
local function legacy_cfg(cfg)
    local router = M.routers[STATIC_ROUTER_NAME]
    if not router then
        -- Create new static instance.
        local router, err = router_new(STATIC_ROUTER_NAME, cfg)
        if router then
            M.static_router = router
            module_mt.__index.static = router
            export_static_router_attributes()
        else
            return nil, err
        end
    else
        -- Reconfigure
        router_cfg_fiber_safe(router, cfg, false)
    end
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------
--
-- About functions, saved in M, and reloading see comment in
-- storage/init.lua.
--
if not rawget(_G, MODULE_INTERNALS) then
    rawset(_G, MODULE_INTERNALS, M)
else
    if not M.ref_id then
        M.ref_id = 0
    end
    for _, router in pairs(M.routers) do
        -- It's not set when reloaded from an old vshard version.
        if router.is_enabled == nil then
            router.is_enabled = true
        end
        if router.api_call_cache == nil then
            router.api_call_cache = router_api_call_unsafe
        end
        router_cfg_fiber_safe(router, router.current_cfg, true)
        setmetatable(router, router_mt)
    end
    if M.static_router then
        module_mt.__index.static = M.static_router
        export_static_router_attributes()
    end
    M.module_version = M.module_version + 1
    util.module_unload_functions(M)
end

M.discovery_f = discovery_f
M.router_mt = router_mt

module.cfg = legacy_cfg
module.new = router_new
module.internal = M
module.module_version = function() return M.module_version end

return module
