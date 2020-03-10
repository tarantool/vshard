local log = require('log')
local lfiber = require('fiber')
local table_new = require('table.new')

local MODULE_INTERNALS = '__module_vshard_router'
-- Reload requirements, in case this module is reloaded manually.
if rawget(_G, MODULE_INTERNALS) then
    local vshard_modules = {
        'vshard.consts', 'vshard.error', 'vshard.cfg',
        'vshard.hash', 'vshard.replicaset', 'vshard.util',
        'vshard.lua_gc',
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
local util = require('vshard.util')
local lua_gc = require('vshard.lua_gc')
local seq_serializer = { __serialize = 'seq' }

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        ---------------- Common module attributes ----------------
        errinj = {
            ERRINJ_CFG = false,
            ERRINJ_FAILOVER_CHANGE_CFG = false,
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
        -- Number of router which require collecting lua garbage.
        collect_lua_garbage_cnt = 0,
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
        -- Fiber to maintain replica connections.
        failover_fiber = nil,
        -- Fiber to discovery buckets in background.
        discovery_fiber = nil,
        -- Bucket count stored on all replicasets.
        total_bucket_count = 0,
        -- Boolean lua_gc state (create periodic gc task).
        collect_lua_garbage = nil,
        -- Timeout after which a ping is considered to be
        -- unacknowledged. Used by failover fiber to detect if a
        -- node is down.
        failover_ping_timeout = nil,
        --
        -- Timeout to wait sync on storages. Used by sync() call
        -- when no timeout is specified.
        --
        sync_timeout = consts.DEFAULT_SYNC_TIMEOUT,
}

local STATIC_ROUTER_NAME = '_static_router'

-- Set a bucket to a replicaset.
local function bucket_set(router, bucket_id, rs_uuid)
    local replicaset = router.replicasets[rs_uuid]
    -- It is technically possible to delete a replicaset at the
    -- same time when route to the bucket is discovered.
    if not replicaset then
        return nil, lerror.vshard(lerror.code.NO_ROUTE_TO_BUCKET, bucket_id)
    end
    local old_replicaset = router.route_map[bucket_id]
    if old_replicaset ~= replicaset then
        if old_replicaset then
            old_replicaset.bucket_count = old_replicaset.bucket_count - 1
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
    end
    router.route_map[bucket_id] = nil
end

--
-- Increase/decrease number of routers which require to collect
-- a lua garbage and change state of the `lua_gc` fiber.
--

local function lua_gc_cnt_inc()
    M.collect_lua_garbage_cnt = M.collect_lua_garbage_cnt + 1
    if M.collect_lua_garbage_cnt == 1 then
        lua_gc.set_state(true, consts.COLLECT_LUA_GARBAGE_INTERVAL)
    end
end

local function lua_gc_cnt_dec()
    M.collect_lua_garbage_cnt = M.collect_lua_garbage_cnt - 1
    assert(M.collect_lua_garbage_cnt >= 0)
    if M.collect_lua_garbage_cnt == 0 then
        lua_gc.set_state(false, consts.COLLECT_LUA_GARBAGE_INTERVAL)
    end
end

--------------------------------------------------------------------------------
-- Discovery
--------------------------------------------------------------------------------

-- Search bucket in whole cluster
local function bucket_discovery(router, bucket_id)
    local replicaset = router.route_map[bucket_id]
    if replicaset ~= nil then
        return replicaset
    end

    log.verbose("Discovering bucket %d", bucket_id)
    local last_err = nil
    local unreachable_uuid = nil
    for uuid, replicaset in pairs(router.replicasets) do
        local _, err =
            replicaset:callrw('vshard.storage.bucket_stat', {bucket_id})
        if err == nil then
            return bucket_set(router, bucket_id, replicaset.uuid)
        elseif err.code ~= lerror.code.WRONG_BUCKET then
            last_err = err
            unreachable_uuid = uuid
        end
    end
    local err = nil
    if last_err then
        if last_err.type == 'ClientError' and
           last_err.code == box.error.NO_CONNECTION then
            err = lerror.vshard(lerror.code.UNREACHABLE_REPLICASET,
                                unreachable_uuid, bucket_id)
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

-- Resolve bucket id to replicaset uuid
local function bucket_resolve(router, bucket_id)
    local replicaset, err
    local replicaset = router.route_map[bucket_id]
    if replicaset ~= nil then
        return replicaset
    end
    -- Replicaset removed from cluster, perform discovery
    replicaset, err = bucket_discovery(router, bucket_id)
    if replicaset == nil then
        return nil, err
    end
    return replicaset
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

local discovery_f

if util.version_is_at_least(1, 10, 0) then
--
-- >= 1.10 version of discovery fiber does parallel discovery of
-- all replicasets at once. It uses is_async feature of netbox
-- for that.
--
discovery_f = function(router)
    local module_version = M.module_version
    while module_version == M.module_version do
        while not next(router.replicasets) do
            lfiber.sleep(consts.DISCOVERY_INTERVAL)
        end
        if module_version ~= M.module_version then
            return
        end
        -- Just typical map reduce - send request to each
        -- replicaset in parallel, and collect responses.
        local pending = {}
        local opts = {is_async = true}
        local args = {}
        for rs_uuid, replicaset in pairs(router.replicasets) do
            local future, err =
                replicaset:callro('vshard.storage.buckets_discovery',
                                  args, opts)
            if not future then
                log.warn('Error during discovery %s: %s', rs_uuid, err)
            else
                pending[rs_uuid] = future
            end
        end

        local deadline = lfiber.clock() + consts.DISCOVERY_INTERVAL
        for rs_uuid, p in pairs(pending) do
            lfiber.yield()
            local timeout = deadline - lfiber.clock()
            local buckets, err = p:wait_result(timeout)
            while M.errinj.ERRINJ_LONG_DISCOVERY do
                M.errinj.ERRINJ_LONG_DISCOVERY = 'waiting'
                lfiber.sleep(0.01)
            end
            local replicaset = router.replicasets[rs_uuid]
            if not buckets then
                p:discard()
                log.warn('Error during discovery %s: %s', rs_uuid, err)
            elseif module_version ~= M.module_version then
                return
            elseif replicaset then
                discovery_handle_buckets(router, replicaset, buckets[1])
            end
        end

        lfiber.sleep(deadline - lfiber.clock())
    end
end

-- Version >= 1.10.
else
-- Version < 1.10.

--
-- Background fiber to perform discovery. It periodically scans
-- replicasets one by one and updates route_map.
--
discovery_f = function(router)
    local module_version = M.module_version
    while module_version == M.module_version do
        while not next(router.replicasets) do
            lfiber.sleep(consts.DISCOVERY_INTERVAL)
        end
        local old_replicasets = router.replicasets
        for rs_uuid, replicaset in pairs(router.replicasets) do
            local active_buckets, err =
                replicaset:callro('vshard.storage.buckets_discovery', {},
                                  {timeout = 2})
            while M.errinj.ERRINJ_LONG_DISCOVERY do
                M.errinj.ERRINJ_LONG_DISCOVERY = 'waiting'
                lfiber.sleep(0.01)
            end
            -- Renew replicasets object captured by the for loop
            -- in case of reconfigure and reload events.
            if router.replicasets ~= old_replicasets then
                break
            end
            if not active_buckets then
                log.error('Error during discovery %s: %s', replicaset, err)
            else
                discovery_handle_buckets(router, replicaset, active_buckets)
            end
            lfiber.sleep(consts.DISCOVERY_INTERVAL)
        end
    end
end

-- Version < 1.10.
end

--
-- Immediately wakeup discovery fiber if exists.
--
local function discovery_wakeup(router)
    if router.discovery_fiber then
        router.discovery_fiber:wakeup()
    end
end

--------------------------------------------------------------------------------
-- API
--------------------------------------------------------------------------------

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
-- Unpack a result given from a future object from
-- vshard.storage.call. If future returns [status, result, ...]
-- this function returns [result]. Or a classical couple
-- nil, error.
--
function future_storage_call_result(self)
    local res, err = self:base_result()
    if not res then
        return nil, err
    end
    local storage_call_status, call_status, call_error = unpack(res)
    if storage_call_status then
        if call_status == nil and call_error ~= nil then
            return call_status, call_error
        else
            return setmetatable({call_status}, seq_serializer)
        end
    end
    return nil, call_status
end

--
-- Given a netbox future object, redefine its 'result' method.
-- It is impossible to just create a new signle metatable per
-- the module as a copy of original future's one because it has
-- some upvalues related to the netbox connection.
--
local function wrap_storage_call_future(future)
    -- Base 'result' below is got from __index metatable under the
    -- hood. But __index is used only when a table has no such a
    -- member in itself. So via adding 'result' as a member to a
    -- future object its __index.result can be redefined.
    future.base_result = future.result
    future.result = future_storage_call_result
    return future
end

-- Perform shard operation
-- Function will restart operation after wrong bucket response until timeout
-- is reached
--
local function router_call_impl(router, bucket_id, mode, prefer_replica,
                                balance, func, args, opts)
    if opts then
        if type(opts) ~= 'table' or
           (opts.timeout and type(opts.timeout) ~= 'number') then
            error('Usage: call(bucket_id, mode, func, args, opts)')
        end
        opts = table.copy(opts)
    elseif not opts then
        opts = {}
    end
    local timeout = opts.timeout or consts.CALL_TIMEOUT_MIN
    local replicaset, err
    local tend = lfiber.time() + timeout
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
            opts.timeout = tend - lfiber.time()
            local storage_call_status, call_status, call_error =
                replicaset[call](replicaset, 'vshard.storage.call',
                                 {bucket_id, mode, func, args}, opts)
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
                    return wrap_storage_call_future(storage_call_status)
                end
            end
            err = lerror.make(call_status)
            if err.code == lerror.code.WRONG_BUCKET or
               err.code == lerror.code.BUCKET_IS_LOCKED then
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
                        while lfiber.time() <= tend do
                            lfiber.sleep(0.05)
                            replicaset = router.replicasets[err.destination]
                            if replicaset then
                                goto replicaset_is_found
                            end
                        end
                    else
                        replicaset = bucket_set(router, bucket_id,
                                                replicaset.uuid)
                        lfiber.yield()
                        -- Protect against infinite cycle in a
                        -- case of broken cluster, when a bucket
                        -- is sent on two replicasets to each
                        -- other.
                        if replicaset and lfiber.time() <= tend then
                            goto replicaset_is_found
                        end
                    end
                    return nil, err
                end
            elseif err.code == lerror.code.TRANSFER_IS_IN_PROGRESS then
                -- Do not repeat write requests, even if an error
                -- is not timeout - these requests are repeated in
                -- any case on client, if error.
                assert(mode == 'write')
                bucket_reset(router, bucket_id)
                return nil, err
            elseif err.code == lerror.code.NON_MASTER then
                -- Same, as above - do not wait and repeat.
                assert(mode == 'write')
                log.warn("Replica %s is not master for replicaset %s anymore,"..
                         "please update configuration!",
                          replicaset.master.uuid, replicaset.uuid)
                return nil, err
            else
                return nil, err
            end
        end
        lfiber.yield()
    until lfiber.time() > tend
    if err then
        return nil, err
    else
        local _, boxerror = pcall(box.error, box.error.TIMEOUT)
        return nil, lerror.box(boxerror)
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
-- Failover
--------------------------------------------------------------------------------

local function failover_ping_round(router)
    for _, replicaset in pairs(router.replicasets) do
        local replica = replicaset.replica
        if replica ~= nil and replica.conn ~= nil and
           replica.down_ts == nil then
            if not replica.conn:ping({timeout =
                                      router.failover_ping_timeout}) then
                log.info('Ping error from %s: perhaps a connection is down',
                         replica)
                -- Connection hangs. Recreate it to be able to
                -- fail over to a replica next by priority. The
                -- old connection is not closed in case if it just
                -- processes too big response at this moment. Any
                -- way it will be eventually garbage collected
                -- and closed.
                replica:detach_conn()
                replicaset:connect_replica(replica)
            end
        end
    end
end

--
-- Replicaset must fall its replica connection to lower priority,
-- if the current one is down too long.
--
local function failover_need_down_priority(replicaset, curr_ts)
    local r = replicaset.replica
    -- down_ts not nil does not mean that the replica is not
    -- connected. Probably it is connected and now fetches schema,
    -- or does authorization. Either case, it is healthy, no need
    -- to down the prio.
    return r and r.down_ts and not r:is_connected() and
           curr_ts - r.down_ts >= consts.FAILOVER_DOWN_TIMEOUT
           and r.next_by_priority
end

--
-- Once per FAILOVER_UP_TIMEOUT a replicaset must try to connect
-- to a replica with a higher priority.
--
local function failover_need_up_priority(replicaset, curr_ts)
    local up_ts = replicaset.replica_up_ts
    return not up_ts or curr_ts - up_ts >= consts.FAILOVER_UP_TIMEOUT
end

--
-- Collect UUIDs of replicasets, priority of whose replica
-- connections must be updated.
--
local function failover_collect_to_update(router)
    local ts = lfiber.time()
    local uuid_to_update = {}
    for uuid, rs in pairs(router.replicasets) do
        if failover_need_down_priority(rs, ts) or
           failover_need_up_priority(rs, ts) then
            table.insert(uuid_to_update, uuid)
        end
    end
    return uuid_to_update
end

--
-- Detect not optimal or disconnected replicas. For not optimal
-- try to update them to optimal, and down priority of
-- disconnected replicas.
-- @retval true A replica of an replicaset has been changed.
--
local function failover_step(router)
    failover_ping_round(router)
    local uuid_to_update = failover_collect_to_update(router)
    if #uuid_to_update == 0 then
        return false
    end
    local curr_ts = lfiber.time()
    local replica_is_changed = false
    for _, uuid in pairs(uuid_to_update) do
        local rs = router.replicasets[uuid]
        if M.errinj.ERRINJ_FAILOVER_CHANGE_CFG then
            rs = nil
            M.errinj.ERRINJ_FAILOVER_CHANGE_CFG = false
        end
        if rs == nil then
            log.info('Configuration has changed, restart failovering')
            lfiber.yield()
            return true
        end
        if not next(rs.replicas) then
            goto continue
        end
        local old_replica = rs.replica
        if failover_need_up_priority(rs, curr_ts) then
            rs:up_replica_priority()
        end
        if failover_need_down_priority(rs, curr_ts) then
            rs:down_replica_priority()
        end
        if old_replica ~= rs.replica then
            log.info('New replica %s for %s', rs.replica, rs)
            replica_is_changed = true
        end
::continue::
    end
    return replica_is_changed
end

--
-- Failover background function. Replica connection is the
-- connection to the nearest available server. Replica connection
-- is hold for each replicaset. This function periodically scans
-- replicasets and their replica connections. And some of them
-- appear to be disconnected or connected not to optimal replica.
--
-- If a connection is disconnected too long (more than
-- FAILOVER_DOWN_TIMEOUT), this function tries to connect to the
-- server with the lower priority. Priorities are specified in
-- weight matrix in config.
--
-- If a current replica connection has no the highest priority,
-- then this function periodically (once per FAILOVER_UP_TIMEOUT)
-- tries to reconnect to the best replica. When the connection is
-- established, it replaces the original replica.
--
local function failover_f(router)
    local module_version = M.module_version
    local min_timeout = math.min(consts.FAILOVER_UP_TIMEOUT,
                                 consts.FAILOVER_DOWN_TIMEOUT)
    -- This flag is used to avoid logging like:
    -- 'All is ok ... All is ok ... All is ok ...'
    -- each min_timeout seconds.
    local prev_was_ok = false
    while module_version == M.module_version do
::continue::
        local ok, replica_is_changed = pcall(failover_step, router)
        if not ok then
            log.error('Error during failovering: %s',
                      lerror.make(replica_is_changed))
            replica_is_changed = true
        elseif not prev_was_ok then
            log.info('All replicas are ok')
        end
        prev_was_ok = not replica_is_changed
        local logf
        if replica_is_changed then
            logf = log.info
        else
            -- In any case it is necessary to periodically log
            -- failover heartbeat.
            logf = log.verbose
        end
        logf('Failovering step is finished. Schedule next after %f seconds',
             min_timeout)
        lfiber.sleep(min_timeout)
    end
end

--------------------------------------------------------------------------------
-- Configuration
--------------------------------------------------------------------------------

local function router_cfg(router, cfg, is_reload)
    cfg = lcfg.check(cfg, router.current_cfg)
    local vshard_cfg, box_cfg = lcfg.split(cfg)
    if not M.replicasets then
        log.info('Starting router configuration')
    else
        log.info('Starting router reconfiguration')
    end
    local new_replicasets = lreplicaset.buildall(vshard_cfg)
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
    end
    -- Move connections from an old configuration to a new one.
    -- It must be done with no yields to prevent usage both of not
    -- fully moved old replicasets, and not fully built new ones.
    lreplicaset.rebind_replicasets(new_replicasets, router.replicasets)
    -- Now the new replicasets are fully built. Can establish
    -- connections and yield.
    for _, replicaset in pairs(new_replicasets) do
        replicaset:connect_all()
    end
    -- Change state of lua GC.
    if vshard_cfg.collect_lua_garbage and not router.collect_lua_garbage then
        lua_gc_cnt_inc()
    elseif not vshard_cfg.collect_lua_garbage and
       router.collect_lua_garbage then
        lua_gc_cnt_dec()
    end
    lreplicaset.wait_masters_connect(new_replicasets)
    lreplicaset.outdate_replicasets(router.replicasets,
                                    vshard_cfg.connection_outdate_delay)
    router.connection_outdate_delay = vshard_cfg.connection_outdate_delay
    router.total_bucket_count = vshard_cfg.bucket_count
    router.collect_lua_garbage = vshard_cfg.collect_lua_garbage
    router.current_cfg = cfg
    router.replicasets = new_replicasets
    router.failover_ping_timeout = vshard_cfg.failover_ping_timeout
    router.sync_timeout = vshard_cfg.sync_timeout
    local old_route_map = router.route_map
    router.route_map = table_new(router.total_bucket_count, 0)
    for bucket, rs in pairs(old_route_map) do
        local new_rs = router.replicasets[rs.uuid]
        if new_rs then
            router.route_map[bucket] = new_rs
            new_rs.bucket_count = new_rs.bucket_count + 1
        end
    end
    if router.failover_fiber == nil then
        router.failover_fiber = util.reloadable_fiber_create(
            'vshard.failover.' .. router.name, M, 'failover_f', router)
    end
    if router.discovery_fiber == nil then
        router.discovery_fiber = util.reloadable_fiber_create(
            'vshard.discovery.' .. router.name, M, 'discovery_f', router)
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

    for uuid, replicaset in pairs(router.replicasets) do
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
    for uuid, replicaset in pairs(router.replicasets) do
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
                     bucket_id, next_bucket_id - 1, uuid)
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

local function router_info(router)
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
    local known_bucket_count = 0
    for rs_uuid, replicaset in pairs(router.replicasets) do
        -- Replicaset info parameters:
        -- * master instance info;
        -- * replica instance info;
        -- * replicaset uuid.
        --
        -- Instance info parameters:
        -- * uri;
        -- * uuid;
        -- * status - available, unreachable, missing;
        -- * network_timeout - timeout for requests, updated on
        --   each 10 success and 2 failed requests. The greater
        --   timeout, the worse network feels itself.
        local rs_info = {
            uuid = replicaset.uuid,
            bucket = {}
        }
        state.replicasets[replicaset.uuid] = rs_info

        -- Build master info.
        local info, color =
            replicaset_instance_info(replicaset, 'master', state.alerts,
                                     consts.STATUS.ORANGE,
                                     -- Master exists, but not
                                     -- available.
                                     lerror.code.UNREACHABLE_MASTER,
                                     {replicaset.uuid, 'disconnected'},
                                     -- Master does not exists.
                                     lerror.code.MISSING_MASTER,
                                     {replicaset.uuid})
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
                                   replicaset.uuid)
            table.insert(state.alerts, a)
            state.status = math.max(state.status, consts.STATUS.YELLOW)
        end

        if rs_info.replica.status ~= 'available' and
           rs_info.master.status ~= 'available' then
            local a = lerror.alert(lerror.code.UNREACHABLE_REPLICASET,
                                   replicaset.uuid)
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
        known_bucket_count = known_bucket_count + replicaset.bucket_count
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
        -- No necessarity to update color - it is done above
        -- during replicaset master and replica checking.
        -- If a bucket is unreachable, then replicaset is
        -- unreachable too and color already is red.
    end
    bucket_info.unknown = router.total_bucket_count - known_bucket_count
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
    return state
end

--
-- Build info about each bucket. Since a bucket map can be huge,
-- the function provides API to get not entire bucket map, but a
-- part.
-- @param offset Offset in a bucket map to select from.
-- @param limit Maximal bucket count in output.
-- @retval Map of type {bucket_id = 'unknown'/replicaset_uuid}.
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
                ret[bucket_id] = {uuid = rs.uuid, status = available_rw}
            elseif rs.replica and rs.replica:is_connected() then
                ret[bucket_id] = {uuid = rs.uuid, status = available_ro}
            else
                ret[bucket_id] = {uuid = rs.uuid, status = unreachable}
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
    local clock = lfiber.clock
    local deadline = timeout and (clock() + timeout)
    local opts = {timeout = timeout}
    for rs_uuid, replicaset in pairs(router.replicasets) do
        if timeout < 0 then
            return nil, box.error.new(box.error.TIMEOUT)
        end
        local status, err = replicaset:callrw('vshard.storage.sync', arg, opts)
        if not status then
            -- Add information about replicaset
            err.replicaset = rs_uuid
            return nil, err
        end
        timeout = deadline - clock()
        arg[1] = timeout
        opts.timeout = timeout
    end
    return true
end

if M.errinj.ERRINJ_RELOAD then
    error('Error injection: reload')
end

--------------------------------------------------------------------------------
-- Managing router instances
--------------------------------------------------------------------------------

local router_mt = {
    __index = {
        cfg = function(router, cfg) return router_cfg(router, cfg, false) end,
        info = router_info;
        buckets_info = router_buckets_info;
        call = router_call;
        callro = router_callro;
        callbro = router_callbro;
        callrw = router_callrw;
        callre = router_callre;
        callbre = router_callbre;
        route = router_route;
        routeall = router_routeall;
        bucket_id = router_bucket_id,
        bucket_id_strcrc32 = router_bucket_id_strcrc32,
        bucket_id_mpcrc32 = router_bucket_id_mpcrc32,
        bucket_count = router_bucket_count;
        sync = router_sync;
        bootstrap = cluster_bootstrap;
        bucket_discovery = bucket_discovery;
        discovery_wakeup = discovery_wakeup;
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
    router.name = name
    M.routers[name] = router
    local ok, err = pcall(router_cfg, router, cfg)
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
    if M.static_router then
        -- Reconfigure.
        router_cfg(M.static_router, cfg, false)
    else
        -- Create new static instance.
        local router, err = router_new(STATIC_ROUTER_NAME, cfg)
        if router then
            M.static_router = router
            module_mt.__index.static = router
            export_static_router_attributes()
        else
            return nil, err
        end
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
    for _, router in pairs(M.routers) do
        router_cfg(router, router.current_cfg, true)
        setmetatable(router, router_mt)
    end
    if M.static_router then
        module_mt.__index.static = M.static_router
        export_static_router_attributes()
    end
    M.module_version = M.module_version + 1
end

M.discovery_f = discovery_f
M.failover_f = failover_f
M.router_mt = router_mt

module.cfg = legacy_cfg
module.new = router_new
module.internal = M
module.module_version = function() return M.module_version end

return module
