-- vshard.replicaset

--
-- <replicaset> = {
--     replicas = {
--         [replica_uuid] = {
--             uri = string,
--             name = string,
--             uuid = string,
--             conn = <netbox> + .replica + .replicaset,
--             zone = number,
--             next_by_priority = <replica object of the same type>,
--             weight = number,
--             down_ts = <timestamp of disconnect from the
--                        replica>,
--             backoff_ts = <timestamp when was sent into backoff state>,
--             backoff_err = <error object caused the backoff>,
--             net_timeout = <current network timeout for calls,
--                            doubled on each network fail until
--                            max value, and reset to minimal
--                            value on each success>,
--             net_sequential_ok = <count of sequential success
--                                  requests to the replica>,
--             net_sequential_fail = <count of sequential failed
--                                    requests to the replica>,
--             is_outdated = nil/true,
--          }
--      },
--      master = <master server from the array above>,
--      master_cond = <condition variable signaled when the replicaset finds or
--                     changes its master>,
--      is_auto_master = <true when is configured to find the master on
--                        its own>,
--      replica = <nearest available replica object>,
--      balance_i = <index of a next replica in priority_list to
--                   use for a load-balanced request>,
--      replica_up_ts = <timestamp updated on each attempt to
--                       connect to the nearest replica, and on
--                       each connect event>,
--      uuid = <replicaset_uuid>,
--      weight = number,
--      priority_list = <list of replicas, sorted by weight asc>,
--      etalon_bucket_count = <bucket count, that must be stored
--                             on this replicaset to reach the
--                             balance in a cluster>,
--      is_outdated = nil/true,
--  }
--
-- replicasets = {
--    [replicaset_uuid] = <replicaset>
-- }
--

local log = require('log')
local netbox = require('net.box')
local consts = require('vshard.consts')
local lerror = require('vshard.error')
local fiber = require('fiber')
local luri = require('uri')
local luuid = require('uuid')
local ffi = require('ffi')
local util = require('vshard.util')
local fiber_clock = fiber.clock
local fiber_yield = fiber.yield
local fiber_cond_wait = util.fiber_cond_wait
local gsc = util.generate_self_checker

--
-- on_connect() trigger for net.box
--
local function netbox_on_connect(conn)
    log.info("connected to %s:%s", conn.host, conn.port)
    local rs = conn.replicaset
    local replica = conn.replica
    assert(replica ~= nil)
    -- If a replica's connection has revived, then unset
    -- replica.down_ts - it is not down anymore.
    replica.down_ts = nil
    if conn.peer_uuid ~= replica.uuid and
        -- XXX: Zero UUID means not a real Tarantool instance. It
        -- is likely to be a cartridge.remote-control server,
        -- which is started before the actual storage. Let it
        -- work, anyway it will be shut down, and reconnect to the
        -- real storage will happen. Otherwise the connection will
        -- be left broken in 'closed' state until a request will
        -- come specifically for this instance, or reconfiguration
        -- will happen. That would prevent reconnect to the real
        -- storage.
       conn.peer_uuid ~= luuid.NULL:str() then
        log.info('Mismatch server UUID on replica %s: expected "%s", but got '..
                 '"%s"', replica, replica.uuid, conn.peer_uuid)
        conn:close()
        return
    end
    if replica == rs.replica and replica == rs.priority_list[1] then
        -- Update replica_up_ts, if the current replica has the
        -- biggest priority. Really, it is not neccessary to
        -- increase replica connection priority, if the current
        -- one already has the biggest priority. (See failover_f).
        rs.replica_up_ts = fiber_clock()
    end
end

--
-- on_disconnect() trigger for net.box
--
local function netbox_on_disconnect(conn)
    log.info("disconnected from %s:%s", conn.host, conn.port)
    assert(conn.replica)
    -- Replica is down - remember this time to decrease replica
    -- priority after FAILOVER_DOWN_TIMEOUT seconds.
    conn.replica.down_ts = fiber_clock()
end

--
-- Wait until the connection is established. This is necessary at least for
-- async requests because they fail immediately if the connection is not done.
-- Returns the remaining timeout because is expected to be used to connect to
-- many instances in a loop, where such return saves one clock get in the caller
-- code and is just cleaner code.
--
local function netbox_wait_connected(conn, timeout)
    -- Fast path. Usually everything is connected.
    if conn:is_connected() then
        return timeout
    end
    local deadline = fiber_clock() + timeout
    -- Loop against spurious wakeups.
    repeat
        -- Netbox uses fiber_cond inside, which throws an irrelevant usage error
        -- at negative timeout. Need to check the case manually.
        if timeout < 0 then
            return nil, lerror.timeout()
        end
        local ok, res = pcall(conn.wait_connected, conn, timeout)
        if not ok then
            return nil, lerror.make(res)
        end
        if not res then
            return nil, lerror.timeout()
        end
        timeout = deadline - fiber_clock()
    until conn:is_connected()
    return timeout
end

--
-- Check if the replica is not in backoff. It also serves as an update - if the
-- replica still has an old backoff timestamp, it is cleared. This way of
-- backoff update does not require any fibers to perform background updates.
-- Hence works not only on the router.
--
local function replica_check_backoff(replica, now)
    if not replica.backoff_ts then
        return true
    end
    if replica.backoff_ts + consts.REPLICA_BACKOFF_INTERVAL > now then
        return false
    end
    log.warn('Replica %s returns from backoff', replica.uuid)
    replica.backoff_ts = nil
    replica.backoff_err = nil
    return true
end

--
-- Connect to a specified replica and remember a new connection
-- in the replica object. Note, that the function does not wait
-- until a connection is established.
--
local function replicaset_connect_to_replica(replicaset, replica)
    local conn = replica.conn
    if not conn or conn.state == 'closed' then
        conn = netbox.connect(replica.uri, {
            reconnect_after = consts.RECONNECT_TIMEOUT,
            wait_connected = false
        })
        conn.replica = replica
        conn.replicaset = replicaset
        conn.on_connect_ref = netbox_on_connect
        conn:on_connect(netbox_on_connect)
        conn.on_disconnect_ref = netbox_on_disconnect
        conn:on_disconnect(netbox_on_disconnect)
        replica.conn = conn
    end
    return conn
end

local function replicaset_wait_master(replicaset, timeout)
    local master = replicaset.master
    -- Fast path - master is almost always known.
    if master then
        return master, timeout
    end
    -- Slow path.
    local deadline = fiber_clock() + timeout
    repeat
        if not replicaset.is_auto_master or
           not fiber_cond_wait(replicaset.master_cond, timeout) then
            return nil, lerror.vshard(lerror.code.MISSING_MASTER,
                                      replicaset.uuid)
        end
        timeout = deadline - fiber_clock()
        master = replicaset.master
    until master
    return master, timeout
end

--
-- Create net.box connection to master.
--
local function replicaset_connect_master(replicaset)
    local master = replicaset.master
    if master == nil then
        return nil, lerror.vshard(lerror.code.MISSING_MASTER,
                                  replicaset.uuid)
    end
    return replicaset_connect_to_replica(replicaset, master)
end

--
-- Wait until the master instance is connected.
--
local function replicaset_wait_connected(replicaset, timeout)
    local master
    master, timeout = replicaset_wait_master(replicaset, timeout)
    if not master then
        return nil, timeout
    end
    local conn = replicaset_connect_to_replica(replicaset, master)
    return netbox_wait_connected(conn, timeout)
end

--
-- Create net.box connections to all replicas and master.
--
local function replicaset_connect_all(replicaset)
    for _, replica in pairs(replicaset.replicas) do
        replicaset_connect_to_replica(replicaset, replica)
    end
end

--
-- Connect to a next replica with less priority against a current
-- one. It is needed, if a current replica's connection is down
-- too long.
--
local function replicaset_down_replica_priority(replicaset)
    local old_replica = replicaset.replica
    assert(old_replica and old_replica.down_ts and
           not old_replica:is_connected())
    local new_replica = old_replica.next_by_priority
    if new_replica then
        assert(new_replica ~= old_replica)
        replicaset_connect_to_replica(replicaset, new_replica)
        replicaset.replica = new_replica
    end
    -- Else the current replica already has the lowest priority.
    -- Can not down it.
end

--
-- Search a replica with higher priority than a current replica
-- has.
--
local function replicaset_up_replica_priority(replicaset)
    local old_replica = replicaset.replica
    if old_replica == replicaset.priority_list[1] and
       old_replica:is_connected() then
        replicaset.replica_up_ts = fiber_clock()
        return
    end
    for _, replica in pairs(replicaset.priority_list) do
        if replica == old_replica then
            -- Failed to up priority.
            return
        end
        if replica:is_connected() then
            replicaset.replica = replica
            assert(not old_replica or
                   old_replica.weight >= replicaset.replica.weight)
            return
        end
    end
end

--
-- Handler for failed request to a replica. It increments count
-- of sequentially failed requests. When it reaches 2, it
-- increases network timeout twice.
--
local function replica_on_failed_request(replica)
    replica.net_sequential_ok = 0
    local val = replica.net_sequential_fail + 1
    if val >= 2 then
        local new_timeout = replica.net_timeout * 2
        if new_timeout <= consts.CALL_TIMEOUT_MAX then
            replica.net_timeout = new_timeout
        end
        replica.net_sequential_fail = 1
    else
        replica.net_sequential_fail = val
    end
end

--
-- Same, as above, but for success request. And when count of
-- success requests reaches 10, the network timeout is decreased
-- to minimal timeout.
--
local function replica_on_success_request(replica)
    replica.net_sequential_fail = 0
    local val = replica.net_sequential_ok + 1
    if val >= 10 then
        replica.net_timeout = consts.CALL_TIMEOUT_MIN
        replica.net_sequential_ok = 1
    else
        replica.net_sequential_ok = val
    end
end

--
-- Call a function on a replica using its connection. The typical
-- usage is calls under storage.call, because of which there
-- are no more than 3 return values. It is because storage.call
-- returns:
-- * true/nil for storage.call();
-- * error object, if storage.call() was not ok, or called
--   function retval;
-- * error object, if called function has been failed, or nil
--   else.
-- @retval  true, ... The correct response is received.
-- @retval false, ... Response is not received. It can be timeout
--         or unexpectedly closed connection.
--
local function replica_call(replica, func, args, opts)
    assert(opts and opts.timeout)
    local conn = replica.conn
    local net_status, storage_status, retval, error_object =
        pcall(conn.call, conn, func, args, opts)
    if not net_status then
        -- Do not increase replica's network timeout, if the
        -- requested one was less, than network's one. For
        -- example, if replica's timeout was 30s, but an user
        -- specified 1s and it was expired, then there is no
        -- reason to increase network timeout.
        if opts.timeout >= replica.net_timeout then
            replica_on_failed_request(replica)
        end
        local err = storage_status
        -- VShard functions can throw exceptions using error() function. When
        -- it reaches the network layer, it is wrapped into LuajitError. Try to
        -- extract the original error if this is the case. Not always is
        -- possible - the string representation could be truncated.
        --
        -- In old Tarantool versions LuajitError turned into ClientError on the
        -- client. Check both types.
        if func:startswith('vshard.') and (err.type == 'LuajitError' or
           err.type == 'ClientError') then
            err = lerror.from_string(err.message) or err
        end
        log.error("Exception during calling '%s' on '%s': %s", func, replica,
                  err)
        return false, nil, lerror.make(err)
    else
        replica_on_success_request(replica)
    end
    if storage_status == nil then
        -- Workaround for `not msgpack.NULL` magic.
        storage_status = nil
    end
    return true, storage_status, retval, error_object
end

--
-- Detach the connection object from its replica object.
-- Detachment means that the connection is not closed, but all its
-- links with the replica are teared. All current requests are
-- finished, but next calls on this replica are processed by
-- another connection.
-- Initially this function is intended for failover, which should
-- not close the old connection in case if it receives a huge
-- response and because of it ignores pings.
--
local function replica_detach_conn(replica)
    local c = replica.conn
    if c ~= nil then
        -- The connection now has nothing to do with the replica
        -- object. In particular, it shall not touch up and down
        -- ts.
        c:on_connect(nil, c.on_connect_ref)
        c.on_connect_ref = nil
        c:on_disconnect(nil, c.on_disconnect_ref)
        c.on_disconnect_ref = nil
        -- Detach looks like disconnect for an observer.
        netbox_on_disconnect(c)
        c.replica = nil
        c.replicaset = nil
        replica.conn = nil
    end
end

--
-- Call a function on remote storage
-- Note: this function uses pcall-style error handling
-- @retval false, err on error
-- @retval true, ... on success
--
local function replicaset_master_call(replicaset, func, args, opts)
    assert(opts == nil or type(opts) == 'table')
    assert(type(func) == 'string', 'function name')
    assert(args == nil or type(args) == 'table', 'function arguments')
    local master = replicaset.master
    if not master then
        opts = opts and table.copy(opts) or {}
        if opts.is_async then
            return nil, lerror.vshard(lerror.code.MISSING_MASTER,
                                      replicaset.uuid)
        end
        local timeout = opts.timeout or consts.MASTER_SEARCH_TIMEOUT
        master, timeout = replicaset_wait_master(replicaset, timeout)
        if not master then
            return nil, timeout
        end
        opts.timeout = master.net_timeout
    else
        if not opts then
            opts = {timeout = master.net_timeout}
        elseif not opts.timeout then
            opts = table.copy(opts)
            opts.timeout = master.net_timeout
        end
    end
    replicaset_connect_to_replica(replicaset, master)
    local net_status, storage_status, retval, error_object =
        replica_call(master, func, args, opts)
    -- Ignore net_status - master does not retry requests.
    return storage_status, retval, error_object
end

--
-- True, if after error @a e a read request can be retried.
--
local function can_retry_after_error(e)
    if not e or (type(e) ~= 'table' and
                 (type(e) ~= 'cdata' or not ffi.istype('struct error', e))) then
        return false
    end
    if e.type == 'ShardingError' and
       (e.code == lerror.code.WRONG_BUCKET or
        e.code == lerror.code.TRANSFER_IS_IN_PROGRESS) then
        return true
    end
    return e.type == 'ClientError' and e.code == box.error.TIMEOUT
end

--
-- True if after the given error on call of the given function the connection
-- must go into backoff.
--
local function can_backoff_after_error(e, func)
    if not e then
        return false
    end
    if type(e) ~= 'table' and
       (type(e) ~= 'cdata' or not ffi.istype('struct error', e)) then
        return false
    end
    -- So far it is enabled only for vshard's own functions. Including
    -- vshard.storage.call(). Otherwise it is not possible to retry safely -
    -- user's function could have side effects before raising that error.
    -- For instance, 'access denied' could be raised by user's function
    -- internally after it already changed some data on the storage.
    if not func:startswith('vshard.') then
        return false
    end
    -- ClientError is sent for all errors by old Tarantool versions which didn't
    -- keep error type. New versions preserve the original error type.
    if e.type == 'ClientError' or e.type == 'AccessDeniedError' then
        if e.code == box.error.ACCESS_DENIED then
            return e.message:startswith("Execute access to function 'vshard.")
        end
        if e.code == box.error.NO_SUCH_PROC then
            return e.message:startswith("Procedure 'vshard.")
        end
    end
    if e.type == 'ShardingError' then
        return e.code == vshard.error.code.STORAGE_IS_DISABLED
    end
    return false
end

--
-- Pick a next replica according to round-robin load balancing
-- policy.
--
local function replicaset_balance_replica(replicaset)
    local i = replicaset.balance_i
    local pl = replicaset.priority_list
    local size = #pl
    replicaset.balance_i = i % size + 1
    assert(i <= size)
    return pl[i]
end

--
-- Template to implement a function able to visit multiple
-- replicas with certain details. One of applicatinos - a function
-- making a call on a nearest available replica. It is possible
-- for 'read' requests only. And if the nearest replica is not
-- available now, then use master's connection - we can not wait
-- until failover fiber will repair the nearest connection.
--
local function replicaset_template_multicallro(prefer_replica, balance)
    local function pick_next_replica(replicaset, now)
        local r
        local master = replicaset.master
        if balance then
            local i = #replicaset.priority_list
            while i > 0 do
                r = replicaset_balance_replica(replicaset)
                i = i - 1
                if r:is_connected() and (not prefer_replica or r ~= master) and
                   replica_check_backoff(r, now) then
                    return r
                end
            end
        else
            local start_r = replicaset.replica
            r = start_r
            while r do
                if r:is_connected() and (not prefer_replica or r ~= master) and
                   replica_check_backoff(r, now) then
                    return r
                end
                r = r.next_by_priority
            end
            -- Iteration above could start not from the best prio replica.
            -- Check the beginning of the list too.
            for _, r in ipairs(replicaset.priority_list) do
                if r == start_r then
                    -- Reached already checked part.
                    break
                end
                if r:is_connected() and (not prefer_replica or r ~= master) and
                   replica_check_backoff(r, now) then
                    return r
                end
            end
        end
    end

    return function(replicaset, func, args, opts)
        assert(opts == nil or type(opts) == 'table')
        assert(type(func) == 'string', 'function name')
        assert(args == nil or type(args) == 'table', 'function arguments')
        opts = opts and table.copy(opts) or {}
        local timeout = opts.timeout or consts.CALL_TIMEOUT_MAX
        local net_status, storage_status, retval, err, replica
        if timeout <= 0 then
            return nil, lerror.timeout()
        end
        local now = fiber_clock()
        local end_time = now + timeout
        while not net_status and timeout > 0 do
            replica = pick_next_replica(replicaset, now)
            if not replica then
                replica, timeout = replicaset_wait_master(replicaset, timeout)
                if not replica then
                    return nil, timeout
                end
                replicaset_connect_to_replica(replicaset, replica)
                if replica.backoff_ts then
                    return nil, lerror.vshard(
                        lerror.code.REPLICASET_IN_BACKOFF, replicaset.uuid,
                        replica.backoff_err)
                end
            end
            opts.timeout = timeout
            net_status, storage_status, retval, err =
                replica_call(replica, func, args, opts)
            now = fiber_clock()
            timeout = end_time - now
            if not net_status and not storage_status and
               not can_retry_after_error(retval) then
                if can_backoff_after_error(retval, func) then
                    if not replica.backoff_ts then
                        log.warn('Replica %s goes into backoff for %s sec '..
                                 'after error %s', replica.uuid,
                                 consts.REPLICA_BACKOFF_INTERVAL, retval)
                        replica.backoff_ts = now
                        replica.backoff_err = retval
                    end
                else
                    -- There is no sense to retry LuaJit errors, such as
                    -- assertions, undefined variables etc.
                    net_status = true
                    break
                end
            end
        end
        if not net_status then
            return nil, lerror.make(retval)
        else
            return storage_status, retval, err
        end
    end
end

--
-- Nice formatter for replicaset
--
local function replicaset_tostring(replicaset)
    local master
    if replicaset.master then
        master = replicaset.master
    else
        master = 'missing'
    end
    return string.format('replicaset(uuid="%s", master=%s)', replicaset.uuid,
                         master)
end

--
-- Copy netbox connections from old replica objects to new ones
-- and outdate old objects.
-- @param replicasets New replicasets
-- @param old_replicasets Replicasets and replicas to be outdated.
--
local function rebind_replicasets(replicasets, old_replicasets)
    for replicaset_uuid, replicaset in pairs(replicasets) do
        local old_replicaset = old_replicasets and
                               old_replicasets[replicaset_uuid]
        for replica_uuid, replica in pairs(replicaset.replicas) do
            local old_replica = old_replicaset and
                                old_replicaset.replicas[replica_uuid]
            if old_replica and old_replica.uri == replica.uri then
                local conn = old_replica.conn
                replica.conn = conn
                replica.down_ts = old_replica.down_ts
                replica.backoff_ts = old_replica.backoff_ts
                replica.backoff_err = old_replica.backoff_err
                replica.net_timeout = old_replica.net_timeout
                replica.net_sequential_ok = old_replica.net_sequential_ok
                replica.net_sequential_fail = old_replica.net_sequential_fail
                if conn then
                    conn.replica = replica
                    conn.replicaset = replicaset
                end
            end
        end
        if old_replicaset then
            -- Take a hint from the old replicaset who is the master now.
            if replicaset.is_auto_master then
                local master = old_replicaset.master
                if master then
                    replicaset.master = replicaset.replicas[master.uuid]
                end
            end
            -- Stop waiting for master in the old replicaset. Its running
            -- requests won't find it anyway. Auto search works only for the
            -- most actual replicaset objects.
            if old_replicaset.is_auto_master then
                old_replicaset.is_auto_master = false
                old_replicaset.master_cond:broadcast()
            end
        end
    end
end

--
-- Let the replicaset know @a old_master_uuid is not a master anymore, should
-- use @a candidate_uuid instead.
-- Returns whether the request, which brought this information, can be retried.
--
local function replicaset_update_master(replicaset, old_master_uuid,
                                        candidate_uuid)
    local is_auto = replicaset.is_auto_master
    local replicaset_uuid = replicaset.uuid
    if old_master_uuid == candidate_uuid then
        -- It should not happen ever, but be ready to everything.
        log.warn('Replica %s in replicaset %s reports self as both master '..
                 'and not master', master_uuid, replicaset_uuid)
        return is_auto
    end
    local master = replicaset.master
    if not master then
        if not is_auto or not candidate_uuid then
            return is_auto
        end
        local candidate = replicaset.replicas[candidate_uuid]
        if not candidate then
            return true
        end
        replicaset.master = candidate
        log.info('Replica %s becomes a master as reported by %s for '..
                 'replicaset %s', candidate_uuid, old_master_uuid,
                 replicaset_uuid)
        return true
    end
    local master_uuid = master.uuid
    if master_uuid ~= old_master_uuid then
        -- Master was changed while the master update information was coming.
        -- It means it is outdated and should be ignored.
        -- Return true regardless of the auto-master config. Because the master
        -- change means the caller's request has a chance to succeed on the new
        -- master on retry.
        return true
    end
    if not is_auto then
        log.warn('Replica %s is not master for replicaset %s anymore, please '..
                 'update configuration', master_uuid, replicaset_uuid)
        return false
    end
    if not candidate_uuid then
        replicaset.master = nil
        log.warn('Replica %s is not a master anymore for replicaset %s, no '..
                 'candidate was reported', master_uuid, replicaset_uuid)
        return true
    end
    local candidate = replicaset.replicas[candidate_uuid]
    if candidate then
        replicaset.master = candidate
        log.info('Replica %s becomes master instead of %s for replicaset %s',
                 candidate_uuid, master_uuid, replicaset_uuid)
    else
        replicaset.master = nil
        log.warn('Replica %s is not a master anymore for replicaset %s, new '..
                 'master %s could not be found in the config',
                 master_uuid, replicaset_uuid, candidate_uuid)
    end
    return true
end

--
-- Check if the master is still master, and find a new master if there is no a
-- known one.
--
local function replicaset_locate_master(replicaset)
    local is_done = true
    local is_nop = true
    if not replicaset.is_auto_master then
        return is_done, is_nop
    end
    local func = 'vshard.storage._call'
    local args = {'info'}
    local const_timeout = consts.MASTER_SEARCH_TIMEOUT
    local ok, res, err, f
    local master = replicaset.master
    if master then
        local sync_opts = {timeout = const_timeout}
        ok, res, err = replica_call(master, func, args, sync_opts)
        if not ok then
            return is_done, is_nop, err
        end
        if res.is_master then
            return is_done, is_nop
        end
        -- Could be changed during the call from the outside. For
        -- instance, by a failed request with a hint from the old
        -- master.
        local cur_master = replicaset.master
        if cur_master == master then
            log.info('Master of replicaset %s, node %s, has resigned. Trying '..
                     'to find a new one', replicaset.uuid, master.uuid)
            replicaset.master = nil
        elseif cur_master then
            -- Another master was already found. But check it via another call
            -- later to avoid an infinite loop here.
            return is_done, is_nop
        end
    end
    is_nop = false

    local last_err
    local futures = {}
    local timeout = const_timeout
    local deadline = fiber_clock() + timeout
    local async_opts = {is_async = true}
    local replicaset_uuid = replicaset.uuid
    for replica_uuid, replica in pairs(replicaset.replicas) do
        local conn = replica.conn
        if conn:is_connected() then
            ok, f = pcall(conn.call, conn, func, args, async_opts)
            if not ok then
                last_err = lerror.make(f)
            else
                futures[replica_uuid] = f
            end
        end
    end
    local master_uuid
    for replica_uuid, f in pairs(futures) do
        if timeout < 0 then
            -- Netbox uses cond var inside, whose wait throws an error when gets
            -- a negative timeout. Avoid that.
            -- Even if the timeout has expired, still walk though the futures
            -- hoping for a chance that some of them managed to finish and bring
            -- a sign of a master.
            timeout = 0
        end
        res, err = f:wait_result(timeout)
        timeout = deadline - fiber_clock()
        if not res then
            f:discard()
            last_err = err
            goto next_wait
        end
        res = res[1]
        if not res.is_master then
            goto next_wait
        end
        if not master_uuid then
            master_uuid = replica_uuid
            goto next_wait
        end
        is_done = false
        last_err = lerror.vshard(lerror.code.MULTIPLE_MASTERS_FOUND,
                                 replicaset_uuid, master_uuid, replica_uuid)
        do return is_done, is_nop, last_err end
        ::next_wait::
    end
    master = replicaset.replicas[master_uuid]
    if master then
        log.info('Found master for replicaset %s: %s', replicaset_uuid,
                 master_uuid)
        replicaset.master = master
        replicaset.master_cond:broadcast()
    else
        is_done = false
    end
    return is_done, is_nop, last_err
end

local function locate_masters(replicasets)
    local is_all_done = true
    local is_all_nop = true
    local last_err
    for _, replicaset in pairs(replicasets) do
        local is_done, is_nop, err = replicaset_locate_master(replicaset)
        is_all_done = is_all_done and is_done
        is_all_nop = is_all_nop and is_nop
        last_err = err or last_err
        fiber_yield()
    end
    return is_all_done, is_all_nop, last_err
end

--
-- Meta-methods
--
local replicaset_mt = {
    __index = {
        connect = replicaset_connect_master;
        connect_master = replicaset_connect_master;
        connect_all = replicaset_connect_all;
        connect_replica = replicaset_connect_to_replica;
        down_replica_priority = replicaset_down_replica_priority;
        up_replica_priority = replicaset_up_replica_priority;
        wait_connected = replicaset_wait_connected,
        call = replicaset_master_call;
        callrw = replicaset_master_call;
        callro = replicaset_template_multicallro(false, false);
        callbro = replicaset_template_multicallro(false, true);
        callre = replicaset_template_multicallro(true, false);
        callbre = replicaset_template_multicallro(true, true);
        update_master = replicaset_update_master,
    };
    __tostring = replicaset_tostring;
}
--
-- Wrap self methods with a sanity checker.
--
local index = {}
for name, func in pairs(replicaset_mt.__index) do
    index[name] = gsc("replicaset", name, replicaset_mt, func)
end
replicaset_mt.__index = index

local replica_mt = {
    __index = {
        is_connected = function(replica)
            return replica.conn and replica.conn:is_connected()
        end,
        safe_uri = function(replica)
            local uri = luri.parse(replica.uri)
            uri.password = nil
            return luri.format(uri)
        end,
        detach_conn = replica_detach_conn,
    },
    __tostring = function(replica)
        if replica.name then
            return replica.name..'('..replica:safe_uri()..')'
        else
            return replica:safe_uri()
        end
    end,
}
index = {}
for name, func in pairs(replica_mt.__index) do
    index[name] = gsc("replica", name, replica_mt, func)
end
replica_mt.__index = index

--
-- Meta-methods of outdated objects.
-- They define only attributes from corresponding metatables to
-- make user able to access fields of old objects.
--
local function outdated_warning(...)
    return nil, lerror.vshard(lerror.code.OBJECT_IS_OUTDATED)
end

local outdated_replicaset_mt = {
    __index = {
        is_outdated = true
    }
}
for fname, func in pairs(replicaset_mt.__index) do
    outdated_replicaset_mt.__index[fname] = outdated_warning
end

local outdated_replica_mt = {
    __index = {
        is_outdated = true
    }
}
for fname, func in pairs(replica_mt.__index) do
    outdated_replica_mt.__index[fname] = outdated_warning
end

local function outdate_replicasets_f(replicasets)
    for _, replicaset in pairs(replicasets) do
        setmetatable(replicaset, outdated_replicaset_mt)
        for _, replica in pairs(replicaset.replicas) do
            setmetatable(replica, outdated_replica_mt)
            replica.conn = nil
        end
    end
    log.info('Old replicaset and replica objects are outdated.')
end

--
-- Outdate replicaset and replica objects:
--  * Set outdated_metatables.
--  * Remove connections.
-- @param replicasets Old replicasets to be outdated.
-- @param outdate_delay Delay in seconds before the outdating.
--
local function outdate_replicasets(replicasets, outdate_delay)
    if replicasets then
        util.async_task(outdate_delay, outdate_replicasets_f,
                        replicasets)
    end
end

--
-- Calculate for each replicaset its etalon bucket count.
-- Iterative algorithm is used to learn the best balance in a
-- cluster. On each step it calculates perfect bucket count for
-- each replicaset. If this count can not be satisfied due to
-- pinned buckets, the algorithm does best effort to get the
-- perfect balance. This is done via ignoring of replicasets
-- disbalanced via pinning, and their pinned buckets. After that a
-- new balance is calculated. And it can happen, that it can not
-- be satisfied too. It is possible, because ignoring of pinned
-- buckets in overpopulated replicasets leads to decrease of
-- perfect bucket count in other replicasets, and a new values can
-- become less that their pinned bucket count.
--
-- On each step the algorithm either is finished, or ignores at
-- least one new overpopulated replicaset, so it has complexity
-- O(N^2), where N - replicaset count.
--
local function cluster_calculate_etalon_balance(replicasets, bucket_count)
    local is_balance_found = false
    local weight_sum = 0
    local step_count = 0
    local replicaset_count = 0
    for _, replicaset in pairs(replicasets) do
        weight_sum = weight_sum + replicaset.weight
        replicaset_count = replicaset_count + 1
    end
    while not is_balance_found do
        step_count = step_count + 1
        assert(weight_sum > 0)
        local bucket_per_weight = bucket_count / weight_sum
        local buckets_calculated = 0
        for _, replicaset in pairs(replicasets) do
            if not replicaset.ignore_disbalance then
                replicaset.etalon_bucket_count =
                    math.ceil(replicaset.weight * bucket_per_weight)
                buckets_calculated =
                    buckets_calculated + replicaset.etalon_bucket_count
            end
        end
        local buckets_rest = buckets_calculated - bucket_count
        is_balance_found = true
        for _, replicaset in pairs(replicasets) do
            if not replicaset.ignore_disbalance then
                -- A situation is possible, when bucket_per_weight
                -- is not integer. Lets spread this disbalance
                -- over the cluster.
                if buckets_rest > 0 then
                    local n = replicaset.weight * bucket_per_weight
                    local ceil = math.ceil(n)
                    local floor = math.floor(n)
                    if replicaset.etalon_bucket_count > 0 and ceil ~= floor then
                        replicaset.etalon_bucket_count =
                            replicaset.etalon_bucket_count - 1
                        buckets_rest = buckets_rest - 1
                    end
                end
                --
                -- Search for incorrigible disbalance due to
                -- pinned buckets.
                --
                local pinned = replicaset.pinned_count
                if pinned and replicaset.etalon_bucket_count < pinned then
                    -- This replicaset can not send out enough
                    -- buckets to reach a balance. So do the best
                    -- effort balance by sending from the
                    -- replicaset though non-pinned buckets. This
                    -- replicaset and its pinned buckets does not
                    -- participate in the next steps of balance
                    -- calculation.
                    is_balance_found = false
                    bucket_count = bucket_count - replicaset.pinned_count
                    replicaset.etalon_bucket_count = replicaset.pinned_count
                    replicaset.ignore_disbalance = true
                    weight_sum = weight_sum - replicaset.weight
                end
            end
        end
        assert(buckets_rest == 0)
        if step_count > replicaset_count then
            -- This can happed only because of a bug in this
            -- algorithm. But it occupies 100% of transaction
            -- thread, so check step count explicitly.
            return error('PANIC: the rebalancer is broken')
        end
    end
end

--
-- Update/build replicasets from configuration
--
local function buildall(sharding_cfg)
    local new_replicasets = {}
    local weights = sharding_cfg.weights
    local zone = sharding_cfg.zone
    local zone_weights
    if weights and zone and weights[zone] then
        zone_weights = weights[zone]
    else
        zone_weights = {}
    end
    local curr_ts = fiber_clock()
    for replicaset_uuid, replicaset in pairs(sharding_cfg.sharding) do
        local new_replicaset = setmetatable({
            replicas = {},
            uuid = replicaset_uuid,
            weight = replicaset.weight,
            bucket_count = 0,
            lock = replicaset.lock,
            balance_i = 1,
            is_auto_master = replicaset.master == 'auto',
            master_cond = fiber.cond(),
        }, replicaset_mt)
        local priority_list = {}
        for replica_uuid, replica in pairs(replicaset.replicas) do
            -- The old replica is saved in the new object to
            -- rebind its connection at the end of a
            -- router/storage reconfiguration.
            local new_replica = setmetatable({
                uri = replica.uri, name = replica.name, uuid = replica_uuid,
                zone = replica.zone, net_timeout = consts.CALL_TIMEOUT_MIN,
                net_sequential_ok = 0, net_sequential_fail = 0,
                down_ts = curr_ts, backoff_ts = nil, backoff_err = nil,
            }, replica_mt)
            new_replicaset.replicas[replica_uuid] = new_replica
            if replica.master then
                new_replicaset.master = new_replica
            end
            if new_replica.zone then
                if zone_weights[new_replica.zone] then
                    new_replica.weight = zone_weights[new_replica.zone]
                elseif zone and new_replica.zone == zone then
                    new_replica.weight = 0
                else
                    new_replica.weight = math.huge
                end
            else
                new_replica.weight = math.huge
            end
            table.insert(priority_list, new_replica)
        end
        --
        -- Sort replicas of a replicaset by weight. The less is weight,
        -- the more priority has the replica. Sorted replicas are stored
        -- into replicaset.priority_list array.
        --

        -- Return true, if r1 has priority over r2.
        local function replica_cmp_weight(r1, r2)
            -- Master has priority over replicas with the same
            -- weight.
            if r1.weight == r2.weight then
                return r1 == new_replicaset.master
            else
                return r1.weight < r2.weight
            end
        end
        table.sort(priority_list, replica_cmp_weight)
        -- Create a forward list for down_replica_priority().
        for i = 1, #priority_list - 1 do
            priority_list[i].next_by_priority = priority_list[i + 1]
        end
        new_replicaset.priority_list = priority_list
        new_replicasets[replicaset_uuid] = new_replicaset
    end
    return new_replicasets
end

--
-- Wait for masters connection during RECONNECT_TIMEOUT seconds.
--
local function wait_masters_connect(replicasets)
    for _, rs in pairs(replicasets) do
        if rs.master then
            rs.master.conn:wait_connected(consts.RECONNECT_TIMEOUT)
        end
    end
end

return {
    buildall = buildall,
    calculate_etalon_balance = cluster_calculate_etalon_balance,
    wait_masters_connect = wait_masters_connect,
    rebind_replicasets = rebind_replicasets,
    outdate_replicasets = outdate_replicasets,
    locate_masters = locate_masters,
}
