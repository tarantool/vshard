-- vshard.replicaset

--
-- <replicaset> = {
--     replicas = {
--         [replica_id] = {
--             uri = URI,
--             name = string,
--             uuid = string,
--             id = <name or uuid>,
--             conn = <netbox> + .replica + .replicaset,
--             zone = number,
--             next_by_priority = <replica object of the same type>,
--             weight = number,
--             down_ts = <timestamp of disconnect from the
--                        replica>,
--             backoff_ts = <timestamp when was sent into backoff state>,
--             activity_ts = <timestamp when the replica was used last time>,
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
--             health_status = <SGREEN  - replica is healthy,
--                              SYELLOW - health of replica is unknown,
--                              SRED    - replica is unhealthy>,
--             replication_status = <SGREEN - replica's lag from master is
--                                      <= cfg.failover_replica_lag_limit,
--                                   SYELLOW - unknown, either master is not
--                                      found or ping failed,
--                                   SRED - replica's lag from master is >
--                                      cfg.failover_replica_lag_limit>
--             fetch_schema = <whether the connection should fetch schema>,
--             worker = { <same as replicaset.worker but for replica> }
--             failover_ping_timeout = <timeout after which a ping is
--                                      considered to be unacknowledged.
--                                      Used by failover service to detect
--                                      if a node is down>,
--             failover_sequential_fail_count = <number of network request
--                                               fails, after which the node
--                                               is considered unhealthy>,
--             failover_interval = <interval in seconds between pings>,
--             limiter = <log ratelimiter, used in the replica_call>,
--          }
--      },
--      master = <master server from the array above>,
--      master_cond = <condition variable signaled when the replicaset finds or
--                     changes its master>,
--      is_master_auto = <true when is configured to find the master on
--                        its own>,
--      master_wait_count = <number of fibers right now waiting for the master to be
--                           discovered>,
--      replica = <nearest available replica object>,
--      balance_i = <index of a next replica in priority_list to
--                   use for a load-balanced request>,
--      replica_up_ts = <timestamp updated on each attempt to
--                       connect to the nearest replica>,
--      name = <replicaset_name>,
--      uuid = <replicaset_uuid>,
--      id = <replicaset_name or replicaset_uuid>,
--      weight = number,
--      priority_list = <list of replicas, sorted by weight asc>,
--      etalon_bucket_count = <bucket count, that must be stored
--                             on this replicaset to reach the
--                             balance in a cluster>,
--      is_outdated = nil/true,
--      worker = {
--          fiber = <fiber to run registered services for replicaset>,
--          services =
--              [service_name] = {
--                  func = <service func pointer>,
--                  data = <used to pass data between service steps>,
--                  deadline = <timestamp, when the next step will be launched>,
--              }
--          },
--      }
--      failover_interval = <interval in seconds between changing the
--                           priority of the replica for callro>,
--  }
--
-- replicasets = {
--    [replicaset_id] = <replicaset>
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
local lcfg = require('vshard.cfg')
local util = require('vshard.util')
local lservice_info = require('vshard.service_info')
local lratelimit = require('vshard.log_ratelimit')
local fiber_clock = fiber.clock
local fiber_yield = fiber.yield
local fiber_cond_wait = util.fiber_cond_wait
local future_wait = util.future_wait
local gsc = util.generate_self_checker
local SGREEN = consts.STATUS.GREEN
local SYELLOW = consts.STATUS.YELLOW
local SRED = consts.STATUS.RED

local replicaset_errinj = {
    ERRINJ_REPLICA_FAILOVER_DELAY = false,
    ERRINJ_MASTER_SEARCH_DELAY = false,
}

local replica_errinj = {
    ERRINJ_REPLICASET_FAILOVER_DELAY = false,
}

--
-- Since state can be set from several places, in order to react on the failures
-- as fast as we can, this tables allows to use unified messages.
--
local health_status_msgs = {
    ['BACKOFF'] = "'%s' is in backoff",
    ['CONN_DOWN'] = "Connection to '%s' was down for %.5f seconds",
    ['SEQ_FAIL'] = "Requests to '%s' failed %d times in a row",
    ['REPLICATION'] = "Replication from master on '%s' is broken"
}

--
-- vconnect is asynchronous vshard greeting, saved inside netbox connection.
-- It stores future object and additional info, needed for its work.
-- Future is initialized, when the connection is established (inside
-- netbox_on_connect). The connection cannot be considered "connected"
-- until vconnect is properly validated.
--
local function conn_vconnect_set(conn)
    assert(conn.replica ~= nil)
    local is_named = conn.replica.id == conn.replica.name
    if not is_named then
        -- Nothing to do. Check is not needed.
        return
    end
    -- Update existing vconnect. Fiber condition cannot be dropped,
    -- somebody may already waiting on it.
    if conn.vconnect then
        -- Connections are preserved during reconfiguration,
        -- identification may be changed.
        conn.vconnect.is_named = is_named
        assert(conn.vconnect.future == nil)
        return
    end
    -- Create new vconnect.
    conn.vconnect = {
        -- Whether the connection is done to the named replica.
        is_named = is_named,
        -- Used to wait for the appearance of vconnect.future object,
        -- if call is done before the connection is established.
        future_cond = fiber.cond(),
    }
end

--
-- Initialize future object. Should be done, when connection is established
-- (inside netbox_on_connect).
--
local function conn_vconnect_start(conn)
    local vconn = conn.vconnect
    if not vconn then
        -- Nothing to do. Check is not needed.
        return
    end

    local opts = {is_async = true}
    vconn.future = conn:call('vshard.storage._call', {'info'}, opts)
    vconn.future_cond:broadcast()
end

--
-- Check, that future is ready, and its result is expected.
-- The function doesn't yield.
-- @retval true; The correct response is received.
-- @retval nil, ...; Response is not received or validation error happened.
--
local function conn_vconnect_check(conn)
    local vconn = conn.vconnect
    local replica = conn.replica
    -- conn.vconnect may be nil, if connection was created on old version
    -- and the storage was reloaded to a new one. It's also nil, when
    -- all checks were already done.
    if not vconn then
        return true
    end
    -- Nothing to do, but wait in such case.
    if not vconn.future or not vconn.future:is_ready() then
        return nil, lerror.vshard(lerror.code.VHANDSHAKE_NOT_COMPLETE,
                                  replica.id)
    end
    -- Critical errors. Connection should be closed after these ones.
    local result, err = vconn.future:result()
    if not result then
        -- Failed to get response. E.g. access error.
        return nil, lerror.make(err)
    end
    -- If name is nil, it means, name was not set yet. If uuid is specified,
    -- then we allow mismatch between config name and nil.
    local is_name_set = result[1].name ~= nil or replica.uuid == nil
    if vconn.is_named and is_name_set and result[1].name ~= replica.name then
        return nil, lerror.vshard(lerror.code.INSTANCE_NAME_MISMATCH,
                                  replica.name, result[1].name)
    end
    -- Don't validate until reconnect happens.
    conn.vconnect = nil
    return true
end

local function conn_vconnect_check_or_close(conn)
    local ok, err = conn_vconnect_check(conn)
    -- Close the connection, if error happened, but it is not
    -- VSHANDSHAKE_NOT_COMPLETE.
    if not ok and err and not (err.type == 'ShardingError' and
       err.code == lerror.code.VHANDSHAKE_NOT_COMPLETE) then
        conn:close()
    end
    return ok, err
end

--
-- Wait until the future object is ready. Returns remaining timeout.
-- @retval timeout; Future boject is ready.
-- @retval nil, err; Timeout passed.
--
local function conn_vconnect_wait(conn, timeout)
    local vconn = conn.vconnect
    -- Fast path. In most cases no validation should be done at all.
    -- conn.vconnect may be nil, if connection was created on old version
    -- and the storage was reloaded to a new one. It's also nil, when
    -- all checks were already done.
    if not vconn or (vconn.future and vconn.future:is_ready()) then
        return timeout
    end
    local deadline = fiber_clock() + timeout
    -- Wait for connection to be established.
    if not vconn.future and
       not fiber_cond_wait(vconn.future_cond, timeout) then
        return nil, lerror.timeout()
    end
    timeout = deadline - fiber_clock()
    -- Wait for async call to return.
    local res, err = future_wait(vconn.future, timeout)
    if res == nil then
        -- Either timeout error or any other. If it's not a timeout error,
        -- then conn must be recteated, handshake must be retried.
        return nil, lerror.make(err)
    end
    return deadline - fiber_clock()
end

local function conn_vconnect_wait_or_close(conn, timeout)
    local ok, err = conn_vconnect_wait(conn, timeout)
    if not ok and not lerror.is_timeout(err) then
        conn:close()
    end
    return ok, err
end

--
-- on_connect() trigger for net.box
--
local function netbox_on_connect(conn)
    log.info("connected to %s:%s", conn.host, conn.port)
    local replica = conn.replica
    assert(replica ~= nil)
    -- If a replica's connection has revived, then unset
    -- replica.down_ts - it is not down anymore.
    replica.down_ts = nil
    -- conn.vconnect is set on every connect, as it may be nil,
    -- if previous check successfully passed. Also, the needed
    -- checks may change on reconnect.
    conn_vconnect_set(conn)
    conn_vconnect_start(conn)
    if replica.uuid and conn.peer_uuid ~= replica.uuid and
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
    -- Future object must be removed from the connection, otherwise
    -- the connection cannot be garbage collected (gh-517).
    -- Moreover, future object must be updated. Old result is irrelevant.
    if conn.vconnect and conn.vconnect.future then
        conn.vconnect.future:discard()
        conn.vconnect.future = nil
    end
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
-- Check if the connection is dead and it won't be restored automatically.
-- Even though replicaset's connections are initialized with the option
-- 'reconnect_after', there's cases, when no reconnect will be done (e.g.
-- if the conn was explicitly cancelled or the connection's worker fiber
-- was killed). In these situations we need to reestablish it manually as
-- the missing connection to some replicaset is a critical problem which
-- leads to the inability of the user to access some part of a data via the
-- router.
--
-- Note: the function expects the conn to be non-nil.
--
local function netbox_is_conn_dead(conn)
    -- Fast path - conn is almost always 'active'.
    if conn.state == 'active' then
        return false
    end
    if conn.state == 'error' or conn.state == 'closed' then
        return true
    end
    if conn.state ~= 'error_reconnect' then
        -- The connection is fetching schema or doing auth, which
        -- means it is not dead and shoudn't be reinitialized.
        return false
    end
    if not conn._fiber then
        -- The fiber field is not present in old netbox, which correctly
        -- reports state as 'error' when its fiber got killed. It would
        -- be filtered out above then.
        return false
    end
    return conn._fiber:status() == "dead"
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
    log.warn('Replica %s returns from backoff', replica.id)
    replica.backoff_ts = nil
    replica.backoff_err = nil
    return true
end

--
-- Check if replica can be considered healthy. It's connection wasn't down
-- for FAILOVER_FOWN_TIMEOUT, the requests didn't fail sequentially for
-- failover_sequential_fail_count (cfg option).
--
local function replica_check_health(replica, now)
    local name = replica.name or replica.uuid
    if not replica_check_backoff(replica, now) then
        return SRED, string.format(health_status_msgs.BACKOFF, name)
    end
    -- down_ts not nil does not mean that the replica is not
    -- connected. Probably it is connected and now fetches schema,
    -- or does authorization. Either case, it is healthy, no need
    -- to down the prio.
    if replica.down_ts and
       now - replica.down_ts >= consts.FAILOVER_DOWN_TIMEOUT then
        return SRED, string.format(health_status_msgs.CONN_DOWN,
                                   name, now - replica.down_ts)
    end
    -- If we failed several sequential requests to replica, then something
    -- is wrong with it. Temporary lower its priority.
    if replica.net_sequential_fail >=
       replica.failover_sequential_fail_count then
        return SRED, string.format(health_status_msgs.SEQ_FAIL,
                                   name, replica.net_sequential_fail)
    end
    return SGREEN
end


--
-- Connect to a specified replica and remember a new connection
-- in the replica object. Note, that the function does not wait
-- until a connection is established.
--
local function replica_connect(replica)
    replica.activity_ts = fiber_clock()
    local conn = replica.conn
    if not conn or netbox_is_conn_dead(conn) then
        conn = netbox.connect(replica.uri, {
            reconnect_after = consts.RECONNECT_TIMEOUT,
            wait_connected = false,
            fetch_schema = replica.fetch_schema
        })
        conn.replica = replica
        -- vconnect must be set before the time, connection is established,
        -- as we must know, that the connection cannot be used. If vconnect
        -- is nil, it means all checks passed, so we may make a call and
        -- only after that 'require' checks.
        conn_vconnect_set(conn)
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
    replicaset.master_wait_count = replicaset.master_wait_count + 1
    if replicaset.worker then
        replicaset.worker:wakeup_service('replicaset_master_search')
    end
    while true do
        master = replicaset.master
        if master then
            break
        end
        if not replicaset.is_master_auto or
           not fiber_cond_wait(replicaset.master_cond, timeout) then
            timeout = lerror.vshard(lerror.code.MISSING_MASTER,
                                    replicaset.id)
            break
        end
        timeout = deadline - fiber_clock()
    end
    assert(replicaset.master_wait_count > 0)
    replicaset.master_wait_count = replicaset.master_wait_count - 1
    return master, timeout
end

--
-- Create net.box connection to master.
--
local function replicaset_connect_master(replicaset)
    local master = replicaset.master
    if master == nil then
        return nil, lerror.vshard(lerror.code.MISSING_MASTER,
                                  replicaset.id)
    end
    return replica_connect(master)
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
    local conn = replica_connect(master)
    return netbox_wait_connected(conn, timeout)
end

--
-- Wait until all instances are connected (with an optional exception).
--
local function replicaset_wait_connected_all(replicaset, opts)
    local timeout = opts.timeout
    local except = opts.except
    local are_all_connected, err
    repeat
        are_all_connected = true
        for replica_id, replica in pairs(replicaset.replicas) do
            if replica_id == except then
                goto next_check
            end
            local conn = replica_connect(replica)
            if not conn:is_connected() then
                timeout, err = netbox_wait_connected(conn, timeout)
                if not timeout then
                    return nil, err, replica_id
                end
                -- While was waiting for this connection, another could break.
                -- Need to re-check all of them.
                are_all_connected = false
            end
        ::next_check::
        end
    until are_all_connected
    return timeout
end

--
-- Create net.box connections to all replicas and master.
--
local function replicaset_connect_all(replicaset)
    for _, replica in pairs(replicaset.replicas) do
        replica_connect(replica)
    end
end

--
-- Connect to a next replica with less priority against a current
-- one. It is needed, if a current replica's connection is down
-- too long.
--
local function replicaset_down_replica_priority(replicaset)
    local old_replica = replicaset.replica
    assert(old_replica and ((old_replica.down_ts and
           not old_replica:is_connected()) or
           old_replica.net_sequential_fail >=
           old_replica.failover_sequential_fail_count or
           old_replica.replication_status == SRED))
    local new_replica = old_replica.next_by_priority
    if new_replica then
        assert(new_replica ~= old_replica)
        replica_connect(new_replica)
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
        local is_healthy = replica.replication_status == SGREEN
        is_healthy = is_healthy and replica.net_sequential_ok > 0
        if replica:is_connected() and (is_healthy or not old_replica) then
            assert(replica.net_sequential_fail == 0)
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
    replica.net_sequential_fail = replica.net_sequential_fail + 1
    if replica.net_sequential_fail >=
       replica.failover_sequential_fail_count then
        local msg = string.format(health_status_msgs.SEQ_FAIL,
                                  replica.name or replica.uuid,
                                  replica.net_sequential_fail)
        replica:update_status('health_status', SRED, msg)
    end
    if replica.net_sequential_fail >= 2 then
        local new_timeout = replica.net_timeout * 2
        if new_timeout <= consts.CALL_TIMEOUT_MAX then
            replica.net_timeout = new_timeout
        end
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
    replica.activity_ts = fiber_clock()
    local conn = replica.conn
    if not opts.is_async then
        -- Async call cannot yield. So, we cannot wait for the connection
        -- to be established and validate vconnect. Immediately fail below,
        -- in conn_vconnect_check_or_close, if something is wrong.
        local timeout, err = conn_vconnect_wait_or_close(conn, opts.timeout)
        if not timeout then
            return false, nil, lerror.make(err)
        end
        opts.timeout = timeout
    end
    local ok, err = conn_vconnect_check_or_close(conn)
    if not ok then
        return false, nil, lerror.make(err)
    end
    assert(conn.vconnect == nil)
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
        replica.limiter:log_error(err,
            "Exception during calling '%s' on '%s': %s", func, replica, err)
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
    local master = replicaset.master
    if not master then
        opts = opts and table.copy(opts) or {}
        if opts.is_async then
            return nil, lerror.vshard(lerror.code.MISSING_MASTER,
                                      replicaset.id)
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
    if not master.conn or not master.conn:is_connected() then
        replica_connect(master)
        -- It could be that the master was disconnected due to a critical
        -- failure and now a new master is assigned. The owner of the connector
        -- must try to find it.
        if replicaset.worker then
            replicaset.worker:wakeup_service('replicaset_master_search')
        end
    end
    -- luacheck: ignore 211/net_status
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
    return lerror.is_timeout(e)
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
        return e.code == lerror.code.STORAGE_IS_DISABLED
    end
    return false
end

--
-- Template to implement a function able to visit multiple
-- replicas with certain details. One of applications - a function
-- making a call on a nearest available replica. It is possible
-- for 'read' requests only. And if the nearest replica is not
-- available now, then use master's connection - we can not wait
-- until failover fiber will repair the nearest connection.
--
local function replicaset_template_multicallro(prefer_replica, balance)
    --
    -- Stateful balancing (balance = true) changes the current
    -- replicaset.balance_i, consequently balancing is preserved between
    -- requests, sequential requests will go to different replicas.
    --
    -- Stateless balancing (balance = false) is local to the current call, it
    -- doesn't change priority list, every new request is firstly made to the
    -- most preferred replica. If request fails with TimeOut error, it's
    -- retried with the next replica by priority in scope of the same call.
    --
    local function replicaset_balance_replica(replicaset, state)
        local i
        local pl = replicaset.priority_list
        local size = #pl
        if balance then
            i = replicaset.balance_i
            replicaset.balance_i = i % size + 1
        else
            i = state.balance_i
            state.balance_i = state.balance_i % size + 1
        end
        assert(i <= size)
        return pl[i]
    end

    --
    -- Pick a next replica according to round-robin load balancing policy.
    --
    local function pick_next_replica(replicaset, state)
        local r
        local master = replicaset.master
        local i = #replicaset.priority_list
        if prefer_replica and state.balance_checked_num >= i then
            -- callre should fallback to master if it tried to access all
            -- replicas and didn't succeed (connections are broken, replicas
            -- are in backoff (e.g. due to insuficient privileges) or
            -- requests failed with timed out error (request_timeout).
            goto fallback_to_master
        end
        while i > 0 do
            i = i - 1
            state.balance_checked_num = state.balance_checked_num + 1
            r = replicaset_balance_replica(replicaset, state)
            if r:is_connected() and (not prefer_replica or r ~= master) and
               r.health_status ~= SRED then
                return r
            end
        end

::fallback_to_master::
        state.balance_checked_num = 0
        return nil
    end

    return function(replicaset, func, args, opts)
        assert(opts == nil or type(opts) == 'table')
        opts = opts and table.copy(opts) or {}
        local timeout = opts.timeout or consts.CALL_TIMEOUT_MAX
        local request_timeout = opts.request_timeout or timeout
        assert(request_timeout <= timeout)
        opts.request_timeout = nil
        local net_status, storage_status, retval, err, replica
        if timeout <= 0 or request_timeout <= 0 then
            return nil, lerror.timeout()
        end
        local state = {
            -- Number of replicas, we tried to access. See pick_next_replica()
            -- for details. Used only when prefer_replica = true.
            balance_checked_num = 0,
            -- Stateless balancer index. See replicaset_balance_replica() for
            -- details. Used only when balance = true.
            balance_i = 1,
        }
        if not balance and replicaset.replica then
            -- Initialize stateless balancer with replicaset.replica's index.
            replica = replicaset.priority_list[state.balance_i]
            while replica ~= replicaset.replica do
                state.balance_i = state.balance_i + 1
                replica = replicaset.priority_list[state.balance_i]
            end
        end
        local now = fiber_clock()
        local end_time = now + timeout
        while timeout > 0 do
            replica = pick_next_replica(replicaset, state)
            if not replica then
                replica, timeout = replicaset_wait_master(replicaset, timeout)
                if not replica then
                    return nil, timeout
                end
                replica_connect(replica)
                if not replica_check_backoff(replica, now) then
                    return nil, lerror.vshard(
                        lerror.code.REPLICASET_IN_BACKOFF, replicaset.id,
                        replica.backoff_err)
                end
            end
            opts.timeout = request_timeout
            net_status, storage_status, retval, err =
                replica_call(replica, func, args, opts)
            if net_status then
                -- Fast path, in most cases everything is all right.
                return storage_status, retval, err
            end
            now = fiber_clock()
            timeout = end_time - now
            if now + request_timeout > end_time then
                -- The `timeout` option sets a strict limit for the entire
                -- operation, which must not be exceeded. To ensure this, we
                -- make as much requests as we can with the speciified
                -- `request_timeout`. However, the last request will use all
                -- the remaining time left within the overall `timeout`,
                -- rather than the `request_timeout` value.
                --
                -- For example, if `request_timeout` is 1 second and `timeout`
                -- is 2.5 seconds, the first two requests to the replicaset
                -- will use a 1-second timeout,  but the last request will
                -- have only 0.5 seconds remaining.
                request_timeout = timeout
            end
            if not net_status and not storage_status and
               not can_retry_after_error(retval) then
                if can_backoff_after_error(retval, func) then
                    if not replica.backoff_ts then
                        log.warn('Replica %s goes into backoff for %s sec '..
                                 'after error %s', replica.id,
                                 consts.REPLICA_BACKOFF_INTERVAL, retval)
                        replica.backoff_ts = now
                        replica.backoff_err = retval
                        local msg = string.format(health_status_msgs.BACKOFF,
                                                  replica.name or replica.uuid)
                        replica:update_status('health_status', SRED, msg)
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
-- Parallel call on all instances in the replicaset. Fails if couldn't be done
-- on at least one instance.
--
-- @return In case of success - a map with replica IDs (UUID or name) keys and
--     values being what the function returned from each replica.
--
-- @return In case of an error - nil, error object, UUID or name of the replica
--     where the error happened.
--
local function replicaset_map_call(replicaset, func, args, opts)
    local timeout = opts.timeout or consts.CALL_TIMEOUT_MIN
    local except = opts.except
    local deadline = fiber_clock() + timeout
    local _, res, err, err_id, map
    local replica_count = 0
    local futures = {}
    local opts_call = {is_async = true, timeout = 0}
    --
    -- Wait all connections. Sending any request if at least one connection is
    -- completely dead would only produce unnecessary workload.
    --
    timeout, err, err_id = replicaset_wait_connected_all(replicaset, {
        timeout = timeout,
        except = except,
    })
    if not timeout then
        goto fail
    end
    --
    -- Send requests.
    --
    for replica_id, replica in pairs(replicaset.replicas) do
        if replica_id == except then
            goto next_call
        end
        _, res, err = replica_call(replica, func, args, opts_call)
        if res == nil then
            err_id = replica_id
            goto fail
        end
        futures[replica_id] = res
        replica_count = replica_count + 1
    ::next_call::
    end
    --
    -- Collect results
    --
    map = table.new(0, replica_count)
    for replica_id, future in pairs(futures) do
        res, err = future_wait(future, timeout)
        if res == nil then
            err_id = replica_id
            goto fail
        end
        map[replica_id] = res
        timeout = deadline - fiber_clock()
    end
    do return map end

::fail::
    for _, f in pairs(futures) do
        f:discard()
    end
    return nil, lerror.make(err), err_id
end

--
-- Nice formatter for replicaset
--
local function replicaset_tostring(is_outdated)
    local outdated_warning = is_outdated and '(outdated) ' or ''
    return function(replicaset)
        local master_str = replicaset.master and replicaset.master or 'missing'
        return string.format('%sreplicaset(id="%s", master=%s)',
                             outdated_warning, replicaset.id, master_str)
    end
end

--
-- Copy netbox connections from old replica objects to new ones
-- and outdate old objects.
-- @param replicasets New replicasets
-- @param old_replicasets Replicasets and replicas to be outdated.
--
local function rebind_replicasets(replicasets, old_replicasets)
    for replicaset_id, replicaset in pairs(replicasets) do
        local old_replicaset = old_replicasets and
                               old_replicasets[replicaset_id]
        for replica_id, replica in pairs(replicaset.replicas) do
            local old_replica = old_replicaset and
                                old_replicaset.replicas[replica_id]
            if old_replica and util.uri_eq(old_replica.uri, replica.uri) then
                replica.down_ts = old_replica.down_ts
                replica.backoff_ts = old_replica.backoff_ts
                replica.backoff_err = old_replica.backoff_err
                replica.net_timeout = old_replica.net_timeout
                replica.net_sequential_ok = old_replica.net_sequential_ok
                replica.net_sequential_fail = old_replica.net_sequential_fail

                if replica.fetch_schema == old_replica.fetch_schema then
                    local conn = old_replica.conn
                    old_replica.conn = nil
                    replica.conn = conn
                    if conn then
                        conn.replica = replica
                    end
                end
            end
        end
        if old_replicaset then
            -- Take a hint from the old replicaset who is the master now.
            if replicaset.is_master_auto then
                local master = old_replicaset.master
                if master then
                    replicaset.master = replicaset.replicas[master.id]
                end
            end
            -- Stop waiting for master in the old replicaset. Its running
            -- requests won't find it anyway. Auto search works only for the
            -- most actual replicaset objects.
            if old_replicaset.is_master_auto then
                old_replicaset.is_master_auto = false
                old_replicaset.master_cond:broadcast()
            end
        end
    end
    if old_replicasets then
        for _, replicaset in pairs(old_replicasets) do
            for _, replica in pairs(replicaset.replicas) do
                -- The connection may still be in use, so detach and not close.
                replica:detach_conn()
            end
        end
    end
end

--
-- Let the replicaset know @a old_master_id is not a master anymore, should
-- use @a candidate_id instead.
-- Returns whether the request, which brought this information, can be retried.
--
local function replicaset_update_master(replicaset, old_master_id, candidate_id)
    local is_auto = replicaset.is_master_auto
    local replicaset_id = replicaset.id
    if old_master_id == candidate_id then
        -- It should not happen ever, but be ready to everything.
        log.warn('Replica %s in replicaset %s reports self as both master '..
                 'and not master', old_master_id, replicaset_id)
        return is_auto
    end
    local master = replicaset.master
    if not master then
        if not is_auto or not candidate_id then
            return is_auto
        end
        local candidate = replicaset.replicas[candidate_id]
        if not candidate then
            return true
        end
        replicaset.master = candidate
        log.info('Replica %s becomes a master as reported by %s for '..
                 'replicaset %s', candidate_id, old_master_id,
                 replicaset_id)
        return true
    end
    local master_id = master.id
    if master_id ~= old_master_id then
        -- Master was changed while the master update information was coming.
        -- It means it is outdated and should be ignored.
        -- Return true regardless of the auto-master config. Because the master
        -- change means the caller's request has a chance to succeed on the new
        -- master on retry.
        return true
    end
    if not is_auto then
        log.warn('Replica %s is not master for replicaset %s anymore, please '..
                 'update configuration', master_id, replicaset_id)
        return false
    end
    if not candidate_id then
        replicaset.master = nil
        log.warn('Replica %s is not a master anymore for replicaset %s, no '..
                 'candidate was reported', master_id, replicaset_id)
        return true
    end
    local candidate = replicaset.replicas[candidate_id]
    if candidate then
        replicaset.master = candidate
        log.info('Replica %s becomes master instead of %s for replicaset %s',
                 candidate_id, master_id, replicaset_id)
    else
        replicaset.master = nil
        log.warn('Replica %s is not a master anymore for replicaset %s, new '..
                 'master %s could not be found in the config',
                 master_id, replicaset_id, candidate_id)
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
    if not replicaset.is_master_auto then
        return is_done, is_nop
    end
    local func = 'vshard.storage._call'
    local args = {'info'}
    local const_timeout = consts.MASTER_SEARCH_TIMEOUT
    local ok, res, err
    local master = replicaset.master
    local old_master_id = master and master.id
    -- Only be optimistic about visiting just the current master when it is
    -- actually alive. Otherwise when it is not connected due to a real failure,
    -- the request would simply hang for a bit and fail. Try to find a new
    -- master among all instances when the current one is not connected.
    if master and master:is_connected() then
        local sync_opts = {timeout = const_timeout}
        ok, res, err = replica_call(master, func, args, sync_opts)
        if ok and res.is_master then
            return is_done, is_nop
        end
        -- Could be changed during the call from the outside. For
        -- instance, by a failed request with a hint from the old
        -- master.
        local cur_master = replicaset.master
        if ok and cur_master == master then
            log.info('Master of replicaset %s, node %s, has resigned. Trying '..
                     'to find a new one', replicaset.id, master.id)
            replicaset.master = nil
        elseif not ok and cur_master == master then
            log.info('Master of replicaset %s, node %s, does not respond: ' ..
                     '%s. Trying to find a new one',
                      replicaset.id, master.id, err)
            -- Try to search for a new master. Master is not nullified, since
            -- we are unsure that there's a new one in the replicaset, but we
            -- must anyway check, because the connection may be shown as
            -- connected even when master is down, e.g. after SIGSTOP.
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
    local async_opts = {is_async = true, timeout = timeout}
    local replicaset_id = replicaset.id
    for replica_id, replica in pairs(replicaset.replicas) do
        if replica_id == old_master_id then
            -- No need to wait for master one more time, we have just tried to
            -- check it and it didn't respond.
            goto next_replica
        end
        replica_connect(replica)
        ok, err = replica:check_is_connected()
        if ok then
            ok, res, err = replica_call(replica, func, args, async_opts)
            if not ok and err ~= nil then
                last_err = err
            else
                futures[replica_id] = res
            end
        elseif err ~= nil then
            last_err = err
        end
        ::next_replica::
    end
    local master_id
    while timeout >= 0 and next(futures) and master_id == nil do
        local ready_futures = {}
        for replica_id, f in pairs(futures) do
            if not f:is_ready() then
                goto next_future
            end
            table.insert(ready_futures, replica_id)
            res, err = f:result()
            if not res then
                last_err = err
                goto next_future
            end
            res = res[1]
            if not res.is_master then
                goto next_future
            end
            master_id = replica_id
            do break end
            ::next_future::
        end
        for _, replica_id in ipairs(ready_futures) do
            futures[replica_id] = nil
        end
        timeout = deadline - fiber_clock()
        fiber.sleep(0.001)
    end
    for _, f in pairs(futures) do
        f:discard()
    end
    master = replicaset.replicas[master_id]
    if master then
        log.info('Found master for replicaset %s: %s', replicaset_id,
                 master_id)
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

local function replica_update_status(replica, status_name, status, reason)
    if replica[status_name] == status then
        return
    end

    replica[status_name] = status
    local name = replica.name or replica.uuid
    if status == SGREEN then
        log.info("%s on '%s' is healthy", status_name, name)
    elseif status == SRED then
        assert(reason ~= nil)
        log.warn("%s on '%s' is unhealthy: %s", status_name, name, reason)
    else
        assert(status == SYELLOW and reason ~= nil)
        log.warn("%s on '%s' is unknown: %s", status_name, name, reason)
    end
end

local function replicaset_service_info(replicaset, service_name)
    local info = {}
    local rs_service_name = 'replicaset_' .. service_name
    local service = replicaset.worker.services[rs_service_name]
    if service ~= nil and service.data.info ~= nil then
        info = service.data.info:info()
    end
    local r_service_name = 'replica_' .. service_name
    for _, r in pairs(replicaset.replicas) do
        service = r.worker.services[r_service_name]
        if service ~= nil and service.data.info ~= nil then
            info.replicas = info.replicas or {}
            info.replicas[r.id] = service.data.info:info()
        end
    end
    return info
end

--
-- Meta-methods
--
local replicaset_mt = {
    __index = {
        connect = replicaset_connect_master;
        connect_master = replicaset_connect_master;
        connect_all = replicaset_connect_all;
        down_replica_priority = replicaset_down_replica_priority;
        up_replica_priority = replicaset_up_replica_priority;
        wait_connected = replicaset_wait_connected,
        wait_connected_all = replicaset_wait_connected_all,
        wait_master = replicaset_wait_master,
        call = replicaset_master_call;
        callrw = replicaset_master_call;
        callro = replicaset_template_multicallro(false, false);
        callbro = replicaset_template_multicallro(false, true);
        callre = replicaset_template_multicallro(true, false);
        callbre = replicaset_template_multicallro(true, true);
        map_call = replicaset_map_call,
        update_master = replicaset_update_master,
        locate_master = replicaset_locate_master,
        service_info = replicaset_service_info,
    };
    __tostring = replicaset_tostring(false);
}
--
-- Wrap self methods with a sanity checker.
--
local index = {}
for name, func in pairs(replicaset_mt.__index) do
    index[name] = gsc("replicaset", name, replicaset_mt, func)
end
replicaset_mt.__index = index

local function replica_safe_uri(replica)
    local uri = luri.parse(replica.uri)
    return util.uri_format(uri, false)
end

--
-- Nice formatter for replica
--
local function replica_tostring(is_outdated)
    local outdated_warning = is_outdated and '(outdated) ' or ''
    return function(replica)
        local replica_str = replica.name and replica.name or replica.uuid
        return string.format('%sreplica(id=%s, uri=%s)', outdated_warning,
                             replica_str, replica_safe_uri(replica))
    end
end

local replica_mt = {
    __index = {
        is_connected = function(replica)
            return replica.conn and replica.conn:is_connected() and
                conn_vconnect_check_or_close(replica.conn)
        end,
        -- Does the same thing as is_connected(), but returns true or nil, err.
        check_is_connected = function(replica)
            if not replica.conn then
                return nil, lerror.from_string("%s.conn is nil")
            end
            if not replica.conn:is_connected() then
                return nil, lerror.from_string('%s.conn is not connected')
            end
            return conn_vconnect_check_or_close(replica.conn)
        end,
        safe_uri = replica_safe_uri,
        connect = replica_connect,
        detach_conn = replica_detach_conn,
        call = replica_call,
        update_status = replica_update_status,
    },
    __tostring = replica_tostring(false),
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
local function outdated_warning()
    return nil, lerror.vshard(lerror.code.OBJECT_IS_OUTDATED)
end

local outdated_replicaset_mt = {
    __index = {
        is_outdated = true
    },
    __tostring = replicaset_tostring(true),
}
for fname, _ in pairs(replicaset_mt.__index) do
    outdated_replicaset_mt.__index[fname] = outdated_warning
end

local outdated_replica_mt = {
    __index = {
        is_outdated = true
    },
    __tostring = replica_tostring(true),
}
for fname, _ in pairs(replica_mt.__index) do
    outdated_replica_mt.__index[fname] = outdated_warning
end

local function worker_cancel(module)
    local worker = module.worker
    if worker ~= nil and worker.fiber ~= nil then
        pcall(worker.fiber.cancel, worker.fiber)
    end
end

local function outdate_replicasets_f(replicasets)
    for _, replicaset in pairs(replicasets) do
        worker_cancel(replicaset)
        setmetatable(replicaset, outdated_replicaset_mt)
        for _, replica in pairs(replicaset.replicas) do
            worker_cancel(replica)
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
    local is_named = sharding_cfg.identification_mode == 'name_as_key'
    for replicaset_id, replicaset in pairs(sharding_cfg.sharding) do
        local replicaset_uuid, replicaset_name = lcfg.extract_identifiers(
            replicaset_id, replicaset, is_named)
        local new_replicaset = setmetatable({
            replicas = {},
            uuid = replicaset_uuid,
            name = replicaset_name,
            id = replicaset_id,
            weight = replicaset.weight,
            bucket_count = 0,
            lock = replicaset.lock,
            balance_i = 1,
            is_master_auto = replicaset.master == 'auto',
            master_cond = fiber.cond(),
            master_wait_count = 0,
            errinj = table.deepcopy(replicaset_errinj),
            failover_interval = sharding_cfg.failover_interval,
        }, replicaset_mt)
        local priority_list = {}
        for replica_id, replica in pairs(replicaset.replicas) do
            local replica_uuid, replica_name = lcfg.extract_identifiers(
                replica_id, replica, is_named)
            local count_name = 'failover_sequential_fail_count'
            local lag_name = 'failover_replica_lag_limit'
            -- The old replica is saved in the new object to
            -- rebind its connection at the end of a
            -- router/storage reconfiguration.
            local new_replica = setmetatable({
                uri = replica.uri, name = replica_name, uuid = replica_uuid,
                zone = replica.zone, net_timeout = consts.CALL_TIMEOUT_MIN,
                net_sequential_ok = 0, net_sequential_fail = 0,
                down_ts = curr_ts, backoff_ts = nil, backoff_err = nil,
                id = replica_id, replication_status = SYELLOW,
                fetch_schema = sharding_cfg.connection_fetch_schema,
                failover_ping_timeout = sharding_cfg.failover_ping_timeout,
                errinj = table.deepcopy(replica_errinj),
                failover_sequential_fail_count = sharding_cfg[count_name],
                failover_interval = sharding_cfg.failover_interval,
                failover_replica_lag_limit = sharding_cfg[lag_name],
                health_status = SYELLOW, limiter = lratelimit.create{name =
                'replica.' .. replica_id},
            }, replica_mt)
            new_replicaset.replicas[replica_id] = new_replica
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
        new_replicasets[replicaset_id] = new_replicaset
    end
    return new_replicasets
end

local function wait_masters_connect(replicasets, timeout)
    local err, err_id
    -- Start connecting to all masters in parallel.
    local deadline = fiber_clock() + timeout
    for _, replicaset in pairs(replicasets) do
        local master
        master, err = replicaset:wait_master(timeout)
        if not master then
            err_id = replicaset.id
            goto fail
        end
        replica_connect(master)
        timeout = deadline - fiber_clock()
    end
    -- Wait until all connections are established.
    for _, replicaset in pairs(replicasets) do
        timeout, err = replicaset:wait_connected(timeout)
        if not timeout then
            err_id = replicaset.id
            goto fail
        end
    end
    do return timeout end
    ::fail::
    return nil, err, err_id
end

--------------------------------------------------------------------------------
-- Replica failover
--------------------------------------------------------------------------------

--
-- Check, whether the instance has properly working connection to the master.
-- Returns the status of the replica's replication and the reason, if it's
-- SRED or SYELLOW.
--
local function failover_check_upstream(upstream, lag_limit)
    if not upstream then
        return SRED, 'Missing upstream from the master'
    end
    local status = upstream.status
    -- 'loading' status is required for 1.10 to work, since applier there
    -- goes into that state, when it's broken, the idle is updated, but the
    -- lag is growing.
    if status and
       (status == 'follow' or status == 'sync' or status == 'loading') then
        if not upstream.lag then
            return SYELLOW, 'Missing upstream lag from the master'
        end
        if upstream.lag > lag_limit then
            local msg = string.format('Upstream to master has lag %.5f > %.5f',
                                       upstream.lag, lag_limit)
            return SRED, msg
        end
        return SGREEN
    end
    if not upstream.idle then
        return SYELLOW, 'Missing upstream idle from the master'
    end
    if upstream.idle > lag_limit then
        local msg = string.format('Upstream to master has idle %.5f > %.5f',
                                  upstream.idle, lag_limit)
        return SRED, msg
    end
    return SGREEN
end

local function replica_failover_ping(replica, opts)
    local status_name = 'replication_status'
    local info_opts = {timeout = opts.timeout, with_health = true}
    local net_status, info, err =
        replica:call('vshard.storage._call', {'info', info_opts}, opts)
    if not info then
        replica:update_status(status_name, SYELLOW, err)
        return net_status, info, err
    elseif not info.health then
        local msg = 'Health state in ping is missing - please upgrade storage'
        replica:update_status(status_name, SYELLOW, msg)
        return net_status, info, msg
    end

    assert(type(info.health) == 'table')
    local replication_status, reason =
        failover_check_upstream(info.health.master_upstream,
                                replica.failover_replica_lag_limit)
    replica:update_status(status_name, replication_status, reason)
    return net_status, info, err
end

local function replica_failover_service_step(replica, data)
    if not data.info then
        local name = 'replica_failover'
        -- Service info is recreated on every reload.
        data.info = lservice_info.new(name)
        data.limiter = lratelimit.create{name = name .. '.' .. replica.id}
    end
    if replica.errinj.ERRINJ_REPLICA_FAILOVER_DELAY then
        replica.errinj.ERRINJ_REPLICA_FAILOVER_DELAY = 'in'
        repeat
            fiber.sleep(0.001)
        until not replica.errinj.ERRINJ_REPLICA_FAILOVER_DELAY
    end
    data.info:next_iter()
    if not replica.conn or replica.down_ts ~= nil then
        -- Nothing to ping. Connection is either dead or missing.
        data.info:set_activity('idling')
        data.info:set_status_error('Connection is down or missing')
        return replica.failover_interval
    end
    local opts = {timeout = replica.failover_ping_timeout}
    data.info:set_activity('pinging')
    local net_status, _, err = replica_failover_ping(replica, opts)
    if not net_status then
        data.limiter:log_error(err, data.info:set_status_error(
                   'Ping error from %s: perhaps a connection is down: %s',
                   replica, err))
        -- Connection hangs. Recreate it to be able to
        -- fail over to a replica next by priority. The
        -- old connection is not closed in case if it just
        -- processes too big response at this moment. Any
        -- way it will be eventually garbage collected
        -- and closed.
        replica:detach_conn()
        replica:connect()
    else
        data.info:set_status_ok()
    end
    data.info:set_activity('idling')
    return replica.failover_interval
end

--------------------------------------------------------------------------------
-- Replicaset failover
--------------------------------------------------------------------------------

--
-- Update the health_status of all replicas in the replicaset.
--
local function replicaset_failover_update_health(replicaset, now)
    -- The replication health check must be applied iff there's node in
    -- replicaset with alive replication or master with alive connection.
    local is_replication_alive = false
    for _, r in pairs(replicaset.replicas) do
        local is_master = r == replicaset.master
        if (is_master and r.replication_status == SGREEN) or
           (not is_master and r.replication_status ~= SRED) then
            is_replication_alive = true
            break
        end
    end
    for _, r in pairs(replicaset.replicas) do
        local status, reason = replica_check_health(r, now)
        if is_replication_alive and status < r.replication_status then
            status = r.replication_status
            reason = string.format(health_status_msgs.REPLICATION,
                                   r.name or r.uuid)
        end
        replica_update_status(r, 'health_status', status, reason)
    end
end

--
-- Replicaset must fall its replica connection to lower priority,
-- if the current one is down too long.
--
local function replicaset_failover_need_down_priority(replicaset)
    local r = replicaset.replica
    if not r or not r.next_by_priority then
        return false
    end
    return r.health_status == SRED
end

--
-- Once per FAILOVER_UP_TIMEOUT a replicaset must try to connect
-- to a replica with a higher priority.
--
local function replicaset_failover_need_up_priority(replicaset, curr_ts)
    local up_ts = replicaset.replica_up_ts
    return not up_ts or curr_ts - up_ts >= consts.FAILOVER_UP_TIMEOUT
end

--
-- Detect not optimal or disconnected replicas. For not optimal
-- try to update them to optimal, and down priority of
-- disconnected replicas.
-- @retval true A replica of an replicaset has been changed.
--
local function replicaset_failover_step(replicaset)
    if not replicaset.replicas or not next(replicaset.replicas) then
        return false
    end
    local curr_ts = fiber_clock()
    replicaset_failover_update_health(replicaset, curr_ts)
    local old_replica = replicaset.replica
    if replicaset_failover_need_up_priority(replicaset, curr_ts) then
        replicaset:up_replica_priority()
    end
    if replicaset_failover_need_down_priority(replicaset) then
        replicaset:down_replica_priority()
    end
    if old_replica ~= replicaset.replica then
        log.info('New replica %s for %s', replicaset.replica, replicaset)
        return true
    end
    return false
end

--
-- Failover service step. Replica connection is the
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
local function replicaset_failover_service_step(replicaset, data)
    if not data.info then
        local name = 'replicaset_failover'
        -- Service info is recreated on every reload.
        data.info = lservice_info.new(name)
        data.limiter = lratelimit.create{name = name .. '.' .. replicaset.id}
        -- This flag is used to avoid logging like:
        -- 'All is ok ... All is ok ... All is ok ...'
        -- each replicaset.failover_interval seconds.
        data.prev_was_ok = false
    end

    if replicaset.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY then
        replicaset.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY = 'in'
        repeat
            fiber.sleep(0.001)
        until not replicaset.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY
    end
    data.info:next_iter()
    data.info:set_activity('updating replicas')
    local ok, replica_is_changed = pcall(replicaset_failover_step, replicaset)
    if not ok then
        data.limiter:log_error(replica_is_changed, data.info:set_status_error(
                   'Error during failovering: %s',
                   lerror.make(replica_is_changed)))
        replica_is_changed = true
    else
        data.info:set_status_ok()
        if not data.prev_was_ok then
            log.info('All replicas are ok')
        end
    end
    data.prev_was_ok = not replica_is_changed
    local logf
    if replica_is_changed then
        logf = log.info
    else
        -- In any case it is necessary to periodically log
        -- failover heartbeat.
        logf = log.verbose
    end
    logf('Failovering step is finished. Schedule next after %f seconds',
         replicaset.failover_interval)
    data.info:set_activity('idling')
    return replicaset.failover_interval
end

--------------------------------------------------------------------------------
-- Replica connection management
--------------------------------------------------------------------------------

local function replica_collect_idle_conns_service_step(replica, data)
    if not data.info then
        -- Service info is recreated on every reload.
        data.info = lservice_info.new('collect_idle_conns')
    end
    data.info:next_iter()
    local c = replica.conn
    local timeout = consts.REPLICA_NOACTIVITY_TIMEOUT
    if c and replica.activity_ts and
       replica.activity_ts + timeout < fiber_clock() then
        replica.conn = nil
        c:close()
        log.info('Closed unused connection to %s', replica)
    end
    data.info:set_status_ok()
    return consts.MASTER_SEARCH_IDLE_INTERVAL
end

--------------------------------------------------------------------------------
-- Master search
--------------------------------------------------------------------------------

local function replicaset_master_search_lazy_step(rs)
    if rs.master and rs.is_master_auto and not rs.master:is_connected() then
        log.warn('Discarded a not connected master %s in rs %s',
            rs.master, rs.id)
        rs.master = nil
    end
    if rs.is_master_auto and not rs.master and rs.master_wait_count > 0 then
        return rs:locate_master()
    end
    return true, true
end

local function replicaset_master_search_aggressive_step(rs)
    return rs:locate_master()
end

local function replicaset_master_search_step(rs, args)
    if args and args.mode == 'lazy' then
        return replicaset_master_search_lazy_step(rs)
    end
    return replicaset_master_search_aggressive_step(rs)
end

--
-- Master discovery background function. It is supposed to notice master changes
-- and find new masters in the replicasets, which are configured for that.
--
-- XXX: due to polling the search might notice master change not right when it
-- happens. In future it makes sense to rewrite master search using
-- subscriptions. The problem is that at the moment of writing the subscriptions
-- are not working well in all Tarantool versions.
--
local function replicaset_master_search_service_step(replicaset, data)
    if not data.info then
        local name = 'master_search'
        data.info = lservice_info.new(name)
        data.limiter = lratelimit.create{name = name .. '.' .. replicaset.id}
        data.is_in_progress = false
    end
    data.info:next_iter()
    data.info:set_activity('locating master')
    if replicaset.errinj.ERRINJ_MASTER_SEARCH_DELAY then
        replicaset.errinj.ERRINJ_MASTER_SEARCH_DELAY = 'in'
        repeat
            fiber.sleep(0.001)
        until not replicaset.errinj.ERRINJ_MASTER_SEARCH_DELAY
    end
    local timeout
    local start_time = fiber_clock()
    local mode = data.args and data.args.mode
    local is_done, is_nop, err =
        replicaset_master_search_step(replicaset, {mode = mode})
    if err then
        data.limiter:log_error(err, data.info:set_status_error(
                   'Error during master search: %s', lerror.make(err)))
    end
    if is_done then
        timeout = consts.MASTER_SEARCH_IDLE_INTERVAL
        data.info:set_status_ok()
        data.info:set_activity('idling')
    elseif err then
        timeout = consts.MASTER_SEARCH_BACKOFF_INTERVAL
        data.info:set_activity('backoff')
    else
        timeout = consts.MASTER_SEARCH_WORK_INTERVAL
    end
    if not data.is_in_progress then
        if not is_nop and is_done then
            log.info('Master search happened')
        elseif not is_done then
            log.info('Master search is started')
            data.is_in_progress = true
        end
    elseif is_done then
        log.info('Master search is finished')
        data.is_in_progress = false
    end
    local end_time = fiber_clock()
    local duration = end_time - start_time
    if not is_nop then
        log.verbose('Master search step took %s seconds. Next in %s '..
                    'seconds', duration, timeout)
    end
    return timeout
end

--------------------------------------------------------------------------------
-- Worker
--------------------------------------------------------------------------------

--
-- Worker fiber, which runs background services.
--
local function worker_f(owner)
    local worker = owner.worker
    assert(worker and worker.services)
    -- The fiber is not reloadable and can be only stopped on outdating of
    -- the owner (either replica or replicaset), which cancel the worker fiber.
    while true do
        fiber.testcancel()
        local min_deadline = fiber.clock() + consts.TIMEOUT_INFINITY
        for _, service in pairs(worker.services) do
            if fiber_clock() >= service.deadline then
                -- The service cannot notice reconfiguration during step.
                -- If reconfiguration happens and the module changes, when
                -- the step is in the middle of yield, then service will end
                -- step on the old module object, but the next step will be
                -- done on the new module. Step sees the module atomically.
                fiber.testcancel()
                local ok, ret = pcall(service.func, owner, service.data)
                fiber.testcancel()
                if not ok then
                    log.error('%s failed: %s', service.func_name, ret)
                    -- Immediately retry after yield. Works as reloadable fiber.
                    service.deadline = 0
                    goto next_service
                end
                assert(type(ret) == 'number')
                service.deadline = fiber_clock() + ret
            end
            ::next_service::
            if service.deadline < min_deadline then
                min_deadline = service.deadline
            end
        end
        -- The fiber sleeps indefinitely, when there're no services registered.
        -- Worker must be woken up on every service add.
        local timeout = min_deadline - fiber_clock()
        timeout = timeout > 0 and timeout or 0
        fiber.testcancel()
        fiber.sleep(timeout)
    end
end

--
-- This table maps service name to the step functions. It's are needed to
-- simplify the API of `worker_add_service` and allow to add a service only
-- by its name, without knowing the function name.
--
local worker_service_to_func = {
    replica_failover = replica_failover_service_step,
    replicaset_failover = replicaset_failover_service_step,
    replica_collect_idle_conns = replica_collect_idle_conns_service_step,
    replicaset_master_search = replicaset_master_search_service_step,
}

local function worker_add_service(worker, service_name, args)
    local func = worker_service_to_func[service_name]
    assert(func ~= nil)
    local service = {
        data = {args = args},
        func = func,
        deadline = 0,
    }
    worker.services[service_name] = service
    assert(worker.fiber ~= nil)
    worker.fiber:wakeup()
end

local function worker_wakeup_service(worker, service_name)
    if worker.services[service_name] then
        worker.services[service_name].deadline = 0
    end
    pcall(worker.fiber.wakeup, worker.fiber)
end

local function worker_remove_service(worker, service_name)
    -- On the next iteration of worker service won't run.
    worker.services[service_name] = nil
end

local worker_mt = {
    __index = {
        add_service = worker_add_service,
        wakeup_service = worker_wakeup_service,
        remove_service = worker_remove_service,
    }
}

local function worker_create(worker_name, owner)
    local worker = setmetatable({services = {}}, worker_mt)
    worker.fiber = fiber.new(worker_f, owner)
    worker.fiber:name(worker_name, {truncate = true})
    return worker
end

--
-- Create and start worker fibers. It's done as a separate step in order to
-- allow connections to be done before starting the services. Note, that
-- workers are recreated on every reconfiguration.
--
local function create_workers(replicasets)
    for _, rs in pairs(replicasets) do
        assert(rs.worker == nil)
        local name = 'vshard.replicaset.' .. (rs.name or rs.uuid)
        rs.worker = worker_create(name, rs)
        for _, replica in pairs(rs.replicas) do
            assert(replica.worker == nil)
            name = 'vshard.replica.' .. (replica.name or replica.uuid)
            replica.worker = worker_create(name, replica)
        end
    end
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------

return {
    buildall = buildall,
    calculate_etalon_balance = cluster_calculate_etalon_balance,
    wait_masters_connect = wait_masters_connect,
    rebind_replicasets = rebind_replicasets,
    replica_safe_uri = replica_safe_uri,
    outdate_replicasets = outdate_replicasets,
    create_workers = create_workers,
    locate_masters = locate_masters,
    -- Functions, exported for testing.
    _worker_service_to_func = worker_service_to_func
}
