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
--      replica = <nearest available replica object>,
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
local ffi = require('ffi')
local util = require('vshard.util')
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
    if conn.peer_uuid ~= replica.uuid then
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
        rs.replica_up_ts = fiber.time()
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
    conn.replica.down_ts = fiber.time()
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
        conn:on_connect(netbox_on_connect)
        conn:on_disconnect(netbox_on_disconnect)
        replica.conn = conn
    end
    return conn
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
        replicaset.replica_up_ts = fiber.time()
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
local function replica_call(replica, func, args, timeout)
    assert(timeout)
    local conn = replica.conn
    local net_status, storage_status, retval, error_object =
        pcall(conn.call, conn, func, args, {timeout = timeout})
    if not net_status then
        -- Do not increase replica's network timeout, if the
        -- requested one was less, than network's one. For
        -- example, if replica's timeout was 30s, but an user
        -- specified 1s and it was expired, then there is no
        -- reason to increase network timeout.
        if timeout >= replica.net_timeout then
            replica_on_failed_request(replica)
        end
        log.error("Exception during calling '%s' on '%s': %s", func, replica,
                  storage_status)
        return false, nil, lerror.make(storage_status)
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
-- Call a function on remote storage
-- Note: this function uses pcall-style error handling
-- @retval false, err on error
-- @retval true, ... on success
--
local function replicaset_master_call(replicaset, func, args, opts)
    assert(opts == nil or type(opts) == 'table')
    assert(type(func) == 'string', 'function name')
    assert(args == nil or type(args) == 'table', 'function arguments')
    replicaset_connect_master(replicaset)
    local timeout = opts and opts.timeout or replicaset.master.net_timeout
    local net_status, storage_status, retval, error_object =
        replica_call(replicaset.master, func, args, timeout)
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
-- Call a function on a nearest available replica. It is possible
-- for 'read' requests only. And if the nearest replica is not
-- available now, then use master's connection - we can not wait
-- until failover fiber will repair the nearest connection.
--
local function replicaset_nearest_call(replicaset, func, args, opts)
    assert(opts == nil or type(opts) == 'table')
    assert(type(func) == 'string', 'function name')
    assert(args == nil or type(args) == 'table', 'function arguments')
    local timeout = opts and opts.timeout or consts.CALL_TIMEOUT_MAX
    local net_status, storage_status, retval, error_object
    local end_time = fiber.time() + timeout
    while not net_status and timeout > 0 do
        local replica = replicaset.replica
        local conn
        if replica and replica:is_connected() then
            conn = replica.conn
        else
            conn = replicaset_connect_master(replicaset)
            replica = replicaset.master
        end
        net_status, storage_status, retval, error_object =
            replica_call(replica, func, args, timeout)
        timeout = end_time - fiber.time()
        if not net_status and not storage_status and
           not can_retry_after_error(retval) then
            -- There is no sense to retry LuaJit errors, such as
            -- assetions, not defined variables etc.
            net_status = true
            break
        end
    end
    if not net_status then
        return nil, lerror.make(retval)
    else
        return storage_status, retval, error_object
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
            if old_replica then
                local conn = old_replica.conn
                replica.conn = conn
                replica.down_ts = old_replica.down_ts
                replica.net_timeout = old_replica.net_timeout
                replica.net_sequential_ok = old_replica.net_sequential_ok
                replica.net_sequential_fail = old_replica.net_sequential_fail
                if conn then
                    conn.replica = replica
                    conn.replicaset = replicaset
                end
            end
        end
    end
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
        call = replicaset_master_call;
        callrw = replicaset_master_call;
        callro = replicaset_nearest_call;
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
            return replica.conn and replica.conn.state == 'active'
        end,
        safe_uri = function(replica)
            local uri = luri.parse(replica.uri)
            uri.password = nil
            return luri.format(uri)
        end,
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
    local curr_ts = fiber.time()
    for replicaset_uuid, replicaset in pairs(sharding_cfg.sharding) do
        local new_replicaset = setmetatable({
            replicas = {},
            uuid = replicaset_uuid,
            weight = replicaset.weight,
            bucket_count = 0,
            lock = replicaset.lock,
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
                down_ts = curr_ts,
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
}
