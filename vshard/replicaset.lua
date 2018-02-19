-- vshard.replicaset

--
-- <replicaset> = {
--     replicas = {
--         [replica_uuid] = {
--             uri = string,
--             name = string,
--             uuid = string,
--             conn = <netbox>,
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
--          }
--      },
--      master = <master server from the array above>,
--      replica = <nearest available replica object>,
--      candidate = <replica with less weight, which tries to
--                   connect and replace an original replica>,
--      replica_up_ts = <timestamp updated on each attempt to
--                       connect to the nearest replica, and on
--                       each connect event>,
--      uuid = <replicaset_uuid>,
--      weight = number,
--      priority_list = <list of replicas, sorted by weight asc>,
--      ethalon_bucket_count = <bucket count, that must be stored
--                              on this replicaset to reach the
--                              balance in a cluster>,
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
    if (replica == rs.replica or replica == rs.candidate) and
       replica == rs.priority_list[1] then
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
local function replicaset_connect(replicaset)
    local master = replicaset.master
    if master == nil then
        return nil, lerror.vshard(lerror.code.MASTER_IS_MISSING,
                                  {replicaset_uuid = replicaset.uuid})
    end
    return replicaset_connect_to_replica(replicaset, master)
end

--
-- Make a replica be used for read requests or be candidate.
-- @param replicaset Replicaset for which a replica is set.
-- @param replica Replica to be used for read requests.
-- @param read_name Either replica or candidate. Both of them can
--        be updated independently (@sa update_candidate(),
--        down_priority()).
--
local function replicaset_make_replica_read(replicaset, replica, read_name)
    assert(read_name == 'replica' or read_name == 'candidate')
    local old_replica = replicaset[read_name]
    assert(old_replica ~= replica)
    local conn = replicaset_connect_to_replica(replicaset, replica)
    replicaset[read_name] = replica
    if old_replica and old_replica ~= replicaset.master then
        assert(conn ~= old_replica.conn)
        -- Each unused connection holds a worker fiber. Close them
        -- to return fibers in pool now. Do not wait lua gc - it
        -- is slow as fuck.
        old_replica.conn:close()
    end
end

--
-- Try to connect to another candidate. There is two cases:
-- either
-- * it is time to reconnect to the nearest replica - choose
--   first replica in priority list, or
-- * current candidate can not connect to a server during
--   DOWN_TIMEOUT seconds - then the candidate is set to a next by
--   priority.
--
-- New connection is stored into candidate. It replaces an
-- original replica when connected.
--
local function replicaset_update_candidate(replicaset)
    local old_candidate = replicaset.candidate
    local new_candidate
    local curr_ts = fiber.time()
    local up_ts = replicaset.replica_up_ts
    if not old_candidate or not up_ts or
       curr_ts - up_ts >= consts.FAILOVER_UP_TIMEOUT then
        new_candidate = replicaset.priority_list[1]
        -- Update timestamp of the last attempt to connect to the
        -- best replica.
        replicaset.replica_up_ts = curr_ts
        -- It is possible, that the current replica already has
        -- the best priority. In such a case there is no need
        -- to create candidate.
        if new_candidate == replicaset.replica or
           new_candidate == old_candidate then
            return
        end
    else
        assert(old_candidate.next_by_priority and old_candidate.down_ts and
               curr_ts - old_candidate.down_ts >= consts.FAILOVER_DOWN_TIMEOUT
               and old_candidate.next_by_priority ~= replicaset.replica)
        new_candidate = old_candidate.next_by_priority
    end
    replicaset_make_replica_read(replicaset, new_candidate, 'candidate')
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
    local new_replica = replicaset.replica.next_by_priority
    if new_replica then
        replicaset_make_replica_read(replicaset, new_replica, 'replica')
    end
    -- Else the current replica already has the lowest priority.
    -- Can not down it.
end

--
-- Set candidate as the current replica. Candidate attribute is
-- nullified and can be reused, for example, to try to connect to
-- the nearest replica.
--
local function replicaset_set_candidate_as_replica(replicaset)
    assert(replicaset.candidate)
    local old_replica = replicaset.replica
    replicaset.replica = replicaset.candidate
    assert(not old_replica or
           old_replica.weight >= replicaset.replica.weight and
           old_replica ~= replicaset.replica)
    replicaset.candidate = nil
    if old_replica and old_replica.conn and
       old_replica ~= replicaset.master then
        old_replica.conn:close()
    end
end

--
-- Destroy net.box connection to master
--
local function replicaset_disconnect(replicaset)
    local master = replicaset.master
    if master == nil then
       return true
    end
    local conn = replicaset.master.conn
    replicaset.master.conn = nil
    conn:close()
    return true
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
    replicaset_connect(replicaset)
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
            conn = replicaset_connect(replicaset)
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
-- Meta-methods
--
local replicaset_mt = {
    __index = {
        connect = replicaset_connect;
        update_candidate = replicaset_update_candidate;
        down_replica_priority = replicaset_down_replica_priority;
        set_candidate_as_replica = replicaset_set_candidate_as_replica;
        disconnect = replicaset_disconnect;
        call = replicaset_master_call;
        callrw = replicaset_master_call;
        callro = replicaset_nearest_call;
    };
    __tostring = replicaset_tostring;
}

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
        return replica.name..'('..replica:safe_uri()..')'
    end,
}

--
-- Calculate for each replicaset its ethalon bucket count.
--
local function cluster_calculate_ethalon_balance(replicasets, bucket_count)
    local weight_sum = 0
    for _, replicaset in pairs(replicasets) do
        weight_sum = weight_sum + (replicaset.weight or 1)
    end
    assert(weight_sum > 0)
    local bucket_per_weight = bucket_count / weight_sum
    local buckets_calculated = 0
    for _, replicaset in pairs(replicasets) do
        replicaset.ethalon_bucket_count =
            math.ceil((replicaset.weight or 1) * bucket_per_weight)
        buckets_calculated =
            buckets_calculated + replicaset.ethalon_bucket_count
    end
    if buckets_calculated == bucket_count then
        return
    end
    -- A situation is possible, when bucket_per_weight is not
    -- integer. Lets spread this disbalance over cluster to
    -- make for any replicaset pair
    -- |replicaset_1 - replicaset_2| <= 1 - this difference is
    -- admissible.
    local buckets_rest = buckets_calculated - bucket_count
    for _, replicaset in pairs(replicasets) do
        replicaset.ethalon_bucket_count = replicaset.ethalon_bucket_count - 1
        buckets_rest = buckets_rest - 1
        if buckets_rest == 0 then
            return
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
        }, replicaset_mt)
        local priority_list = {}
        for replica_uuid, replica in pairs(replicaset.replicas) do
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
    cluster_calculate_ethalon_balance(new_replicasets,
                                      sharding_cfg.bucket_count or
                                      consts.DEFAULT_BUCKET_COUNT)
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

--
-- Close all connections of all replicas.
--
local function destroy(replicasets)
    for _, rs in pairs(replicasets) do
        if rs.master and rs.master.conn then
            rs.master.conn:close()
        end
        if rs.replica and rs.replica.conn then
            rs.replica.conn:close()
        end
        if rs.candidate and rs.candidate.conn then
            rs.candidate.conn:close()
        end
    end
end

return {
    buildall = buildall,
    calculate_ethalon_balance = cluster_calculate_ethalon_balance,
    destroy = destroy,
    wait_masters_connect = wait_masters_connect,
}
