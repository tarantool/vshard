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

--
-- on_connect() trigger for net.box
--
local function netbox_on_connect(conn)
    log.info("connected to %s:%s", conn.host, conn.port)
    local rs = conn.replicaset
    local replica
    if rs.replica and conn == rs.replica.conn then
        replica = rs.replica
    elseif rs.candidate and conn == rs.candidate.conn then
        replica = rs.candidate
    elseif rs.master and conn == rs.master.conn then
        replica = rs.master
    else
        -- There can be some connections, left from the previous
        -- config and still not garbage collected. Ignore them.
        return
    end
    if conn.peer_uuid ~= replica.uuid then
        log.info('Mismatch server UUID: expected "%s", but got "%s"',
                 replica.uuid, conn.peer_uuid)
        conn:close()
        return
    end
    if replica ~= rs.replica and replica ~= rs.candidate then
        return
    end
    -- If a replica's connection has revived, then unset
    -- replica.down_ts - it is not down anymore.
    assert(replica ~= nil)
    replica.down_ts = nil
    if replica == rs.priority_list[1] then
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
    local rs = conn.replicaset
    -- Replica is down - remember this time to decrease replica
    -- priority after FAILOVER_DOWN_TIMEOUT seconds.
    if rs.replica and conn == rs.replica.conn then
        rs.replica.down_ts = fiber.time()
    elseif rs.candidate and conn == rs.candidate.conn then
        rs.candidate.down_ts = fiber.time()
    end
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
    if not conn:is_connected() then
        -- A connection is not established - reset timestamp. It
        -- is nullified in on_connect(), if the connection is
        -- established.
        replica.down_ts = fiber.time()
    else
        -- Unsed down_ts explicitly, because it could be candidate
        -- earlier. And candidate in a case of failure sets
        -- down_ts.
        replica.down_ts = nil
    end
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
-- Helper for replicaset_master/nearest_call().
--
local function replicaset_call_tail(uuid, func, pstatus, status, ...)
    if not pstatus then
        log.error("Exception during calling '%s' on '%s': %s", func, uuid,
                  status)
        return nil, lerror.make(status)
    end
    if status == nil then
        status = nil -- Workaround for `not msgpack.NULL` magic.
    end
    return status, ...
end

--
-- Call a function on remote storage
-- Note: this function uses pcall-style error handling
-- @retval false, err on error
-- @retval true, ... on success
--
local function replicaset_master_call(replicaset, func, args)
    assert(type(func) == 'string', 'function name')
    assert(args == nil or type(args) == 'table', 'function arguments')
    local conn, err = replicaset_connect(replicaset)
    if conn == nil then
        return nil, err
    end
    return replicaset_call_tail(replicaset.master.uuid, func,
                                pcall(conn.call, conn, func, args,
                                      {timeout = consts.CALL_TIMEOUT}))
end

--
-- Call a function on a nearest available replica. It is possible
-- for 'read' requests only. And if the nearest replica is not
-- available now, then use master's connection - we can not wait
-- until failover fiber will repair the nearest connection.
--
local function replicaset_nearest_call(replicaset, func, args)
    assert(type(func) == 'string', 'function name')
    assert(args == nil or type(args) == 'table', 'function arguments')
    local replica = replicaset.replica
    if replica and replica:is_connected() then
        local conn = replica.conn
        return replicaset_call_tail(replica.uuid, func,
                                    pcall(conn.call, conn, func, args,
                                          {timeout = consts.CALL_TIMEOUT}))
    else
        return replicaset_master_call(replicaset, func, args)
    end
end

--
-- Nice formatter for replicaset
--
local function replicaset_tostring(replicaset)
    local uri = ''
    if replicaset.master then
        uri = replicaset.master.uri
    end
    return string.format('Replicaset(uuid=%s, master=%s)',
                         replicaset.uuid, uri)
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
            return replica.conn and replica.conn:is_connected()
        end
    }
}

--
-- Update/build replicasets from configuration
--
local function buildall(sharding_cfg, existing_replicasets)
    local new_replicasets = {}
    local weights = sharding_cfg.weights
    local zone = sharding_cfg.zone
    local zone_weights
    if weights and zone and weights[zone] then
        zone_weights = weights[zone]
    else
        zone_weights = {}
    end
    for replicaset_uuid, replicaset in pairs(sharding_cfg.sharding) do
        local new_replicaset = setmetatable({
            replicas = {},
            uuid = replicaset_uuid,
            weight = replicaset.weight
        }, replicaset_mt)
        local priority_list = {}
        for replica_uuid, replica in pairs(replicaset.replicas) do
            local new_replica = setmetatable({
                uri = replica.uri, name = replica.name, uuid = replica_uuid,
                zone = replica.zone
            }, replica_mt)
            local existing_rs = existing_replicasets[replicaset_uuid]
            if existing_rs ~= nil and existing_rs.replicas[replica_uuid] then
                new_replica.conn = existing_rs.replicas[replica_uuid].conn
            end
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

return {
    buildall = buildall;
}
