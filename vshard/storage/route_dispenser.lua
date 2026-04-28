--
-- Dispenser is a container of routes received from the rebalancer. Its task is
-- to hand out the routes to worker fibers in a round-robin manner so as any
-- two sequential results are different. It allows to spread dispensing evenly
-- over the receiver nodes.
--
local fiber = require('fiber')
local rlist = require('vshard.rlist')
local lerror = require('vshard.error')
local util = require('vshard.util')

--
-- Put one bucket back to the dispenser. It happens on any error.
--
local function route_dispenser_put(dispenser, id)
    local dst = dispenser.map[id]
    if dst then
        dispenser.remaining_count = dispenser.remaining_count + 1
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
local function route_dispenser_skip(dispenser, id)
    local map = dispenser.map
    local dst = map[id]
    if dst then
        map[id] = nil
        dispenser.rlist:remove(dst)
        dispenser.remaining_count = dispenser.remaining_count - dst.bucket_count
        assert(dispenser.remaining_count >= 0)
    end
end

--
-- Set that the receiver @a id was throttled. When it happens
-- first time it is logged.
--
local function route_dispenser_throttle(dispenser, id)
    local dst = dispenser.map[id]
    if dst then
        local old_value = dst.is_throttle_warned
        dst.is_throttle_warned = true
        return not old_value
    end
    return false
end

--
-- Notify the dispenser that a bucket was successfully sent to
-- @a id. It has no any functional purpose except tracking
-- progress.
--
local function route_dispenser_sent(dispenser, id)
    local dst = dispenser.map[id]
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
    local buckets = dispenser.prepared_buckets
    local rlist = dispenser.rlist
    local dst = rlist.first
    if dst and #buckets > 0 then
        dispenser.remaining_count = dispenser.remaining_count - 1
        assert(dispenser.remaining_count >= 0)
        local bucket_count = dst.bucket_count - 1
        dst.bucket_count = bucket_count
        rlist:remove(dst)
        if bucket_count > 0 then
            rlist:add_tail(dst)
        end
        assert(dispenser.send_buckets_deadline)
        return dst.id, table.remove(buckets), dispenser.send_buckets_deadline
    end
    return nil
end

local function route_dispenser_wait_prepared(dispenser, timeout)
    if #dispenser.prepared_buckets > 0 then
        -- Fast path. In most cases buckets are already prepared.
        return true
    end
    if dispenser.prepare_error then
        return nil, dispenser.prepare_error
    end
    if not dispenser.rlist.first or dispenser.remaining_count == 0 then
        return nil, lerror.make('Nothing to wait anymore')
    end
    if not dispenser.is_prepare_in_progress then
        -- Nobody is preparing the buckets, we must do it.
        return false
    end
    return util.fiber_cond_wait(dispenser.prepare_cond, timeout)
end

local function route_dispenser_prepare_begin(dispenser)
    assert(not dispenser.is_prepare_in_progress)
    assert(#dispenser.prepared_buckets == 0)
    dispenser.is_prepare_in_progress = true
end

local function route_dispenser_prepare_commit(dispenser, buckets, deadline)
    assert(dispenser.is_prepare_in_progress)
    assert(#dispenser.prepared_buckets == 0)
    dispenser.is_prepare_in_progress = false
    dispenser.prepare_cond:broadcast()
    if buckets then
        assert(type(buckets) == 'table')
        dispenser.prepared_buckets = buckets
        dispenser.send_buckets_deadline = deadline
    end
end

local route_dispenser_mt = {
    __index = {
        put = route_dispenser_put,
        skip = route_dispenser_skip,
        throttle = route_dispenser_throttle,
        sent = route_dispenser_sent,
        pop = route_dispenser_pop,
        wait_prepared = route_dispenser_wait_prepared,
        prepare_begin = route_dispenser_prepare_begin,
        prepare_commit = route_dispenser_prepare_commit,
    }
}

local function route_dispenser_new(routes)
    local rlist = rlist.new()
    local map = {}
    local total = 0
    for id, bucket_count in pairs(routes) do
        total = total + bucket_count
        local new = {
            -- Receiver's ID.
            id = id,
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
        map[id] = new
        rlist:add_tail(new)
    end
    return setmetatable({
        rlist = rlist,
        map = map,
        -- Remaining bucket count, which has to be sent.
        remaining_count = total,
        -- The table of buckets, which are ready to be sent.
        prepared_buckets = {},
        -- Deadline until the prepared_buckets must be sent or released.
        send_buckets_deadline = nil,
        -- The buckets are prepared by a single worker, which locks this var.
        is_prepare_in_progress = false,
        -- Condition to wait for buckets to be prepared.
        prepare_cond = fiber.cond(),
    }, route_dispenser_mt)
end

return {
    new = route_dispenser_new,
}
