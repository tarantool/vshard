local t = require('luatest')
local test_group = t.group('dispenser-unit')
local route_dispenser = require('vshard.storage.route_dispenser')
local vconsts = require('vshard.consts')
local fiber = require('fiber')

local function create_dispenser_with_buckets(routes, count)
    local d = route_dispenser.new(routes)
    d:prepare_begin()
    local buckets = {}
    for i = 1, count do
        table.insert(buckets, i)
    end
    d:prepare_commit(buckets, vconsts.TIMEOUT_INFINITY)
    return d
end

--
-- gh-161: parallel rebalancer. One of the most important part of
-- the latter is a dispenser. It is a structure which hands out
-- destination UUIDs in a round-robin manner to worker fibers.
--
test_group.test_dispenser_basic = function()
    local d = create_dispenser_with_buckets({uuid = 15}, 15)
    for _ = 1, 15 do
        t.assert_equals(d:pop(), 'uuid')
    end
    for _ = 1, 3 do
        t.assert_equals(d.rlist.count, 0)
        t.assert_equals(d.remaining_count, 0)
        t.assert_equals(d.map.uuid.bucket_count, 0)
        t.assert_equals(d.map.uuid.progress, 0)
        t.assert_equals(d.map.uuid.need_to_send, 15)
        t.assert_equals(d:pop(), nil)
    end

    -- Test throttle.
    d = create_dispenser_with_buckets({uuid1 = 5, uuid2 = 5}, 10)
    local uuid, bid = d:pop()
    t.assert_equals(uuid, 'uuid2')
    t.assert_equals(bid, 10)
    t.assert_equals(d.remaining_count, 9)
    t.assert_equals(d.rlist.last.bucket_count, 4)
    t.assert_equals(d.rlist.first.bucket_count, 5)
    d:put(uuid)
    t.assert_equals(d.remaining_count, 10)
    t.assert_equals(d.rlist.last.bucket_count, 5)
    t.assert(d:throttle(uuid))
    t.assert_not(d:throttle(uuid))
    for _ = 1, 4 do
        t.assert_equals(d:pop(), 'uuid1')
        t.assert_equals(d:pop(), 'uuid2')
    end
    t.assert_equals(d:pop(), 'uuid1')
    -- Not enough bucket anymore, prepare more.
    d:prepare_begin()
    d:prepare_commit({100}, vconsts.TIMEOUT_INFINITY)
    t.assert_equals(d:pop(), 'uuid2')
    t.assert_not(d:pop())
end

test_group.test_skip = function()
    -- Double skip should be ok. It happens, if there were several
    -- workers on one destination, and all of them received an error.
    local d = create_dispenser_with_buckets({uuid1 = 1}, 1)
    d:skip('uuid1')
    d:skip('uuid1')
    t.assert_equals(d.remaining_count, 0)
    -- Basic test of the skip flow.
    d = create_dispenser_with_buckets({uuid1 = 5, uuid2 = 5}, 10)
    t.assert_equals(d:pop(), 'uuid2')
    t.assert_equals(d:pop(), 'uuid1')
    d:put('uuid1')
    t.assert_equals(d.remaining_count, 9)
    d:skip('uuid1')
    t.assert_equals(d.remaining_count, 4)
    -- Put after skip changes nothing.
    d:put('uuid1')
    t.assert_equals(d.remaining_count, 4)
    d:put('uuid2')
    d:skip('uuid2')
    t.assert_equals(d.remaining_count, 0)
    t.assert_not(d:pop())
    -- Buckets are not returned and must be cleaned on exit.
    t.assert(#d.prepared_buckets, 8)
end

test_group.test_prepare_begin = function()
    local d = route_dispenser.new({uuid = 10})
    -- Successful begin changes the `is_prepare_in_progress`.
    d:prepare_begin()
    t.assert(d.is_prepare_in_progress)
    -- Second begin in a row - error.
    t.assert_not(pcall(d.prepare_begin, d))
    -- Begin with buckets available - error.
    d:prepare_commit({1}, vconsts.TIMEOUT_INFINITY)
    t.assert_gt(#d.prepared_buckets, 0)
    t.assert_not(pcall(d.prepare_begin, d))
end

test_group.test_prepare_commit = function()
    local d = route_dispenser.new({uuid = 10})
    -- Commit without begin - error.
    t.assert_not(pcall(d.prepare_commit, d))
    -- Commit with buckets available - error.
    d.prepared_buckets = {1}
    t.assert_not(pcall(d.prepare_commit, d, {2}))
    d.prepared_buckets = {}
    -- Successful commit without buckets.
    d:prepare_begin()
    t.assert(d.is_prepare_in_progress)
    d:prepare_commit()
    t.assert_not(d.is_prepare_in_progress)
    t.assert_equals(d.prepared_buckets, {})
    -- Successful commit with buckets.
    d:prepare_begin()
    t.assert(d.is_prepare_in_progress)
    local bids = {1, 2}
    d:prepare_commit(bids)
    t.assert_not(d.is_prepare_in_progress)
    t.assert_equals(d.prepared_buckets, bids)
end

test_group.test_wait_prepared = function()
    local d = route_dispenser.new({uuid = 10})
    -- wait_prepared with buckets - true (buckets are ready).
    d:prepare_begin()
    d:prepare_commit({1}, vconsts.TIMEOUT_INFINITY)
    t.assert(d:wait_prepared())
    d:pop()
    -- No preparation - false (caller must prepare).
    t.assert_not(d:wait_prepared())
    -- Waken up with commit.
    d:prepare_begin()
    local f = fiber.create(function()
        d:wait_prepared(vconsts.TIMEOUT_INFINITY)
    end)
    f:set_joinable(true)
    d:prepare_commit()
    t.assert(f:join())
    -- Error after skip (exit).
    d:skip('uuid')
    local ok, err = d:wait_prepared(vconsts.TIMEOUT_INFINITY)
    t.assert_not(ok)
    t.assert_equals(err.message, 'Nothing to wait anymore')
    -- Error from preparation (exit).
    local error = 'err'
    d.prepare_error = error
    ok, err = d:wait_prepared(vconsts.TIMEOUT_INFINITY)
    t.assert_not(ok)
    t.assert_equals(err, error)
end
