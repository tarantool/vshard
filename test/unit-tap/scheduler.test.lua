#!/usr/bin/env tarantool

local fiber = require('fiber')
local tap = require('tap')
local lregistry = require('vshard.registry')
local lref = require('vshard.storage.ref')
local lsched = require('vshard.storage.sched')

local big_timeout = 1000000
local small_timeout = 0.000001

--
-- gh-147: scheduler helps to share time fairly between incompatible but
-- necessary operations - storage refs and bucket moves. Refs are used for the
-- consistent map-reduce feature when the whole cluster can be scanned without
-- being afraid that some data may slip through requests on behalf of the
-- rebalancer.
--

--
-- Storage registry is used by the ref module. The ref module is used in the
-- tests in order to ensure the scheduler performs ref garbage collection.
--
local function bucket_are_all_rw()
    return true
end

lregistry.storage = {
    bucket_are_all_rw = bucket_are_all_rw,
}

local function fiber_csw()
    return fiber.info()[fiber.self():id()].csw
end

local function fiber_set_joinable()
    fiber.self():set_joinable(true)
end

local function test_basic(test)
    test:plan(32)

    local ref_strike = lsched.ref_strike
    --
    -- Simplest possible test - start and end a ref.
    --
    test:is(lsched.ref_start(big_timeout), big_timeout, 'start ref')
    test:is(lsched.ref_count, 1, '1 ref')
    test:is(lsched.ref_strike, ref_strike + 1, '+1 ref in a row')
    lsched.ref_end(1)
    test:is(lsched.ref_count, 0, '0 refs after end')
    test:is(lsched.ref_strike, ref_strike + 1, 'strike is kept')

    lsched.ref_start(big_timeout)
    lsched.ref_end(1)
    test:is(lsched.ref_strike, ref_strike + 2, 'strike grows')
    test:is(lsched.ref_count, 0, 'count does not')

    --
    -- Move ends ref strike.
    --
    test:is(lsched.move_start(big_timeout), big_timeout, 'start move')
    test:is(lsched.move_count, 1, '1 move')
    test:is(lsched.move_strike, 1, '+1 move strike')
    test:is(lsched.ref_strike, 0, 'ref strike is interrupted')

    --
    -- Ref times out if there is a move in progress.
    --
    local ok, err = lsched.ref_start(small_timeout)
    test:ok(not ok and err, 'ref fails')
    test:is(lsched.move_count, 1, 'still 1 move')
    test:is(lsched.move_strike, 1, 'still 1 move strike')
    test:is(lsched.ref_count, 0, 'could not add ref')
    test:is(lsched.ref_queue, 0, 'empty ref queue')

    --
    -- Ref succeeds when move ends.
    --
    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.ref_start(big_timeout)
    end)
    fiber.sleep(small_timeout)
    lsched.move_end(1)
    local new_timeout
    ok, new_timeout = f:join()
    test:ok(ok and new_timeout < big_timeout, 'correct timeout')
    test:is(lsched.move_count, 0, 'no moves')
    test:is(lsched.move_strike, 0, 'move strike ends')
    test:is(lsched.ref_count, 1, '+1 ref')
    test:is(lsched.ref_strike, 1, '+1 ref strike')

    --
    -- Move succeeds when ref ends.
    --
    f = fiber.create(function()
        fiber_set_joinable()
        return lsched.move_start(big_timeout)
    end)
    fiber.sleep(small_timeout)
    lsched.ref_end(1)
    ok, new_timeout = f:join()
    test:ok(ok and new_timeout < big_timeout, 'correct timeout')
    test:is(lsched.ref_count, 0, 'no refs')
    test:is(lsched.ref_strike, 0, 'ref strike ends')
    test:is(lsched.move_count, 1, '+1 move')
    test:is(lsched.move_strike, 1, '+1 move strike')
    lsched.move_end(1)

    --
    -- Move times out when there is a ref.
    --
    test:is(lsched.ref_start(big_timeout), big_timeout, '+ ref')
    ok, err = lsched.move_start(small_timeout)
    test:ok(not ok and err, 'move fails')
    test:is(lsched.ref_count, 1, 'still 1 ref')
    test:is(lsched.ref_strike, 1, 'still 1 ref strike')
    test:is(lsched.move_count, 0, 'could not add move')
    test:is(lsched.move_queue, 0, 'empty move queue')
    lsched.ref_end(1)
end

local function test_negative_timeout(test)
    test:plan(12)

    --
    -- Move works even with negative timeout if no refs.
    --
    test:is(lsched.move_start(-1), -1, 'timeout does not matter if no refs')
    test:is(lsched.move_count, 1, '+1 move')

    --
    -- Ref fails immediately if timeout negative and has moves.
    --
    local csw = fiber_csw()
    local ok, err = lsched.ref_start(-1)
    test:ok(not ok and err, 'ref fails')
    test:is(csw, fiber_csw(), 'no yields')
    test:is(lsched.ref_count, 0, 'no refs')
    test:is(lsched.ref_queue, 0, 'no ref queue')

    --
    -- Ref works even with negative timeout if no moves.
    --
    lsched.move_end(1)
    test:is(lsched.ref_start(-1), -1, 'timeout does not matter if no moves')
    test:is(lsched.ref_count, 1, '+1 ref')

    --
    -- Move fails immediately if timeout is negative and has refs.
    --
    csw = fiber_csw()
    ok, err = lsched.move_start(-1)
    test:ok(not ok and err, 'move fails')
    test:is(csw, fiber_csw(), 'no yields')
    test:is(lsched.move_count, 0, 'no moves')
    test:is(lsched.move_queue, 0, 'no move queue')
    lsched.ref_end(1)
end

local function test_move_gc_ref(test)
    test:plan(10)

    --
    -- Move deletes expired refs if it may help to start the move.
    --
    for sid = 1, 10 do
        for rid = 1, 5 do
            lref.add(rid, sid, small_timeout)
        end
    end
    test:is(lsched.ref_count, 50, 'refs are in progress')
    local ok, err = lsched.move_start(-1)
    test:ok(not ok and err, 'move without timeout failed')

    fiber.sleep(small_timeout)
    test:is(lsched.move_start(-1), -1, 'succeeds even with negative timeout')
    test:is(lsched.ref_count, 0, 'all refs are expired and deleted')
    test:is(lref.count, 0, 'ref module knows about it')
    test:is(lsched.move_count, 1, 'move is started')
    lsched.move_end(1)

    --
    -- May need more than 1 GC step.
    --
    for rid = 1, 5 do
        lref.add(0, rid, small_timeout)
    end
    for rid = 1, 5 do
        lref.add(1, rid, small_timeout * 100)
    end
    local new_timeout = lsched.move_start(big_timeout)
    test:ok(new_timeout < big_timeout, 'succeeds by doing 2 gc steps')
    test:is(lsched.ref_count, 0, 'all refs are expired and deleted')
    test:is(lref.count, 0, 'ref module knows about it')
    test:is(lsched.move_count, 1, 'move is started')
    lsched.move_end(1)
end

local function test_ref_strike(test)
    test:plan(10)

    local quota = lsched.ref_quota
    --
    -- Strike should stop new refs if they exceed the quota and there is a
    -- pending move.
    --
    -- End ref strike if there was one.
    lsched.move_start(small_timeout)
    lsched.move_end(1)
    -- Ref strike starts.
    assert(lsched.ref_start(small_timeout))

    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.move_start(big_timeout)
    end)
    test:is(lsched.move_queue, 1, 'move is queued')
    --
    -- New refs should work only until quota is reached, because there is a
    -- pending move.
    --
    for i = 1, quota - 1 do
        assert(lsched.ref_start(small_timeout))
    end
    local ok, err = lsched.ref_start(small_timeout)
    test:ok(not ok and err, 'too long strike with move queue not empty')
    test:is(lsched.ref_strike, quota, 'max strike is reached')
    -- Even if number of current refs decreases, new still are not accepted.
    -- Because there was too many in a row while a new move was waiting.
    lsched.ref_end(1)
    ok, err = lsched.ref_start(small_timeout)
    test:ok(not ok and err, 'still too long strike after one unref')
    test:is(lsched.ref_strike, quota, 'strike is unchanged')

    lsched.ref_end(quota - 1)
    local new_timeout
    ok, new_timeout = f:join()
    test:ok(ok and new_timeout < big_timeout, 'move succeeded')
    test:is(lsched.move_count, 1, '+1 move')
    test:is(lsched.move_strike, 1, '+1 move strike')
    test:is(lsched.ref_count, 0, 'no refs')
    test:is(lsched.ref_strike, 0, 'no ref strike')
    lsched.move_end(1)
end

local function test_move_strike(test)
    test:plan(10)

    local quota = lsched.move_quota
    --
    -- Strike should stop new moves if they exceed the quota and there is a
    -- pending ref.
    --
    -- End move strike if there was one.
    lsched.ref_start(small_timeout)
    lsched.ref_end(1)
    -- Move strike starts.
    assert(lsched.move_start(small_timeout))

    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.ref_start(big_timeout)
    end)
    test:is(lsched.ref_queue, 1, 'ref is queued')
    --
    -- New moves should work only until quota is reached, because there is a
    -- pending ref.
    --
    for i = 1, quota - 1 do
        assert(lsched.move_start(small_timeout))
    end
    local ok, err = lsched.move_start(small_timeout)
    test:ok(not ok and err, 'too long strike with ref queue not empty')
    test:is(lsched.move_strike, quota, 'max strike is reached')
    -- Even if number of current moves decreases, new still are not accepted.
    -- Because there was too many in a row while a new ref was waiting.
    lsched.move_end(1)
    ok, err = lsched.move_start(small_timeout)
    test:ok(not ok and err, 'still too long strike after one move end')
    test:is(lsched.move_strike, quota, 'strike is unchanged')

    lsched.move_end(quota - 1)
    local new_timeout
    ok, new_timeout = f:join()
    test:ok(ok and new_timeout < big_timeout, 'ref succeeded')
    test:is(lsched.ref_count, 1, '+1 ref')
    test:is(lsched.ref_strike, 1, '+1 ref strike')
    test:is(lsched.move_count, 0, 'no moves')
    test:is(lsched.move_strike, 0, 'no move strike')
    lsched.ref_end(1)
end

local function test_ref_increase_quota(test)
    test:plan(4)

    local quota = lsched.ref_quota
    --
    -- Ref quota increase allows to do more refs even if there are pending
    -- moves.
    --
    -- End ref strike if there was one.
    lsched.move_start(big_timeout)
    lsched.move_end(1)
    -- Fill the quota.
    for _ = 1, quota do
        assert(lsched.ref_start(big_timeout))
    end
    -- Start move to block new refs by quota.
    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.move_start(big_timeout)
    end)
    test:ok(not lsched.ref_start(small_timeout), 'can not add ref - full quota')

    lsched.cfg({sched_ref_quota = quota + 1})
    test:ok(lsched.ref_start(small_timeout), 'now can add - quota is extended')

    -- Decrease quota - should not accept new refs again.
    lsched.cfg{sched_ref_quota = quota}
    test:ok(not lsched.ref_start(small_timeout), 'full quota again')

    lsched.ref_end(quota + 1)
    local ok, new_timeout = f:join()
    test:ok(ok and new_timeout < big_timeout, 'move started')
    lsched.move_end(1)
end

local function test_move_increase_quota(test)
    test:plan(4)

    local quota = lsched.move_quota
    --
    -- Move quota increase allows to do more moves even if there are pending
    -- refs.
    --
    -- End move strike if there was one.
    lsched.ref_start(big_timeout)
    lsched.ref_end(1)
    -- Fill the quota.
    for _ = 1, quota do
        assert(lsched.move_start(big_timeout))
    end
    -- Start ref to block new moves by quota.
    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.ref_start(big_timeout)
    end)
    test:ok(not lsched.move_start(small_timeout), 'can not add move - full quota')

    lsched.cfg({sched_move_quota = quota + 1})
    test:ok(lsched.move_start(small_timeout), 'now can add - quota is extended')

    -- Decrease quota - should not accept new moves again.
    lsched.cfg{sched_move_quota = quota}
    test:ok(not lsched.move_start(small_timeout), 'full quota again')

    lsched.move_end(quota + 1)
    local ok, new_timeout = f:join()
    test:ok(ok and new_timeout < big_timeout, 'ref started')
    lsched.ref_end(1)
end

local function test_ref_decrease_quota(test)
    test:plan(4)

    local old_quota = lsched.ref_quota
    --
    -- Quota decrease should not affect any existing operations or break
    -- anything.
    --
    lsched.cfg({sched_ref_quota = 10})
    for _ = 1, 5 do
        assert(lsched.ref_start(big_timeout))
    end
    test:is(lsched.ref_count, 5, 'started refs below quota')

    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.move_start(big_timeout)
    end)
    test:ok(lsched.ref_start(big_timeout), 'another ref after move queued')

    lsched.cfg({sched_ref_quota = 2})
    test:ok(not lsched.ref_start(small_timeout), 'quota decreased - can not '..
            'start ref')

    lsched.ref_end(6)
    local ok, new_timeout = f:join()
    test:ok(ok and new_timeout, 'move is started')
    lsched.move_end(1)

    lsched.cfg({sched_ref_quota = old_quota})
end

local function test_move_decrease_quota(test)
    test:plan(4)

    local old_quota = lsched.move_quota
    --
    -- Quota decrease should not affect any existing operations or break
    -- anything.
    --
    lsched.cfg({sched_move_quota = 10})
    for _ = 1, 5 do
        assert(lsched.move_start(big_timeout))
    end
    test:is(lsched.move_count, 5, 'started moves below quota')

    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.ref_start(big_timeout)
    end)
    test:ok(lsched.move_start(big_timeout), 'another move after ref queued')

    lsched.cfg({sched_move_quota = 2})
    test:ok(not lsched.move_start(small_timeout), 'quota decreased - can not '..
            'start move')

    lsched.move_end(6)
    local ok, new_timeout = f:join()
    test:ok(ok and new_timeout, 'ref is started')
    lsched.ref_end(1)

    lsched.cfg({sched_move_quota = old_quota})
end

local function test_ref_zero_quota(test)
    test:plan(6)

    local old_quota = lsched.ref_quota
    --
    -- Zero quota is a valid value. Moreover, it is special. It means the
    -- 0-quoted operation should always be paused in favor of the other
    -- operation.
    --
    lsched.cfg({sched_ref_quota = 0})
    test:ok(lsched.ref_start(big_timeout), 'started ref with 0 quota')

    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.move_start(big_timeout)
    end)
    test:ok(not lsched.ref_start(small_timeout), 'can not add more refs if '..
            'move is queued - quota 0')

    lsched.ref_end(1)
    local ok, new_timeout = f:join()
    test:ok(ok and new_timeout, 'move is started')

    -- Ensure ref never starts if there are always moves, when quota is 0.
    f = fiber.create(function()
        fiber_set_joinable()
        return lsched.ref_start(big_timeout)
    end)
    local move_count = lsched.move_quota + 3
    -- Start from 2 to account the already existing move.
    for _ = 2, move_count do
        -- Start one new move.
        assert(lsched.move_start(big_timeout))
        -- Start second new move.
        assert(lsched.move_start(big_timeout))
        -- End first move.
        lsched.move_end(1)
        -- In result the moves are always interleaving - no time for refs at
        -- all.
    end
    test:is(lsched.move_count, move_count, 'moves exceed quota')
    test:ok(lsched.move_strike > move_count, 'strike is not interrupted')

    lsched.move_end(move_count)
    ok, new_timeout = f:join()
    test:ok(ok and new_timeout, 'ref finally started')
    lsched.ref_end(1)

    lsched.cfg({sched_ref_quota = old_quota})
end

local function test_move_zero_quota(test)
    test:plan(6)

    local old_quota = lsched.move_quota
    --
    -- Zero quota is a valid value. Moreover, it is special. It means the
    -- 0-quoted operation should always be paused in favor of the other
    -- operation.
    --
    lsched.cfg({sched_move_quota = 0})
    test:ok(lsched.move_start(big_timeout), 'started move with 0 quota')

    local f = fiber.create(function()
        fiber_set_joinable()
        return lsched.ref_start(big_timeout)
    end)
    test:ok(not lsched.move_start(small_timeout), 'can not add more moves if '..
            'ref is queued - quota 0')

    lsched.move_end(1)
    local ok, new_timeout = f:join()
    test:ok(ok and new_timeout, 'ref is started')

    -- Ensure move never starts if there are always refs, when quota is 0.
    f = fiber.create(function()
        fiber_set_joinable()
        return lsched.move_start(big_timeout)
    end)
    local ref_count = lsched.ref_quota + 3
    -- Start from 2 to account the already existing ref.
    for _ = 2, ref_count do
        -- Start one new ref.
        assert(lsched.ref_start(big_timeout))
        -- Start second new ref.
        assert(lsched.ref_start(big_timeout))
        -- End first ref.
        lsched.ref_end(1)
        -- In result the refs are always interleaving - no time for moves at
        -- all.
    end
    test:is(lsched.ref_count, ref_count, 'refs exceed quota')
    test:ok(lsched.ref_strike > ref_count, 'strike is not interrupted')

    lsched.ref_end(ref_count)
    ok, new_timeout = f:join()
    test:ok(ok and new_timeout, 'move finally started')
    lsched.move_end(1)

    lsched.cfg({sched_move_quota = old_quota})
end

local test = tap.test('scheduler')
test:plan(11)

-- Change default values. Move is 1 by default, which would reduce the number of
-- possible tests. Ref is decreased to speed the tests up.
lsched.cfg({sched_ref_quota = 10, sched_move_quota = 5})

test:test('basic', test_basic)
test:test('negative timeout', test_negative_timeout)
test:test('ref gc', test_move_gc_ref)
test:test('ref strike', test_ref_strike)
test:test('move strike', test_move_strike)
test:test('ref add quota', test_ref_increase_quota)
test:test('move add quota', test_move_increase_quota)
test:test('ref decrease quota', test_ref_decrease_quota)
test:test('move decrease quota', test_move_decrease_quota)
test:test('ref zero quota', test_ref_zero_quota)
test:test('move zero quota', test_move_zero_quota)

os.exit(test:check() and 0 or 1)
