#!/usr/bin/env tarantool

local tap = require('tap')
local fiber = require('fiber')
local lregistry = require('vshard.registry')
local lref = require('vshard.storage.ref')

local big_timeout = 1000000
local small_timeout = 0.000001
local sid = 0
local sid2 = 1
local sid3 = 2

--
-- gh-147: refs allow to pin all the buckets on the storage at once. Is invented
-- for map-reduce functionality to pin all buckets on all storages in the
-- cluster to execute consistent map-reduce calls on all cluster data.
--

--
-- Refs use storage API to get bucket space state and wait on its changes. But
-- not important for these unit tests.
--
local function bucket_are_all_rw()
    return true
end

lregistry.storage = {
    bucket_are_all_rw = bucket_are_all_rw,
}

local function test_ref_basic(test)
    test:plan(15)

    local rid = 0
    local ok, err
    --
    -- Basic ref/unref.
    --
    ok, err = lref.add(rid, sid, big_timeout)
    test:ok(ok and not err, '+1 ref')
    test:is(lref.count, 1, 'accounted')
    ok, err = lref.use(rid, sid)
    test:ok(ok and not err, 'use the ref')
    test:is(lref.count, 1, 'but still accounted')
    ok, err = lref.del(rid, sid)
    test:ok(ok and not err, '-1 ref')
    test:is(lref.count, 0, 'accounted')

    --
    -- Bad ref ID.
    --
    rid = 1
    ok, err = lref.use(rid, sid)
    test:ok(not ok and err, 'invalid RID at use')
    ok, err = lref.del(rid, sid)
    test:ok(not ok and err, 'invalid RID at del')

    --
    -- Bad session ID.
    --
    lref.kill(sid)
    rid = 0
    ok, err = lref.use(rid, sid)
    test:ok(not ok and err, 'invalid SID at use')
    ok, err = lref.del(rid, sid)
    test:ok(not ok and err, 'invalid SID at del')

    --
    -- Duplicate ID.
    --
    ok, err = lref.add(rid, sid, big_timeout)
    test:ok(ok and not err, 'add ref')
    ok, err = lref.add(rid, sid, big_timeout)
    test:ok(not ok and err, 'duplicate ref')
    test:is(lref.count, 1, 'did not affect count')
    test:ok(lref.use(rid, sid) and lref.del(rid, sid), 'del old ref')
    test:is(lref.count, 0, 'accounted')
end

local function test_ref_incremental_gc(test)
    test:plan(20)

    --
    -- Ref addition expires 2 old refs.
    --
    local ok, err
    for i = 0, 2 do
        assert(lref.add(i, sid, small_timeout))
    end
    fiber.sleep(small_timeout)
    test:is(lref.count, 3, 'expired refs are still here')
    test:ok(lref.add(3, sid, 0), 'add new ref')
    -- 3 + 1 new - 2 old = 2.
    test:is(lref.count, 2, 'it collected 2 old refs')
    -- Sleep again so the just created ref with 0 timeout becomes older than the
    -- deadline.
    fiber.sleep(small_timeout)
    test:ok(lref.add(4, sid, 0), 'add new ref')
    -- 2 + 1 new - 2 old = 1.
    test:is(lref.count, 1, 'it collected 2 old refs')
    test:ok(lref.del(4, sid), 'del the latest manually')

    --
    -- Incremental GC works fine if only one ref was GCed.
    --
    test:ok(lref.add(0, sid, small_timeout), 'add ref with small timeout')
    test:ok(lref.add(1, sid, big_timeout), 'add ref with big timeout')
    fiber.sleep(small_timeout)
    test:ok(lref.add(2, sid, 0), 'add ref with 0 timeout')
    test:is(lref.count, 2, 'collected 1 old ref, 1 is kept')
    test:ok(lref.del(2, sid), 'del newest ref, it was not collected')
    test:ok(lref.del(1, sid), 'del ref with big timeout')
    test:ok(lref.count, 0, 'all is deleted')

    --
    -- GC works fine when only one ref was left and it was expired.
    --
    test:ok(lref.add(0, sid, small_timeout), 'add ref with small timeout')
    test:is(lref.count, 1, '1 ref total')
    fiber.sleep(small_timeout)
    test:ok(lref.add(1, sid, big_timeout), 'add ref with big timeout')
    test:is(lref.count, 1, 'collected the old one')
    lref.gc()
    test:is(lref.count, 1, 'still 1 - timeout was big')
    test:ok(lref.del(1, sid), 'delete it')
    test:is(lref.count, 0, 'no refs')
end

local function test_ref_gc(test)
    test:plan(7)

    --
    -- Generic GC works fine with multiple sessions.
    --
    assert(lref.add(0, sid, big_timeout))
    assert(lref.add(1, sid, small_timeout))
    assert(lref.add(0, sid3, small_timeout))
    assert(lref.add(0, sid2, small_timeout))
    assert(lref.add(1, sid2, big_timeout))
    assert(lref.add(1, sid3, big_timeout))
    test:is(lref.count, 6, 'add 6 refs total')
    fiber.sleep(small_timeout)
    lref.gc()
    test:is(lref.count, 3, '3 collected')
    test:ok(lref.del(0, sid), 'del first')
    test:ok(lref.del(1, sid2), 'del second')
    test:ok(lref.del(1, sid3), 'del third')
    test:is(lref.count, 0, '3 deleted')
    lref.gc()
    test:is(lref.count, 0, 'gc on empty refs did not break anything')
end

local function test_ref_use(test)
    test:plan(7)

    --
    -- Ref use updates the session heap.
    --
    assert(lref.add(0, sid, small_timeout))
    assert(lref.add(0, sid2, big_timeout))
    test:ok(lref.count, 2, 'add 2 refs')
    test:ok(lref.use(0, sid), 'use one with small timeout')
    lref.gc()
    test:is(lref.count, 2, 'still 2 refs')
    fiber.sleep(small_timeout)
    test:is(lref.count, 2, 'still 2 refs after sleep')
    test:ok(lref.del(0, sid, 'del first'))
    test:ok(lref.del(0, sid2, 'del second'))
    test:is(lref.count, 0, 'now all is deleted')
end

local function test_ref_del(test)
    test:plan(7)

    --
    -- Ref del updates the session heap.
    --
    assert(lref.add(0, sid, small_timeout))
    assert(lref.add(0, sid2, big_timeout))
    test:is(lref.count, 2, 'add 2 refs')
    test:ok(lref.del(0, sid), 'del with small timeout')
    lref.gc()
    test:is(lref.count, 1, '1 ref remains')
    fiber.sleep(small_timeout)
    test:is(lref.count, 1, '1 ref remains after sleep')
    lref.gc()
    test:is(lref.count, 1, '1 ref remains after sleep and gc')
    test:ok(lref.del(0, sid2), 'del with big timeout')
    test:is(lref.count, 0, 'now all is deleted')
end

local function test_ref_dead_session(test)
    test:plan(4)

    --
    -- Session after disconnect still might have running requests. It must
    -- be kept alive with its refs until the requests are done.
    --
    assert(lref.add(0, sid, small_timeout))
    assert(lref.use(0, sid))
    lref.kill(sid)
    test:ok(lref.del(0, sid))

    --
    -- The dead session is kept only while the used requests are running. It is
    -- deleted when use count becomes 0 even if there were unused refs.
    --
    assert(lref.add(0, sid, big_timeout))
    assert(lref.add(1, sid, big_timeout))
    assert(lref.use(0, sid))
    lref.kill(sid)
    test:is(lref.count, 2, '2 refs in a dead session')
    test:ok(lref.del(0, sid), 'delete the used ref')
    test:is(lref.count, 0, '0 refs - the unused ref was deleted with session')
end

local test = tap.test('ref')
test:plan(6)

test:test('basic', test_ref_basic)
test:test('incremental gc', test_ref_incremental_gc)
test:test('gc', test_ref_gc)
test:test('use', test_ref_use)
test:test('del', test_ref_del)
test:test('dead session', test_ref_dead_session)

os.exit(test:check() and 0 or 1)
