test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage()')

--
-- gh-147: refs allow to pin all the buckets on the storage at once. Is invented
-- for map-reduce functionality to pin all buckets on all storages in the
-- cluster to execute consistent map-reduce calls on all cluster data.
--

_ = test_run:switch('storage_1_a')
vshard.storage.rebalancer_disable()
vshard.storage.bucket_force_create(1, 1500)

_ = test_run:switch('storage_2_a')
vshard.storage.rebalancer_disable()
vshard.storage.bucket_force_create(1501, 1500)

_ = test_run:switch('storage_1_a')
lref = require('vshard.storage.ref')

--
-- Bucket moves are not allowed under a ref.
--
util = require('util')
sid = 0
rid = 0
big_timeout = 1000000
small_timeout = 0.001

timeout = 0.01
lref.add(rid, sid, big_timeout)
-- Send fails.
ok, err = vshard.storage.bucket_send(1, util.replicasets[2],                    \
                                     {timeout = timeout})
assert(not ok and err.message)
lref.use(rid, sid)
-- Still fails - use only makes ref undead until it is deleted explicitly.
ok, err = vshard.storage.bucket_send(1, util.replicasets[2],                    \
                                     {timeout = timeout})
assert(not ok and err.message)

_ = test_run:switch('storage_2_a')
-- Receive (from another replicaset) also fails.
big_timeout = 1000000
timeout = 0.01
ok, err = vshard.storage.bucket_send(1501, util.replicasets[1],                 \
                                     {timeout = timeout})
assert(not ok and util.is_timeout_error(err))

--
-- After unref all the bucket moves are allowed again.
--
_ = test_run:switch('storage_1_a')
lref.del(rid, sid)

vshard.storage.bucket_send(1, util.replicasets[2], {timeout = big_timeout})
wait_bucket_is_collected(1)

_ = test_run:switch('storage_2_a')
vshard.storage.bucket_send(1, util.replicasets[1], {timeout = big_timeout})
wait_bucket_is_collected(1)

--
-- While bucket move is in progress, ref won't work.
--

_ = test_run:switch('storage_1_a')
fiber = require('fiber')
vshard.storage.internal.errinj.ERRINJ_LAST_SEND_DELAY = true
_ = fiber.create(vshard.storage.bucket_send, 1, util.replicasets[2],            \
                 {timeout = big_timeout})
ok, err = lref.add(rid, sid, small_timeout)
assert(not ok and util.is_timeout_error(err))
-- Ref will wait if timeout is big enough.
ok, err = nil
_ = fiber.create(function()                                                     \
    ok, err = lref.add(rid, sid, big_timeout)                                   \
end)

_ = test_run:switch('storage_1_a')
vshard.storage.internal.errinj.ERRINJ_LAST_SEND_DELAY = false
wait_bucket_is_collected(1)
test_run:wait_cond(function() return ok or err end)
lref.use(rid, sid)
lref.del(rid, sid)
assert(ok and not err)

_ = test_run:switch('storage_2_a')
vshard.storage.bucket_send(1, util.replicasets[1], {timeout = big_timeout})
wait_bucket_is_collected(1)

--
-- Refs are bound to sessions.
--
box.schema.user.grant('storage', 'super')
lref = require('vshard.storage.ref')
small_timeout = 0.001
function make_ref(rid, timeout)                                                 \
    return lref.add(rid, box.session.id(), timeout)                             \
end
function check_ref(rid)                                                         \
    return lref.check(rid, box.session.id())                                    \
end
function use_ref(rid)                                                           \
    return lref.use(rid, box.session.id())                                      \
end
function del_ref(rid)                                                           \
    return lref.del(rid, box.session.id())                                      \
end

_ = test_run:switch('storage_1_a')
netbox = require('net.box')
remote_uri = test_run:eval('storage_2_a', 'return box.cfg.listen')[1]
c = netbox.connect(remote_uri)

-- Ref is added and does not disappear anywhere on its own.
c:call('make_ref', {1, small_timeout})
_ = test_run:switch('storage_2_a')
assert(lref.count == 1)
_ = test_run:switch('storage_1_a')

-- Check works.
ok, err = c:call('check_ref', {1})
assert(ok and not err)
_ = test_run:switch('storage_2_a')
assert(lref.count == 1)
_ = test_run:switch('storage_1_a')

ok, err = c:call('check_ref', {2})
assert(ok == nil and err.message)
_ = test_run:switch('storage_2_a')
assert(lref.count == 1)
_ = test_run:switch('storage_1_a')

-- Use works.
c:call('use_ref', {1})
_ = test_run:switch('storage_2_a')
assert(lref.count == 1)
_ = test_run:switch('storage_1_a')

-- Del works.
c:call('del_ref', {1})
_ = test_run:switch('storage_2_a')
assert(lref.count == 0)
_ = test_run:switch('storage_1_a')

-- Expiration works. Try to add a second ref when the first one is expired - the
-- first is collected and a subsequent use and del won't work.
c:call('make_ref', {1, small_timeout})
_ = test_run:switch('storage_2_a')
assert(lref.count == 1)
_ = test_run:switch('storage_1_a')

fiber.sleep(small_timeout)
c:call('make_ref', {2, small_timeout})
ok, err = c:call('use_ref', {1})
assert(ok == nil and err.message)
ok, err = c:call('del_ref', {1})
assert(ok == nil and err.message)
_ = test_run:switch('storage_2_a')
assert(lref.count == 1)
_ = test_run:switch('storage_1_a')

--
-- Session disconnect keeps the refs, but the session is deleted when
-- used ref count becomes 0. Unused refs don't prevent session deletion.
--
_ = test_run:switch('storage_2_a')
keep_long_ref = true
function long_ref_request(rid)                                                  \
    local sid = box.session.id()                                                \
    assert(lref.add(rid, sid, big_timeout))                                     \
    assert(lref.use(rid, sid))                                                  \
    while keep_long_ref do                                                      \
        fiber.sleep(small_timeout)                                              \
    end                                                                         \
    assert(lref.del(rid, sid))                                                  \
end

_ = test_run:switch('storage_1_a')
_ = c:call('long_ref_request', {3}, {is_async = true})
c:call('make_ref', {4, big_timeout})

_ = test_run:switch('storage_2_a')
test_run:wait_cond(function() return lref.count == 2 end)

_ = test_run:switch('storage_1_a')
c:close()

_ = test_run:switch('storage_2_a')
-- Still 2 refs.
assert(lref.count == 2)
-- The long request ends and the session must be deleted - that was the last
-- used ref.
keep_long_ref = false
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch("default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
