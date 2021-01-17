test_run = require('test_run').new()
netbox = require('net.box')
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'storage')
test_run:create_cluster(REPLICASET_2, 'storage')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage()')

_ = test_run:switch('storage_1_a')
vshard.storage.rebalancer_disable()
vshard.storage.bucket_force_create(1, 1500)

_ = test_run:switch('storage_2_a')
vshard.storage.rebalancer_disable()
vshard.storage.bucket_force_create(1501, 1500)

_ = test_run:switch('storage_1_a')
lref = require('vshard.storage.ref')
--
-- Simple ref-unref sequence.
--
rid = 0
sid = 0
timeout = 1000
lref.add(rid, sid, timeout)
lref.count
lref.use(rid, sid)
lref.count
lref.del(rid, sid)
lref.count

-- Bad ref ID.
rid = 1
_, err = lref.use(rid, sid)
err.name
_, err = lref.del(rid, sid)
err.name
-- Bad session ID.
rid = 0
sid = 1
_, err = lref.use(rid, sid)
err.name
_, err = lref.del(rid, sid)
err.name

-- Duplicate ID.
lref.add(rid, sid, timeout)
_, err = lref.add(rid, sid, timeout)
err.name
lref.count
lref.use(rid, sid)
lref.del(rid, sid)
lref.count

-- Ref addition expires 2 old refs.
sid = 0
timeout = 0.000001
do                                                                              \
    lref.add(0, sid, timeout)                                                   \
    lref.add(1, sid, timeout)                                                   \
    lref.add(2, sid, timeout)                                                   \
end
fiber.sleep(timeout)
lref.count
lref.add(3, sid, 0)
lref.count
lref.add(4, sid, 0)
lref.count

-- Ensure that if a ref is used, it updates the session heap properly.
big_timeout = 1000000
sid2 = 1
do                                                                              \
    lref.add(0, sid, big_timeout)                                               \
    lref.add(1, sid, timeout)                                                   \
    lref.use(0, sid)                                                            \
    lref.del(0, sid)                                                            \
    lref.gc()                                                                   \
    return lref.count                                                           \
end
-- Now the other ref should be collected by GC, because its session should have
-- been moved on top.
fiber.sleep(timeout)
lref.gc()
lref.count

--
-- Bucket moves are not allowed under a ref.
--
util = require('util')
rid = 0
lref.add(rid, sid, big_timeout)
-- Send fails.
vshard.storage.bucket_send(1, util.replicasets[2], {timeout = big_timeout})
lref.use(rid, sid)
-- Still fails - use only makes ref undead until it is deleted explicitly.
vshard.storage.bucket_send(1, util.replicasets[2], {timeout = big_timeout})

_ = test_run:switch('storage_2_a')
-- Receive (from another replicaset) also fails.
big_timeout = 1000000
vshard.storage.bucket_send(1501, util.replicasets[1], {timeout = big_timeout})

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
vshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = true

_ = test_run:switch('storage_1_a')
fiber = require('fiber')
_ = fiber.create(vshard.storage.bucket_send, 1, util.replicasets[2],            \
                 {timeout = big_timeout})
ok, err = lref.add(rid, sid, timeout)
err.type
-- Ref will wait if timeout is big enough.
ok, err = nil, nil
_ = fiber.create(function()                                                     \
    ok, err = lref.add(rid, sid, big_timeout)                                   \
end)

_ = test_run:switch('storage_2_a')
vshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = false

_ = test_run:switch('storage_1_a')
wait_bucket_is_collected(1)
test_run:wait_cond(function() return ok or err end)
lref.use(rid, sid)
lref.del(rid, sid)
ok, err

_ = test_run:switch('storage_2_a')
vshard.storage.bucket_send(1, util.replicasets[1], {timeout = big_timeout})
wait_bucket_is_collected(1)

-- --
-- -- Bucket move tries to collect expired refs before give up.
-- --
-- _ = test_run:switch('storage_1_a')
-- do                                                                              \
--     ref = vshard.storage.internal.storage_ref_add(rid, sid, timeout)            \
--     ok, err = vshard.storage.bucket_send(1, util.replicasets[2],                \
--                                          {timeout = big_timeout})               \
-- end
-- ok, err
-- fiber.sleep(timeout)
-- vshard.storage.bucket_send(1, util.replicasets[2], {timeout = big_timeout})
-- -- Fails - the ref has expired.
-- vshard.storage.internal.storage_ref_use(rid, sid)
-- wait_bucket_is_collected(1)

_ = test_run:switch("default")
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
