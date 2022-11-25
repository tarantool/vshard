test_run = require('test_run').new()
REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'router')
test_run:create_cluster(REPLICASET_2, 'router')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, 'bootstrap_storage()')
util.push_rs_filters(test_run)
_ = test_run:cmd("create server router_1 with script='router/router_1.lua'")
_ = test_run:cmd("start server router_1")

_ = test_run:switch("router_1")
util = require('util')

--
-- gh-147: consistent map-reduce.
--
big_timeout = 1000000
big_timeout_opts = {timeout = big_timeout}
vshard.router.cfg(cfg)
vshard.router.bootstrap(big_timeout_opts)
-- Trivial basic sanity test. Multireturn is not supported, should be truncated.
vshard.router.map_callrw('echo', {1, 2, 3}, big_timeout_opts)

--
-- Fail during connecting to storages. For the succeeded storages the router
-- tries to send unref.
--
timeout = 0.001
timeout_opts = {timeout = timeout}

test_run:cmd('stop server storage_1_a')
ok, err = vshard.router.map_callrw('echo', {1}, timeout_opts)
assert(not ok and err.message)
-- Even if ref was sent successfully to storage_2_a, it was deleted before
-- router returned an error.
_ = test_run:switch('storage_2_a')
lref = require('vshard.storage.ref')
-- Wait because unref is sent asynchronously. Could arrive not immediately.
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch('router_1')
test_run:cmd('start server storage_1_a')
-- Works again - router waited for connection being established.
vshard.router.map_callrw('echo', {1}, big_timeout_opts)

--
-- Do all the same but with another storage being stopped. The same test is done
-- again because can't tell at which of the tests to where the router will go
-- first.
--
test_run:cmd('stop server storage_2_a')
ok, err = vshard.router.map_callrw('echo', {1}, timeout_opts)
assert(not ok and err.message)
_ = test_run:switch('storage_1_a')
lref = require('vshard.storage.ref')
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch('router_1')
test_run:cmd('start server storage_2_a')
vshard.router.map_callrw('echo', {1}, big_timeout_opts)

--
-- Fail at ref stage handling. Unrefs are sent to cancel those refs which
-- succeeded. To simulate a ref fail make the router think there is a moving
-- bucket.
--
_ = test_run:switch('storage_1_a')
lsched = require('vshard.storage.sched')
big_timeout = 1000000
lsched.move_start(big_timeout)

_ = test_run:switch('router_1')
ok, err = vshard.router.map_callrw('echo', {1}, timeout_opts)
assert(not ok and util.is_timeout_error(err))

_ = test_run:switch('storage_2_a')
lsched = require('vshard.storage.sched')
lref = require('vshard.storage.ref')
test_run:wait_cond(function() return lref.count == 0 end)

--
-- Do all the same with another storage being busy with a 'move'.
--
big_timeout = 1000000
lsched.move_start(big_timeout)

_ = test_run:switch('storage_1_a')
lref = require('vshard.storage.ref')
lsched.move_end(1)
assert(lref.count == 0)

_ = test_run:switch('router_1')
ok, err = vshard.router.map_callrw('echo', {1}, timeout_opts)
assert(not ok and util.is_timeout_error(err))

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch('storage_2_a')
lsched.move_end(1)
assert(lref.count == 0)

_ = test_run:switch('router_1')
vshard.router.map_callrw('echo', {1}, big_timeout_opts)

--
-- Ref can fail earlier than by a timeout. Router still should broadcast unrefs
-- correctly. To simulate ref fail add a duplicate manually.
--
_ = test_run:switch('storage_1_a')
box.schema.user.grant('storage', 'super')
router_sid = nil
function save_router_sid()                                                      \
    router_sid = box.session.id()                                               \
end

_ = test_run:switch('storage_2_a')
box.schema.user.grant('storage', 'super')
router_sid = nil
function save_router_sid()                                                      \
    router_sid = box.session.id()                                               \
end

_ = test_run:switch('router_1')
vshard.router.map_callrw('save_router_sid', {}, big_timeout_opts)

_ = test_run:switch('storage_1_a')
lref.add(1, router_sid, big_timeout)

_ = test_run:switch('router_1')
vshard.router.internal.ref_id = 1
ok, err = vshard.router.map_callrw('echo', {1}, big_timeout_opts)
assert(not ok and err.message)

_ = test_run:switch('storage_1_a')
_ = lref.del(1, router_sid)

_ = test_run:switch('storage_2_a')
test_run:wait_cond(function() return lref.count == 0 end)
lref.add(1, router_sid, big_timeout)

_ = test_run:switch('router_1')
vshard.router.internal.ref_id = 1
ok, err = vshard.router.map_callrw('echo', {1}, big_timeout_opts)
assert(not ok and err.message)

_ = test_run:switch('storage_2_a')
_ = lref.del(1, router_sid)

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return lref.count == 0 end)

--
-- Fail if some buckets are not visible. Even if all the known replicasets were
-- scanned. It means consistency violation.
--
test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1}, false)

_ = test_run:switch('storage_1_a')
bucket_id = box.space._bucket.index.pk:min().id
vshard.storage.bucket_force_drop(bucket_id)
vshard.storage.sync()

test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_1}, true)

_ = test_run:switch('router_1')
ok, err = vshard.router.map_callrw('echo', {1}, big_timeout_opts)
assert(not ok and err.message)

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return lref.count == 0 end)
vshard.storage.bucket_force_create(bucket_id)

test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_2}, false)

_ = test_run:switch('storage_2_a')
test_run:wait_cond(function() return lref.count == 0 end)
bucket_id = box.space._bucket.index.pk:min().id
vshard.storage.bucket_force_drop(bucket_id)
vshard.storage.sync()

test_run:switch('default')
util.map_bucket_protection(test_run, {REPLICASET_2}, true)

_ = test_run:switch('router_1')
ok, err = vshard.router.map_callrw('echo', {1}, big_timeout_opts)
assert(not ok and err.message)

_ = test_run:switch('storage_2_a')
test_run:wait_cond(function() return lref.count == 0 end)
vshard.storage.bucket_force_create(bucket_id)

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return lref.count == 0 end)

--
-- Storage map unit tests.
--

-- Map fails not being able to use the ref.
ok, err = vshard.storage._call('storage_map', 0, 'echo', {1})
ok, err.message

-- Map fails and clears the ref when the user function fails.
vshard.storage._call('storage_ref', 0, big_timeout)
assert(lref.count == 1)
ok, err = vshard.storage._call('storage_map', 0, 'raise_client_error', {})
assert(lref.count == 0)
assert(not ok and err.message)

-- Map fails gracefully when couldn't delete the ref.
vshard.storage._call('storage_ref', 0, big_timeout)
ok, err = vshard.storage._call('storage_map', 0, 'vshard.storage._call',        \
                               {'storage_unref', 0})
assert(lref.count == 0)
assert(not ok and err.message)

--
-- Map fail is handled and the router tries to send unrefs.
--
_ = test_run:switch('storage_1_a')
need_throw = true
function map_throw()                                                            \
    if need_throw then                                                          \
        raise_client_error()                                                    \
    end                                                                         \
    return '+'                                                                  \
end

_ = test_run:switch('storage_2_a')
need_throw = false
function map_throw()                                                            \
    if need_throw then                                                          \
        raise_client_error()                                                    \
    end                                                                         \
    return '+'                                                                  \
end

_ = test_run:switch('router_1')
ok, err = vshard.router.map_callrw('raise_client_error', {}, big_timeout_opts)
ok, err.message

_ = test_run:switch('storage_1_a')
need_throw = false
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch('storage_2_a')
need_throw = true
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch('router_1')
ok, err = vshard.router.map_callrw('raise_client_error', {}, big_timeout_opts)
ok, err.message

_ = test_run:switch('storage_1_a')
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch('storage_2_a')
test_run:wait_cond(function() return lref.count == 0 end)

_ = test_run:switch('default')
_ = test_run:cmd("stop server router_1")
_ = test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
_ = test_run:cmd('clear filter')
