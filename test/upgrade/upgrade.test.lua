test_run = require('test_run').new()
git_util = require('git_util')
util = require('util')
vutil = require('vshard.util')

oldest_version = nil
is_at_least_3_0 = vutil.version_is_at_least(3, 0, 0, 'entrypoint', 0, 0)
-- On 3.0 old vshard versions won't work. The users are supposed to update
-- vshard first, and then update Tarantool.
if is_at_least_3_0 then                                                         \
-- Commit 'Support 3.0'.                                                        \
    oldest_version = 'e5f2cc022bb12b0b272a3bad026cd64f549abc9c'                 \
else                                                                            \
-- Commit 'Improve compatibility with 1.9'.                                     \
    oldest_version = '79a4dbfc4229e922cbfe4be259193a7b18dc089d'                 \
end
vshard_copy_path = util.git_checkout('vshard_git_tree_copy', oldest_version)

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }
test_run:create_cluster(REPLICASET_1, 'upgrade', {args = vshard_copy_path})
test_run:create_cluster(REPLICASET_2, 'upgrade', {args = vshard_copy_path})
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, [[                       \
    bootstrap_storage('memtx')                                                  \
    is_at_least_3_0 = %s                                                        \
]], is_at_least_3_0)

test_run:switch('storage_1_a')
if is_at_least_3_0 then                                                         \
    local t = box.space._schema:get{'vshard_version'}                           \
    assert(table.equals(t:totable(), {'vshard_version', 0, 1, 16, 0}))          \
else                                                                            \
    assert(box.space._schema:get{'oncevshard:storage:1'} ~= nil)                \
end

bucket_count = vshard.consts.DEFAULT_BUCKET_COUNT / 2
vshard.storage.bucket_force_create(1, bucket_count)
box.begin()                                                                     \
for i = 1, bucket_count do box.space.test:replace{i, i} end                     \
box.commit()
box.space.test:count()

test_run:switch('storage_2_a')
if is_at_least_3_0 then                                                         \
    local t = box.space._schema:get{'vshard_version'}                           \
    assert(table.equals(t:totable(), {'vshard_version', 0, 1, 16, 0}))          \
else                                                                            \
    assert(box.space._schema:get{'oncevshard:storage:1'} ~= nil)                \
end
bucket_count = vshard.consts.DEFAULT_BUCKET_COUNT / 2
first_bucket = vshard.consts.DEFAULT_BUCKET_COUNT / 2 + 1
vshard.storage.bucket_force_create(first_bucket, bucket_count)
box.begin()                                                                     \
for i = first_bucket, first_bucket + bucket_count - 1 do                        \
    box.space.test:replace{i, i}                                                \
end                                                                             \
box.commit()
box.space.test:count()

test_run:switch('default')
test_run:cmd('stop server storage_1_a')
test_run:cmd('start server storage_1_a')
test_run:cmd('stop server storage_1_b')
test_run:cmd('start server storage_1_b')

test_run:switch('storage_1_a')
vschema = require('vshard.storage.schema')
box.space._schema:get({'vshard_version'})
vschema.current_version()
vschema.latest_version
vshard.storage._call ~= nil
vshard.storage._call('test_api', 1, 2, 3)

test_run:switch('storage_1_b')
vschema = require('vshard.storage.schema')
test_run:wait_lsn('storage_1_b', 'storage_1_a')
box.space._schema:get({'vshard_version'})
vschema.current_version()
vschema.latest_version
vshard.storage._call ~= nil

test_run:switch('default')
-- Main purpose of the test - ensure that data can be safely moved
-- from an old instance to a newer one. Weight difference makes
-- rebalancer move the buckets from old storage_2 to new upgraded
-- storage_1.
util.map_evals(test_run, {REPLICASET_1, REPLICASET_2}, [[                       \
    cfg.sharding[ util.replicasets[2] ].weight = 1                              \
    cfg.sharding[ util.replicasets[1] ].weight = 2                              \
    cfg.rebalancer_max_sending = 5                                              \
    vshard.storage.cfg(cfg, util.name_to_uuid[NAME])                            \
]])

test_run:switch('storage_2_a')
wait_rebalancer_state('The cluster is balanced ok', test_run)
active_count = 0
index = box.space._bucket.index.status
for _, t in index:pairs({vshard.consts.BUCKET.ACTIVE}) do                       \
    active_count = active_count + 1                                             \
    assert(box.space.test:get({t.id}) ~= nil)                                   \
end
active_count

test_run:switch('storage_1_a')
active_count = 0
index = box.space._bucket.index.status
for _, t in index:pairs({vshard.consts.BUCKET.ACTIVE}) do                       \
    active_count = active_count + 1                                             \
    assert(box.space.test:get({t.id}) ~= nil)                                   \
end
active_count

test_run:switch('default')
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_1)
test_run:cmd('clear filter')
