test_run = require('test_run').new()
vshard = require('vshard')
fiber = require('fiber')
engine = test_run:get_cfg('engine')

test_run:cmd("setopt delimiter ';'")
function show_sharded_spaces()
    local result = {}
    for k, space in pairs(vshard.storage.sharded_spaces()) do
        table.insert(result, space.name)
    end
    table.sort(result)
    return result
end;
test_run:cmd("setopt delimiter ''");

vshard.storage.internal.shard_index = 'bucket_id'
vshard.storage.internal.collect_bucket_garbage_interval = vshard.consts.DEFAULT_COLLECT_BUCKET_GARBAGE_INTERVAL

--
-- Find nothing if no bucket_id anywhere, or there is no index
-- by it, or bucket_id is not unsigned.
--

s = box.schema.create_space('test', {engine = engine})
_ = s:create_index('pk')
--
-- gh-96: public API to see all sharded spaces.
--
show_sharded_spaces()

sk = s:create_index('bucket_id', {parts = {{2, 'string'}}})
show_sharded_spaces()

-- Bucket id must be the first part of an index.
sk:drop()
sk = s:create_index('bucket_id', {parts = {{1, 'unsigned'}, {2, 'unsigned'}}})
show_sharded_spaces()

-- Ok to find sharded space.
sk:drop()

--
-- gh-74: allow to choose any name for shard indexes.
--
sk = s:create_index('vbuckets', {parts = {{2, 'unsigned'}}, unique = false})
vshard.storage.internal.shard_index = 'vbuckets'
show_sharded_spaces()
sk:drop()
vshard.storage.internal.shard_index = 'bucket_id'

sk = s:create_index('bucket_id', {parts = {{2, 'unsigned'}}, unique = false})
show_sharded_spaces()

s2 = box.schema.create_space('test2', {engine = engine})
pk2 = s2:create_index('pk')
sk2 = s2:create_index('bucket_id', {parts = {{2, 'unsigned'}}, unique = false})
show_sharded_spaces()

s:drop()
s2:drop()

--
-- gh-111: cache sharded spaces based on schema version
--
cached_spaces = vshard.storage.internal.cached_find_sharded_spaces()
cached_spaces == vshard.storage.internal.cached_find_sharded_spaces()
s = box.schema.create_space('test', {engine = engine})
cached_spaces == vshard.storage.internal.cached_find_sharded_spaces()
s:drop()

--
-- Test garbage buckets deletion from space.
--
format = {}
format[1] = {name = 'id', type = 'unsigned'}
format[2] = {name = 'status', type = 'string'}
_bucket = box.schema.create_space('_bucket', {format = format})
_ = _bucket:create_index('pk')
_ = _bucket:create_index('status', {parts = {{2, 'string'}}, unique = false})
_bucket:replace{1, vshard.consts.BUCKET.ACTIVE}
_bucket:replace{2, vshard.consts.BUCKET.RECEIVING}
_bucket:replace{3, vshard.consts.BUCKET.ACTIVE}
_bucket:replace{4, vshard.consts.BUCKET.SENT}
_bucket:replace{5, vshard.consts.BUCKET.GARBAGE}
_bucket:replace{6, vshard.consts.BUCKET.GARBAGE}
_bucket:replace{200, vshard.consts.BUCKET.GARBAGE}

s = box.schema.create_space('test', {engine = engine})
pk = s:create_index('pk')
sk = s:create_index('bucket_id', {parts = {{2, 'unsigned'}}, unique = false})
s:replace{1, 1}
s:replace{2, 1}
s:replace{3, 2}
s:replace{4, 2}

gc_bucket_step_by_type = vshard.storage.internal.gc_bucket_step_by_type
s2 = box.schema.create_space('test2', {engine = engine})
pk2 = s2:create_index('pk')
sk2 = s2:create_index('bucket_id', {parts = {{2, 'unsigned'}}, unique = false})
s2:replace{1, 1}
s2:replace{3, 3}

test_run:cmd("setopt delimiter ';'")
function fill_spaces_with_garbage()
    s:replace{5, 100}
    s:replace{6, 100}
    s:replace{7, 4}
    s:replace{8, 5}
    for i = 9, 1107 do s:replace{i, 200} end
    s2:replace{4, 200}
    s2:replace{5, 100}
    s2:replace{5, 300}
    s2:replace{6, 4}
    s2:replace{7, 5}
    s2:replace{7, 6}
end;
test_run:cmd("setopt delimiter ''");

fill_spaces_with_garbage()

#s2:select{}
#s:select{}
gc_bucket_step_by_type(vshard.consts.BUCKET.GARBAGE)
#s2:select{}
#s:select{}
gc_bucket_step_by_type(vshard.consts.BUCKET.SENT)
s2:select{}
s:select{}
-- Nothing deleted - update collected generation.
gc_bucket_step_by_type(vshard.consts.BUCKET.GARBAGE)
gc_bucket_step_by_type(vshard.consts.BUCKET.SENT)
#s2:select{}
#s:select{}

--
-- Test continuous garbage collection via background fiber.
--
fill_spaces_with_garbage()
_ = _bucket:on_replace(function() vshard.storage.internal.bucket_generation = vshard.storage.internal.bucket_generation + 1 end)
f = fiber.create(vshard.storage.internal.gc_bucket_f)
-- Wait until garbage collection is finished.
while s2:count() ~= 3 or s:count() ~= 6 do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end
s:select{}
s2:select{}
-- Check garbage bucket is deleted by background fiber.
_bucket:select{}
--
-- Test deletion of 'sent' buckets after a specified timeout.
--
_bucket:replace{2, vshard.consts.BUCKET.SENT}
-- Wait deletion after a while.
while _bucket:get{2} ~= nil do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end
_bucket:select{}
s:select{}
s2:select{}

--
-- Test full lifecycle of a bucket.
--
_bucket:replace{4, vshard.consts.BUCKET.ACTIVE}
s:replace{5, 4}
s:replace{6, 4}
_bucket:replace{4, vshard.consts.BUCKET.SENT}
while _bucket:get{4} ~= nil do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end

--
-- Test WAL errors during deletion from _bucket.
--
function rollback_on_delete(old, new) if old ~= nil and new == nil then box.rollback() end end
_ = _bucket:on_replace(rollback_on_delete)
_bucket:replace{4, vshard.consts.BUCKET.SENT}
s:replace{5, 4}
s:replace{6, 4}
while not test_run:grep_log("default", "Error during deletion of empty sent buckets") do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end
while #sk:select{4} ~= 0 do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end
s:select{}
_bucket:select{}
_ = _bucket:on_replace(nil, rollback_on_delete)
while _bucket:get{4} ~= nil do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end

f:cancel()

--
-- Test API function to delete a specified bucket data.
--
util = require('util')

util.check_error(vshard.storage.bucket_delete_garbage)

-- Delete an existing garbage bucket.
_bucket:replace{4, vshard.consts.BUCKET.SENT}
s:replace{5, 4}
s:replace{6, 4}
vshard.storage.bucket_delete_garbage(4)
s:select{}

-- Delete a not existing garbage bucket.
_ = _bucket:delete{4}
s:replace{5, 4}
s:replace{6, 4}
vshard.storage.bucket_delete_garbage(4)
s:select{}

-- Fail to delete a not garbage bucket.
_bucket:replace{4, vshard.consts.BUCKET.ACTIVE}
s:replace{5, 4}
s:replace{6, 4}
util.check_error(vshard.storage.bucket_delete_garbage, 4)
util.check_error(vshard.storage.bucket_delete_garbage, 4, 10000)
-- 'Force' option ignores this error.
vshard.storage.bucket_delete_garbage(4, {force = true})
s:select{}

--
-- Test huge bucket count deletion.
--
for i = 1, 2000 do _bucket:replace{i, vshard.consts.BUCKET.GARBAGE} s:replace{i, i} s2:replace{i, i} end
#_bucket:select{}
#s:select{}
#s2:select{}
f = fiber.create(vshard.storage.internal.gc_bucket_f)
while _bucket:count() ~= 0 do vshard.storage.garbage_collector_wakeup() fiber.sleep(0.001) end
_bucket:select{}
s:select{}
s2:select{}
f:cancel()

s2:drop()
s:drop()
_bucket:drop()
