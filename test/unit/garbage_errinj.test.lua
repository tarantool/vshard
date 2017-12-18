test_run = require('test_run').new()
vshard = require('vshard')
fiber = require('fiber')

format = {}
format[1] = {name = 'id', type = 'unsigned'}
format[2] = {name = 'status', type = 'string', is_nullable = true}
_bucket = box.schema.create_space('_bucket', {format = format})
_ = _bucket:create_index('pk')
_ = _bucket:create_index('status', {parts = {{2, 'string'}}, unique = false})
_bucket:replace{1, vshard.consts.BUCKET.ACTIVE}
_bucket:replace{2, vshard.consts.BUCKET.RECEIVING}
_bucket:replace{3, vshard.consts.BUCKET.ACTIVE}
_bucket:replace{4, vshard.consts.BUCKET.SENT}
_bucket:replace{5, vshard.consts.BUCKET.GARBAGE}

s = box.schema.create_space('test')
pk = s:create_index('pk')
sk = s:create_index('bucket_id', {parts = {{2, 'unsigned'}}, unique = false})
s:replace{1, 1}
s:replace{2, 1}
s:replace{3, 2}
s:replace{4, 2}
s:replace{5, 100}
s:replace{6, 100}
s:replace{7, 4}
s:replace{8, 5}

s2 = box.schema.create_space('test2')
pk2 = s2:create_index('pk')
sk2 = s2:create_index('bucket_id', {parts = {{2, 'unsigned'}}, unique = false})
s2:replace{1, 1}
s2:replace{3, 3}
for i = 7, 1107 do s:replace{i, 200} end
s2:replace{4, 200}
s2:replace{5, 100}
s2:replace{5, 300}
s2:replace{6, 4}
s2:replace{7, 5}

garbage_step = vshard.storage.internal.collect_garbage_step
control = {bucket_generation = 0, bucket_generation_collected = -1}

-- Restart garbage collection.
garbage_step(control)
control.bucket_generation_collected

--
-- Test _bucket generation change during garbage buckets search.
--
s:truncate()
control.bucket_generation_collected = -1
control.bucket_generation = 1
vshard.storage.internal.errinj.ERRINJ_BUCKET_FIND_GARBAGE_DELAY = true
f = fiber.create(function() garbage_step(control) end)
_bucket:replace{4, vshard.consts.BUCKET.GARBAGE}
s:replace{5, 4}
s:replace{6, 4}
#s:select{}
vshard.storage.internal.errinj.ERRINJ_BUCKET_FIND_GARBAGE_DELAY = false
while f:status() ~= 'dead' do fiber.sleep(0.1) end
-- Nothing is deleted - _bucket:replace() has changed _bucket
-- generation during search of garbage buckets.
#s:select{}
_bucket:select{4}
-- Next step deletes garbage ok.
garbage_step(control)
#s:select{}
_bucket:delete{4}

s2:drop()
s:drop()
_bucket:drop()
