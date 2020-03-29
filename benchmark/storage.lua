local common = require('common')
local fiber = require('fiber')
local fio = require('fio')
local log = require('log')
require('strict').on()
vshard = require('vshard')

local instance_name = fio.basename(arg[0], '.lua')

vshard.storage.cfg(common.config, common.instance_uuid[instance_name])

log.info('Start storage %s', instance_name)
local bench_space

function bench_call_yield()
    fiber.yield()
end

function bench_call_echo(...)
    return ...
end

function bench_call_select(limit)
    return bench_space:select(nil, {limit = limit})
end

function bench_call_random()
    return box.space.bench.index.pk:random()
end

box.once('bench_schema', function()
    local format = {}
    format[1] = {name = 'id', type = 'unsigned'}
    format[2] = {name = 'bucket_id', type = 'unsigned'}
    local space = box.schema.create_space('bench', {format = format})
    space:create_index('pk')
    space:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
    bench_space = space
    box.schema.user.grant('guest', 'super')
end)

log.info('Storage %s is started', instance_name)
