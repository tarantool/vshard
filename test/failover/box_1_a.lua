#!/usr/bin/env tarantool
-- Get instance name
require('strict').on()
local fio = require('fio')
NAME = fio.basename(arg[0], '.lua')
test_run = require('test_run').new()
require('console').listen(os.getenv('ADMIN'))

vshard = require('vshard')
names = dofile('names.lua')
cfg = dofile('config.lua')
cfg.weights = nil
vshard.storage.cfg(cfg, names.replica_uuid[NAME])

box.once('schema', function()
    box.schema.func.create('echo')
    box.schema.role.grant('public', 'execute', 'function', 'echo')
end)

echo_count = 0

function echo(...)
    echo_count = echo_count + 1
    return ...
end
