#!/usr/bin/env tarantool

local NAME = require('fio').basename(arg[0], '.lua')
fiber = require('fiber')
test_run = require('test_run').new()
util = require('util')
require('console').listen(os.getenv('ADMIN'))
cfg = require('localcfg')

vshard = require('vshard')
cfg.replication_connect_timeout = 3
vshard.storage.cfg(cfg, util.name_to_uuid[NAME])
function bootstrap_storage(engine)
    box.once("testapp:schema:1", function()
        local format = {{'id', 'unsigned'}, {'bucket_id', 'unsigned'}}
        local s = box.schema.create_space('test', {engine = engine, format = format})
        s:create_index('pk', {parts = {{'id'}}})
        s:create_index('bucket_id', {parts = {{'bucket_id'}}, unique = false})

        local s2 = box.schema.create_space('test2', {engine = engine, format = format})
        s2:create_index('pk', {parts = {{'id'}}})
        s2:create_index('bucket_id', {parts = {{'bucket_id'}}, unique = false})

        box.schema.func.create('echo')
        box.schema.role.grant('public', 'execute', 'function', 'echo')
        box.schema.func.create('sleep')
        box.schema.role.grant('public', 'execute', 'function', 'sleep')
        box.schema.func.create('space_get')
        box.schema.role.grant('public', 'execute', 'function', 'space_get')
        box.schema.func.create('space_insert')
        box.schema.role.grant('public', 'execute', 'function', 'space_insert')
        box.schema.func.create('raise_luajit_error')
        box.schema.role.grant('public', 'execute', 'function', 'raise_luajit_error')
        box.schema.func.create('raise_client_error')
        box.schema.role.grant('public', 'execute', 'function', 'raise_client_error')
        box.snapshot()
    end)
end

function echo(...)
    return ...
end

function space_get(space_name, key)
    return box.space[space_name]:get(key)
end

function space_insert(space_name, tuple)
    return box.space[space_name]:insert(tuple)
end

function sleep(time)
    fiber.sleep(time)
    return true
end

function raise_luajit_error()
    assert(1 == 2)
end

function raise_client_error()
    box.error(box.error.UNKNOWN)
end
