#!/usr/bin/env tarantool

NAME = require('fio').basename(arg[0], '.lua')
fiber = require('fiber')
test_run = require('test_run').new()
util = require('util')
require('console').listen(os.getenv('ADMIN'))
cfg = rawget(_G, "cfg") or require('localcfg')
log = require('log')
if not cfg.shard_index then
    cfg.shard_index = 'bucket_id'
end

vshard = require('vshard')
echo_count = 0
cfg.replication_connect_timeout = 3
vshard.storage.cfg(cfg, util.name_to_uuid[NAME])
function bootstrap_storage(engine)
    box.once("testapp:schema:1", function()
        if rawget(_G, 'CHANGE_SPACE_IDS') then
            box.schema.create_space("CHANGE_SPACE_IDS")
        end
        local format = {{'id', 'unsigned'}, {'bucket_id', 'unsigned'}}
        local s = box.schema.create_space('test', {engine = engine, format = format})
        s:create_index('pk', {parts = {{'id'}}})
        s:create_index(cfg.shard_index, {parts = {{'bucket_id'}}, unique = false})

        local s2 = box.schema.create_space('test2', {engine = engine, format = format})
        s2:create_index('pk', {parts = {{'id'}}})
        s2:create_index(cfg.shard_index, {parts = {{'bucket_id'}}, unique = false})

        box.schema.func.create('echo')
        box.schema.role.grant('public', 'execute', 'function', 'echo')
        box.schema.func.create('sleep')
        box.schema.role.grant('public', 'execute', 'function', 'sleep')
        box.schema.func.create('space_get')
        box.schema.role.grant('public', 'execute', 'function', 'space_get')
        box.schema.func.create('space_insert')
        box.schema.role.grant('public', 'execute', 'function', 'space_insert')
        box.schema.func.create('do_replace')
        box.schema.role.grant('public', 'execute', 'function', 'do_replace')
        box.schema.func.create('do_select')
        box.schema.role.grant('public', 'execute', 'function', 'do_select')
        box.schema.func.create('raise_luajit_error')
        box.schema.role.grant('public', 'execute', 'function', 'raise_luajit_error')
        box.schema.func.create('raise_client_error')
        box.schema.role.grant('public', 'execute', 'function', 'raise_client_error')
        box.schema.func.create('do_push')
        box.schema.role.grant('public', 'execute', 'function', 'do_push')
        box.snapshot()
    end)
end

function echo(...)
    echo_count = echo_count + 1
    return ...
end

function space_get(space_name, key)
    return box.space[space_name]:get(key)
end

function space_insert(space_name, tuple)
    return box.space[space_name]:insert(tuple)
end

function do_replace(...)
    box.space.test:replace(...)
    return true
end

function do_select(...)
    return box.space.test:select(...)
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

function check_consistency()
    for _, tuple in box.space.test:pairs() do
        assert(box.space._bucket:get{tuple.bucket_id})
    end
    return true
end

function do_push(push, retval)
    box.session.push(push)
    return retval
end

--
-- Wait a specified log message.
-- Requirements:
-- * Should be executed from a storage with a rebalancer.
-- * NAME - global variable, name of instance should be set.
function wait_rebalancer_state(state, test_run)
    log.info(string.rep('a', 1000))
    vshard.storage.rebalancer_wakeup()
    while not test_run:grep_log(NAME, state, 1000) do
        fiber.sleep(0.1)
        vshard.storage.rebalancer_wakeup()
    end
end
