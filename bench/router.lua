#!/usr/bin/env tarantool

log = require('log')
cfg = require('config')
fiber = require('fiber')
cfg.listen = 3300

vshard = require('vshard')
verror = require('vshard.error')
vshard.router.cfg(cfg)
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

local bootstrap_timeout = 10
local deadline = fiber.clock() + bootstrap_timeout
while true do
    local ok, err = vshard.router.bootstrap()
    if ok then
        break
    end
    if err.type == 'ShardingError' and err.code == verror.code.NON_EMPTY then
        break
    end
    log.error('Error during bootstrap: %s', err)
    assert(fiber.clock() < deadline)
    fiber.sleep(1)
end

local opts_return_raw = {
    return_raw = true,
    timeout = 100000,
}

local opts_normal = {
    timeout = 100000,
}

local empty_args = {}

function echo_return_raw(raw)
    raw = raw:iterator()
    raw:decode_array_header()
    local bucket_id = raw:decode()
    local args = raw:take()
    return vshard.router.callbro(bucket_id, 'echo', args, opts_return_raw)
end

function echo_normal(bucket_id, args)
    return vshard.router.callbro(bucket_id, 'echo', args, opts_normal)
end

function select_return_raw(raw)
    raw = raw:iterator()
    raw:decode_array_header()
    return vshard.router.callbro(raw:decode(), 'select_some', empty_args,
                                 opts_return_raw)
end

function select_normal(bucket_id)
    return vshard.router.callbro(bucket_id, 'select_some', empty_args,
                                 opts_normal)
end

box.schema.func.create('echo_return_raw', {takes_raw_args = true,
                       if_not_exists = true})
box.schema.func.create('select_return_raw', {takes_raw_args = true,
                       if_not_exists = true})

require('console').start()
