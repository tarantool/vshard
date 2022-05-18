#!/usr/bin/env tarantool
local helpers = require('test.luatest_helpers')

--
-- Commonly used libraries. Use 'i' prefix as 'instance'. The purpose is to be
-- able to use the libs in server:exec() calls and not get upvalue errors if the
-- same lib is declared in the _test.lua file.
--
_G.ifiber = require('fiber')
_G.ilt = require('luatest')
_G.ivconst = require('vshard.consts')

-- Do not load entire vshard into the global namespace to catch errors when code
-- relies on that.
_G.vshard = {
    storage = require('vshard.storage'),
}
_G.ivshard = _G.vshard

-- Get rid of luacheck warnings that _G members != variables.
local vshard = _G.ivshard
local vconst = _G.ivconst
local t = _G.ilt

-- Somewhy shutdown hangs on new Tarantools even though the nodes do not seem to
-- have any long requests running.
if box.ctl.set_on_shutdown_timeout then
    box.ctl.set_on_shutdown_timeout(0.001)
end

box.cfg(helpers.box_cfg())
local instance_uuid = box.info.uuid
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

local function box_error()
    box.error(box.error.PROC_LUA, 'box_error')
end

local function echo(...)
    return ...
end

local function get_uuid()
    return instance_uuid
end

local function get_first_bucket()
    local res = box.space._bucket.index.status:min(vconst.BUCKET.ACTIVE)
    return res ~= nil and res.id or nil
end

local function session_set(key, value)
    box.session.storage[key] = value
    return true
end

local function session_get(key)
    return box.session.storage[key]
end

local function wait_bucket_gc(timeout)
    local status_index = box.space._bucket.index.status
    t.helpers.retrying({timeout = timeout}, function()
        vshard.storage.garbage_collector_wakeup()
        if status_index:min({vconst.BUCKET.SENT}) ~= nil then
            error('Still have SENT buckets')
        end
        if status_index:min({vconst.BUCKET.GARBAGE}) ~= nil then
            error('Still have GARBAGE buckets')
        end
    end)
end

_G.box_error = box_error
_G.echo = echo
_G.get_uuid = get_uuid
_G.get_first_bucket = get_first_bucket
_G.session_set = session_set
_G.session_get = session_get
_G.wait_bucket_gc = wait_bucket_gc

_G.ready = true
