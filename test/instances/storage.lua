#!/usr/bin/env tarantool
local helpers = require('test.luatest_helpers')
-- Do not load entire vshard into the global namespace to catch errors when code
-- relies on that.
_G.vshard = {
    storage = require('vshard.storage'),
}
-- Somewhy shutdown hangs on new Tarantools even though the nodes do not seem to
-- have any long requests running.
if box.ctl.set_on_shutdown_timeout then
    box.ctl.set_on_shutdown_timeout(0.001)
end

box.cfg(helpers.box_cfg())
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

local function box_error()
    box.error(box.error.PROC_LUA, 'box_error')
end

local function echo(...)
    return ...
end

_G.box_error = box_error
_G.echo = echo

_G.ready = true
