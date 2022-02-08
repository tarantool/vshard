#!/usr/bin/env tarantool
local helpers = require('test.luatest_helpers')
_G.msgpack = require('msgpack')
-- Do not load entire vshard into the global namespace to catch errors when code
-- relies on that.
_G.vshard = {
    router = require('vshard.router'),
}
-- Somewhy shutdown hangs on new Tarantools even though the nodes do not seem to
-- have any long requests running.
if box.ctl.set_on_shutdown_timeout then
    box.ctl.set_on_shutdown_timeout(0.001)
end

box.cfg(helpers.box_cfg())
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

_G.ready = true
