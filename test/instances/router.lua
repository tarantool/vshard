#!/usr/bin/env tarantool
local helpers = require('test.luatest_helpers')

--
-- Commonly used libraries. Use 'i' prefix as 'instance'. The purpose is to be
-- able to use the libs in server:exec() calls and not get upvalue errors if the
-- same lib is declared in the _test.lua file.
--
_G.ifiber = require('fiber')
_G.ilt = require('luatest')
_G.imsgpack = require('msgpack')
_G.ivtest = require('test.luatest_helpers.vtest')
_G.iwait_timeout = _G.ivtest.wait_timeout

-- Do not load entire vshard into the global namespace to catch errors when code
-- relies on that.
_G.vshard = {
    router = require('vshard.router'),
}
_G.ivshard = _G.vshard

-- Somewhy shutdown hangs on new Tarantools even though the nodes do not seem to
-- have any long requests running.
if box.ctl.set_on_shutdown_timeout then
    box.ctl.set_on_shutdown_timeout(0.001)
end

box.cfg(helpers.box_cfg())
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

_G.ready = true
