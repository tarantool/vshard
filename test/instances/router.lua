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
_G.ivconst = require('vshard.consts')
_G.iverror = require('vshard.error')
_G.iwait_timeout = _G.ivtest.wait_timeout
_G.iyaml = require('yaml')

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

local function failover_wakeup(router)
    router = router or _G.ivshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.worker:wakeup_service('replicaset_failover')
        for _, r in pairs(rs.replicas) do
            r.worker:wakeup_service('replica_failover')
        end
    end
end

local function failover_pause(router)
    router = router or _G.ivshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY = true
        for _, r in pairs(rs.replicas) do
            r.errinj.ERRINJ_REPLICA_FAILOVER_DELAY = true
        end
    end
    -- Wait for stop.
    _G.ilt.helpers.retrying({timeout = _G.iwait_timeout}, function()
        failover_wakeup(router)
        for _, rs in pairs(replicasets) do
            _G.ilt.assert_equals(
                rs.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY, 'in')
            for _, r in pairs(rs.replicas) do
                _G.ilt.assert_equals(
                    r.errinj.ERRINJ_REPLICA_FAILOVER_DELAY, 'in')
            end
        end
    end)
end

local function failover_continue(router)
    router = router or _G.ivshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY = false
        for _, r in pairs(rs.replicas) do
            r.errinj.ERRINJ_REPLICA_FAILOVER_DELAY = false
        end
    end
end

local function master_search_pause(router)
    router = router or _G.ivshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.errinj.ERRINJ_MASTER_SEARCH_DELAY = true
    end
    -- Wait for stop.
    _G.ilt.helpers.retrying({timeout = _G.iwait_timeout}, function()
        _G.ivshard.router.master_search_wakeup()
        for _, rs in pairs(replicasets) do
            _G.ilt.assert_equals(rs.errinj.ERRINJ_MASTER_SEARCH_DELAY, 'in')
        end
    end)
end

local function master_search_continue(router)
    router = router or _G.ivshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.errinj.ERRINJ_MASTER_SEARCH_DELAY = false
    end
end

_G.failover_wakeup = failover_wakeup
_G.failover_pause = failover_pause
_G.failover_continue = failover_continue
_G.master_search_pause = master_search_pause
_G.master_search_continue = master_search_continue

_G.ready = true
