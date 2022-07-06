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
_G.ivconst = require('vshard.consts')
_G.ivutil = require('vshard.util')
_G.iverror = require('vshard.error')
_G.ivtest = require('test.luatest_helpers.vtest')

_G.iwait_timeout = _G.ivtest.wait_timeout

-- Do not load entire vshard into the global namespace to catch errors when code
-- relies on that.
_G.vshard = {
    storage = require('vshard.storage'),
}
_G.ivshard = _G.vshard

-- Get rid of luacheck warnings that _G members != variables.
local t = _G.ilt
local vconst = _G.ivconst
local vshard = _G.ivshard
local vutil = _G.ivutil
local wait_timeout = _G.iwait_timeout

local index_has = vutil.index_has
local index_min = vutil.index_min

-- Somewhy shutdown hangs on new Tarantools even though the nodes do not seem to
-- have any long requests running.
if box.ctl.set_on_shutdown_timeout then
    box.ctl.set_on_shutdown_timeout(0.001)
end

box.cfg(helpers.box_cfg())
local instance_uuid = box.info.uuid
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
local local_meta = box.schema.create_space('ilocal_meta', {
    is_local = true,
    if_not_exists = true,
    format = {
        {'key', 'string'},
        {'value', 'any', is_nullable = true}
    }
})
local_meta:create_index('pk', {if_not_exists = true})

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
    local res = index_min(box.space._bucket.index.status, vconst.BUCKET.ACTIVE)
    return res ~= nil and res.id or nil
end

local function session_set(key, value)
    box.session.storage[key] = value
    return true
end

local function session_get(key)
    return box.session.storage[key]
end

local function bucket_gc_wait()
    local status_index = box.space._bucket.index.status
    t.helpers.retrying({timeout = wait_timeout}, function()
        vshard.storage.garbage_collector_wakeup()
        if index_has(status_index, vconst.BUCKET.SENT) then
            error('Still have SENT buckets')
        end
        if index_has(status_index, vconst.BUCKET.GARBAGE) then
            error('Still have GARBAGE buckets')
        end
    end)
end

local function bucket_gc_pause()
    local errinj = vshard.storage.internal.errinj
    errinj.ERRINJ_BUCKET_GC_PAUSE = true
    t.helpers.retrying({timeout = wait_timeout}, function()
        if errinj.ERRINJ_BUCKET_GC_PAUSE == 1 then
            return
        end
        vshard.storage.garbage_collector_wakeup()
        error('Bucket GC is still not paused')
    end)
end

local function bucket_gc_continue()
    vshard.storage.internal.errinj.ERRINJ_BUCKET_GC_PAUSE = false
    vshard.storage.garbage_collector_wakeup()
end

local function bucket_recovery_pause()
    local errinj = vshard.storage.internal.errinj
    errinj.ERRINJ_RECOVERY_PAUSE = true
    t.helpers.retrying({timeout = wait_timeout}, function()
        if errinj.ERRINJ_RECOVERY_PAUSE == 1 then
            return
        end
        vshard.storage.recovery_wakeup()
        error('Bucket recovery is still not paused')
    end)
end

local function bucket_recovery_continue()
    vshard.storage.internal.errinj.ERRINJ_RECOVERY_PAUSE = false
    vshard.storage.garbage_collector_wakeup()
end

local function wal_sync()
    -- Any WAL write guarantees that all the previous WAL writes are finished.
    -- Local space is used so as to work on replicas too.
    local_meta:replace{'wal_sync'}
end

_G.box_error = box_error
_G.echo = echo
_G.get_uuid = get_uuid
_G.get_first_bucket = get_first_bucket
_G.session_set = session_set
_G.session_get = session_get
_G.bucket_gc_wait = bucket_gc_wait
_G.bucket_gc_pause = bucket_gc_pause
_G.bucket_gc_continue = bucket_gc_continue
_G.bucket_recovery_pause = bucket_recovery_pause
_G.bucket_recovery_continue = bucket_recovery_continue
_G.wal_sync = wal_sync

_G.ready = true
