local MODULE_INTERNALS = '__module_vshard_storage_sched'
local MODULE_VERSION = 1

local lfiber = require('fiber')
local lerror = require('vshard.error')
local lconsts = require('vshard.consts')
local lregistry = require('vshard.registry')
local clock = lfiber.clock

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        module_version = MODULE_VERSION,

        cond = lfiber.cond(),
        ref_queue = 0,
        ref_count = 0,
        ref_strike = 0,
        ref_quota = lconsts.DEFAULT_SCHED_REF_QUOTA,
        move_queue = 0,
        move_count = 0,
        move_strike = 0,
        move_quota = lconsts.DEFAULT_SCHED_MOVE_QUOTA,
    }
else
    return M
end

local function sched_wait_anything_xc(timeout)
    if timeout <= 0 then
        error(lerror.make(box.error.new(box.error.TIMEOUT)))
    end
    if not M.cond:wait(timeout) then
        error(lerror.make(box.error.last()))
    end
end

local function sched_wait_anything(timeout)
    local ok, err = pcall(sched_wait_anything_xc, timeout)
    if not ok then
        return nil, err
    end
    return true
end

local function sched_ref_start(timeout)
    local deadline = clock() + timeout
    local ok, err

    M.ref_queue = M.ref_queue + 1

::retry::

    while M.move_count > 0 do
        ok, err = sched_wait_anything(timeout)
        if not ok then
            return nil, err
        end
        timeout = deadline - clock()
    end

    if M.move_queue > 0 and M.ref_strike > M.ref_quota then
        ok, err = sched_wait_anything(timeout)
        if not ok then
            return nil, err
        end
        timeout = deadline - clock()
        goto retry
    end

    M.ref_queue = M.ref_queue - 1
    M.ref_count = M.ref_count + 1
    M.ref_strike = M.ref_strike + 1
    M.move_strike = 0
    return timeout
end

local function sched_ref_end(count)
    count = M.ref_count - count
    M.ref_count = count
    if count == 0 and M.move_queue > 0 then
        M.cond:broadcast()
    end
end

local function sched_move_start(timeout)
    local deadline = clock() + timeout
    local ok, err, ref_deadline
    local lref = lregistry.storage_ref

    M.move_queue = M.move_queue + 1

::retry::

    while M.ref_count > 0 do
        ref_deadline = lref.next_deadline()
        if ref_deadline < deadline then
            timeout = ref_deadline - clock()
        end
        ok, err = sched_wait_anything(timeout)
        timeout = deadline - clock()
        if not ok then
            lref.gc()
            timeout = deadline - clock()
            if timeout < 0 then
                return nil, err
            end
        end
    end

    if M.ref_queue > 0 and M.move_strike > M.move_quota then
        ok, err = sched_wait_anything(timeout)
        if not ok then
            return nil, err
        end
        timeout = deadline - clock()
        goto retry
    end

    M.move_queue = M.move_queue - 1
    M.move_count = M.move_count + 1
    M.move_strike = M.move_strike + 1
    M.ref_strike = 0
    return timeout
end

local function sched_move_end()
    local count = M.move_count - 1
    M.move_count = count
    if count == 0 and M.ref_queue > 0 then
        M.cond:broadcast()
    end
end

local function sched_cfg(cfg)
    local new_ref_quota = cfg.ref_quota
    local new_move_quota = cfg.move_quota

    if new_ref_quota > M.ref_quota then
        M.cond:broadcast()
    end
    M.ref_quota = new_ref_quota

    if new_move_quota > M.move_quota then
        M.cond:broadcast()
    end
    M.move_quota = new_move_quota
end

M.ref_start = sched_ref_start
M.ref_end = sched_ref_end
M.move_start = sched_move_start
M.move_end = sched_move_end
lregistry.storage_sched = M

return M
