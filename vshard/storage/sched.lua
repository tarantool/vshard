--
-- Scheduler module ensures fair time sharing between incompatible operations:
-- storage refs and bucket moves.
-- Storage ref is supposed to prevent all bucket moves and provide safe
-- environment for all kinds of possible requests on entire dataset of all
-- spaces stored on the instance.
-- Bucket move, on the contrary, wants to make a part of the dataset not usable
-- temporary.
-- Without a scheduler it would be possible to always keep at least one ref on
-- the storage and block bucket moves forever. Or vice versa - during
-- rebalancing block all incoming refs for the entire time of data migration,
-- essentially making map-reduce not usable since it heavily depends on refs.
--
-- The schedule divides storage time between refs and moves so both of them can
-- execute without blocking each other. Division proportions depend on the
-- configuration settings.
--
-- Idea of non-blockage is based on quotas and strikes. Move and ref both have
-- quotas. When one op executes more than quota requests in a row (makes a
-- strike) while the other op has queued requests, the first op stops accepting
-- new requests until the other op executes.
--

local MODULE_INTERNALS = '__module_vshard_storage_sched'
-- Update when change behaviour of anything in the file, to be able to reload.
local MODULE_VERSION = 1

local lfiber = require('fiber')
local lconsts = require('vshard.consts')
local lregistry = require('vshard.registry')
local lutil = require('vshard.util')
local fiber_clock = lfiber.clock
local fiber_cond_wait = lutil.fiber_cond_wait
local fiber_is_self_canceled = lutil.fiber_is_self_canceled

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        ---------------- Common module attributes ----------------
        module_version = MODULE_VERSION,
        -- Scheduler condition is signaled every time anything significant
        -- happens - count of an operation type drops to 0, or quota increased,
        -- etc.
        cond = lfiber.cond(),

        -------------------------- Refs --------------------------
        -- Number of ref requests waiting for start.
        ref_queue = 0,
        -- Number of ref requests being executed. It is the same as ref's module
        -- counter, but is duplicated here for the sake of isolation and
        -- symmetry with moves.
        ref_count = 0,
        -- Number of ref requests executed in a row. When becomes bigger than
        -- quota, any next queued move blocks new refs.
        ref_strike = 0,
        ref_quota = lconsts.DEFAULT_SCHED_REF_QUOTA,

        ------------------------- Moves --------------------------
        -- Number of move requests waiting for start.
        move_queue = 0,
        -- Number of move requests being executed.
        move_count = 0,
        -- Number of move requests executed in a row. When becomes bigger than
        -- quota, any next queued ref blocks new moves.
        move_strike = 0,
        move_quota = lconsts.DEFAULT_SCHED_MOVE_QUOTA,
    }
else
    return M
end

local function sched_wait_anything(timeout)
    return fiber_cond_wait(M.cond, timeout)
end

--
-- Return the remaining timeout in case there was a yield. This helps to save
-- current clock get in the caller code if there were no yields.
--
local function sched_ref_start(timeout)
    local deadline, ok, err
    -- Fast-path. Moves are extremely rare. No need to inc-dec the ref queue
    -- then nor try to start some loops.
    if M.move_count == 0 and M.move_queue == 0 then
        goto success
    end
    deadline = fiber_clock() + timeout

    M.ref_queue = M.ref_queue + 1

::retry::
    if M.move_count > 0 then
        goto wait_and_retry
    end
    -- Even if move count is zero, must ensure the time usage is fair. Does not
    -- matter in case the moves have no quota at all. That allows to ignore them
    -- infinitely until all refs end voluntarily.
    if M.move_queue > 0 and M.ref_strike >= M.ref_quota and
       M.move_quota > 0 then
        goto wait_and_retry
    end

    M.ref_queue = M.ref_queue - 1

::success::
    M.ref_count = M.ref_count + 1
    M.ref_strike = M.ref_strike + 1
    M.move_strike = 0
    do return timeout end

::wait_and_retry::
    ok, err = sched_wait_anything(timeout)
    if not ok then
        M.ref_queue = M.ref_queue - 1
        return nil, err
    end
    timeout = deadline - fiber_clock()
    goto retry
end

local function sched_ref_end(count)
    count = M.ref_count - count
    M.ref_count = count
    if count == 0 and M.move_queue > 0 then
        M.cond:broadcast()
    end
end

--
-- Return the remaining timeout in case there was a yield. This helps to save
-- current clock get in the caller code if there were no yields.
--
local function sched_move_start(timeout)
    local ok, err, deadline, ref_deadline
    local lref = lregistry.storage_ref
    -- Fast-path. Refs are not extremely rare *when used*. But they are not
    -- expected to be used in a lot of installations. So most of the times the
    -- moves should work right away.
    if M.ref_count == 0 and M.ref_queue == 0 then
        goto success
    end
    deadline = fiber_clock() + timeout

    M.move_queue = M.move_queue + 1

::retry::
    if M.ref_count > 0 then
        ref_deadline = lref.next_deadline()
        if ref_deadline < deadline then
            timeout = ref_deadline - fiber_clock()
        end
        ok, err = sched_wait_anything(timeout)
        timeout = deadline - fiber_clock()
        if ok then
            goto retry
        end
        if fiber_is_self_canceled() then
            goto fail
        end
        -- Even if the timeout has expired already (or was 0 from the
        -- beginning), it is still possible the move can be started if all the
        -- present refs are expired too and can be collected.
        lref.gc()
        -- GC could yield - need to refetch the clock again.
        timeout = deadline - fiber_clock()
        if M.ref_count > 0 then
            if timeout < 0 then
                goto fail
            end
            goto retry
        end
    end

    if M.ref_queue > 0 and M.move_strike >= M.move_quota and
       M.ref_quota > 0 then
        ok, err = sched_wait_anything(timeout)
        if not ok then
            goto fail
        end
        timeout = deadline - fiber_clock()
        goto retry
    end

    M.move_queue = M.move_queue - 1

::success::
    M.move_count = M.move_count + 1
    M.move_strike = M.move_strike + 1
    M.ref_strike = 0
    do return timeout end

::fail::
    M.move_queue = M.move_queue - 1
    return nil, err
end

local function sched_move_end(count)
    count = M.move_count - count
    M.move_count = count
    if count == 0 and M.ref_queue > 0 then
        M.cond:broadcast()
    end
end

local function sched_cfg(cfg)
    local new_ref_quota = cfg.sched_ref_quota
    local new_move_quota = cfg.sched_move_quota

    if new_ref_quota then
        if new_ref_quota > M.ref_quota then
            M.cond:broadcast()
        end
        M.ref_quota = new_ref_quota
    end
    if new_move_quota then
        if new_move_quota > M.move_quota then
            M.cond:broadcast()
        end
        M.move_quota = new_move_quota
    end
end

M.ref_start = sched_ref_start
M.ref_end = sched_ref_end
M.move_start = sched_move_start
M.move_end = sched_move_end
M.cfg = sched_cfg
lregistry.storage_sched = M

return M
