-- vshard.util
local log = require('log')
local fiber = require('fiber')
local lerror = require('vshard.error')

local MODULE_INTERNALS = '__module_vshard_util'
local M = rawget(_G, MODULE_INTERNALS)
if not M then
    --
    -- The module is loaded for the first time.
    --
    M = {
        -- Latest versions of functions.
        reloadable_fiber_main_loop = nil,
    }
    rawset(_G, MODULE_INTERNALS, M)
end

local version_str = _TARANTOOL:sub(1, _TARANTOOL:find('-')-1)
local dot = version_str:find('%.')
local major = tonumber(version_str:sub(1, dot - 1))
version_str = version_str:sub(dot + 1)
dot = version_str:find('%.')
local middle = tonumber(version_str:sub(1, dot - 1))
local minor = tonumber(version_str:sub(dot + 1))

--
-- Extract parts of a tuple.
-- @param tuple Tuple to extract a key from.
-- @param parts Array of index parts. Each part must contain
--        'fieldno' attribute.
--
-- @retval Extracted key.
--
local function tuple_extract_key(tuple, parts)
    local key = {}
    for _, part in ipairs(parts) do
        table.insert(key, tuple[part.fieldno])
    end
    return key
end

--
-- Wrapper to run a func in infinite loop and restart it on
-- errors and module reload.
-- This loop executes the latest version of itself in case of
-- reload of that module.
-- See description of parameters in `reloadable_fiber_create`.
--
local function reloadable_fiber_main_loop(module, func_name, data)
    log.info('%s has been started', func_name)
    local func = module[func_name]
::restart_loop::
    local ok, err = pcall(func, data)
    -- yield serves two purposes:
    --  * makes this fiber cancellable
    --  * prevents 100% cpu consumption
    fiber.yield()
    if not ok then
        log.error('%s has been failed: %s', func_name, err)
        if func == module[func_name] then
            goto restart_loop
        end
        -- There is a chance that error was raised during reload
        -- (or caused by reload). Perform reload in case function
        -- has been changed.
        log.error('reloadable function %s has been changed', func_name)
    end
    log.info('module is reloaded, restarting')
    -- luajit drops this frame if next function is called in
    -- return statement.
    return M.reloadable_fiber_main_loop(module, func_name, data)
end

--
-- Create a new fiber which runs a function in a loop. This loop
-- is aware of reload mechanism and it loads a new version of the
-- function in that case.
-- To handle module reload and run new version of a function
-- in the module, the function should just return.
-- @param fiber_name Name of a new fiber. E.g.
--        "vshard.rebalancer".
-- @param module Module which can be reloaded.
-- @param func_name Name of a function to be executed in the
--        module.
-- @param data Data to be passed to the specified function.
-- @retval New fiber.
--
local function reloadable_fiber_create(fiber_name, module, func_name, data)
    assert(type(fiber_name) == 'string')
    local xfiber = fiber.create(reloadable_fiber_main_loop, module, func_name,
                                data)
    xfiber:name(fiber_name, {truncate = true})
    return xfiber
end

--
-- Wrap a given function to check that self argument is passed.
-- New function returns an error in case one called a method
-- like object.func instead of object:func.
-- Returning wrapped function to a user and using raw functions
-- inside of a module improves speed.
-- This function can be used only in case the second argument is
-- not of the "table" type or has different metatable.
-- @param obj_name Name of the called object. Used only for error
--        message construction.
-- @param func_name Name of the called function. Used only for an
--        error message construction.
-- @param mt Meta table of self argument.
-- @param func A function which is about to be wrapped.
--
-- @retval Wrapped function.
--
local function generate_self_checker(obj_name, func_name, mt, func)
    return function (self, ...)
        if getmetatable(self) ~= mt then
            local fmt = 'Use %s:%s(...) instead of %s.%s(...)'
            error(string.format(fmt, obj_name, func_name, obj_name, func_name))
        end
        return func(self, ...)
    end
end

-- Update latest versions of function
M.reloadable_fiber_main_loop = reloadable_fiber_main_loop

local function sync_task(delay, task, ...)
    if delay then
        fiber.sleep(delay)
    end
    task(...)
end

--
-- Run a function without interrupting current fiber.
-- @param delay Delay in seconds before the task should be
--        executed.
-- @param task Function to be executed.
-- @param ... Arguments which would be passed to the `task`.
--
local function async_task(delay, task, ...)
    assert(delay == nil or type(delay) == 'number')
    fiber.create(sync_task, delay, task, ...)
end

--
-- Check if Tarantool version is >= that a specified one.
--
local function version_is_at_least(major_need, middle_need, minor_need)
    if major > major_need then return true end
    if major < major_need then return false end
    if middle > middle_need then return true end
    if middle < middle_need then return false end
    return minor >= minor_need
end

--
-- Copy @a src table. Fiber yields every @a interval keys copied. Does not give
-- any guarantees on what is the result when the source table is changed during
-- yield.
--
local function table_copy_yield(src, interval)
    local res = {}
    -- Time-To-Yield.
    local tty = interval
    for k, v in pairs(src) do
        res[k] = v
        tty = tty - 1
        if tty <= 0 then
            fiber.yield()
            tty = interval
        end
    end
    return res
end

--
-- Remove @a src keys from @a dst if their values match. Fiber yields every
-- @a interval iterations. Does not give any guarantees on what is the result
-- when the source table is changed during yield.
--
local function table_minus_yield(dst, src, interval)
    -- Time-To-Yield.
    local tty = interval
    for k, srcv in pairs(src) do
        if dst[k] == srcv then
            dst[k] = nil
        end
        tty = tty - 1
        if tty <= 0 then
            fiber.yield()
            tty = interval
        end
    end
    return dst
end

local function fiber_cond_wait_xc(cond, timeout)
    -- Handle negative timeout specifically - otherwise wait() will throw an
    -- ugly usage error.
    -- Don't trust this check to the caller's code, because often it just calls
    -- wait many times until it fails or the condition is met. Code looks much
    -- cleaner when it does not need to check the timeout sign. On the other
    -- hand, perf is not important here - anyway wait() yields which is slow on
    -- its own, but also breaks JIT trace recording which makes pcall() in the
    -- non-xc version of this function inconsiderable.
    if timeout < 0 or not cond:wait(timeout) then
        -- Don't use the original error if cond sets it. Because it sets
        -- TimedOut error. It does not have a proper error code, and may not be
        -- detected by router as a special timeout case if necessary. Or at
        -- least would complicate the handling in future. Instead, try to use a
        -- unified timeout error where possible.
        error(lerror.timeout())
    end
    -- Still possible though that the fiber is canceled and cond:wait() throws.
    -- This is why the _xc() version of this function throws even the timeout -
    -- anyway pcall() is inevitable.
end

--
-- Exception-safe cond wait with unified errors in vshard format.
--
local function fiber_cond_wait(cond, timeout)
    local ok, err = pcall(fiber_cond_wait_xc, cond, timeout)
    if ok then
        return true
    end
    return nil, lerror.make(err)
end

--
-- Exception-safe way to check if the current fiber is canceled.
--
local function fiber_is_self_canceled()
    return not pcall(fiber.testcancel)
end

return {
    tuple_extract_key = tuple_extract_key,
    reloadable_fiber_create = reloadable_fiber_create,
    generate_self_checker = generate_self_checker,
    async_task = async_task,
    internal = M,
    version_is_at_least = version_is_at_least,
    table_copy_yield = table_copy_yield,
    table_minus_yield = table_minus_yield,
    fiber_cond_wait = fiber_cond_wait,
    fiber_is_self_canceled = fiber_is_self_canceled,
}
