-- vshard.util
local log = require('log')
local fiber = require('fiber')

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
    xfiber:name(fiber_name)
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

return {
    tuple_extract_key = tuple_extract_key,
    reloadable_fiber_create = reloadable_fiber_create,
    generate_self_checker = generate_self_checker,
    async_task = async_task,
    internal = M,
    version_is_at_least = version_is_at_least,
}
