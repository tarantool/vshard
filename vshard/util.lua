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
        reloadable_fiber_f = nil,
    }
    rawset(_G, MODULE_INTERNALS, M)
end

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
-- To handle module reload and run new version of a function
-- in the module, the function should just return.
--
-- @param module Module which can be reloaded.
-- @param func_name Name of a function to be executed in the
--        module.
-- @param worker_name Name of the reloadable background subsystem.
--        For example: "Garbage Collector", "Recovery", "Discovery",
--        "Rebalancer". Used only for an activity logging.
--
local function reloadable_fiber_f(module, func_name, worker_name)
    log.info('%s has been started', worker_name)
    local func = module[func_name]
::reload_loop::
    local ok, err = pcall(func, module.module_version)
    -- yield serves two pursoses:
    --  * makes this fiber cancellable
    --  * prevents 100% cpu consumption
    fiber.yield()
    if not ok then
        log.error('%s has been failed: %s', worker_name, err)
        if func == module[func_name] then
            goto reload_loop
        end
        -- There is a chance that error was raised during reload
        -- (or caused by reload). Perform reload in case function
        -- has been changed.
        log.error('%s: reloadable function %s has been changed',
                  worker_name, func_name)
    end
    log.info('%s is reloaded, restarting', worker_name)
    -- luajit drops this frame if next function is called in
    -- return statement.
    return M.reloadable_fiber_f(module, func_name, worker_name)
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
M.reloadable_fiber_f = reloadable_fiber_f

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

return {
    tuple_extract_key = tuple_extract_key,
    reloadable_fiber_f = reloadable_fiber_f,
    generate_self_checker = generate_self_checker,
    async_task = async_task,
    internal = M,
}
