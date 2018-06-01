-- vshard.util
local log = require('log')
local fiber = require('fiber')

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
-- Wrapper to run @a func in infinite loop and restart it on the
-- module reload. This function CAN NOT BE AUTORELOADED. To update
-- it you must manualy stop all fibers, run by this function, do
-- reload, and then restart all stopped fibers. This can be done,
-- for example, by calling vshard.storage/router.cfg() again with
-- the same config as earlier.
--
-- @param func Reloadable function to run. It must accept current
--        module version as an argument, and interrupt itself,
--        when it is changed.
-- @param worker_name Name of the function. Usual infinite fiber
--        represents a background subsystem, which has a name. For
--        example: "Garbage Collector", "Recovery", "Discovery",
--        "Rebalancer".
-- @param M Module which can reload.
--
local function reloadable_fiber_f(M, func_name, worker_name)
    while true do
        local ok, err = pcall(M[func_name], M.module_version)
        if not ok then
            log.error('%s has been failed: %s', worker_name, err)
            fiber.yield()
        else
            log.info('%s has been reloaded', worker_name)
            fiber.yield()
        end
    end
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

return {
    tuple_extract_key = tuple_extract_key,
    reloadable_fiber_f = reloadable_fiber_f,
    generate_self_checker = generate_self_checker,
}
