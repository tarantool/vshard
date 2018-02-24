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

return {
    tuple_extract_key = tuple_extract_key,
    reloadable_fiber_f = reloadable_fiber_f,
}
