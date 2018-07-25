--
-- This module implements background lua GC fiber.
-- It's purpose is to make GC more aggressive.
--

local lfiber = require('fiber')
local MODULE_INTERNALS = '__module_vshard_lua_gc'

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        -- Background fiber.
        bg_fiber = nil,
        -- GC interval in seconds.
        interval = nil,
        -- Main loop.
        -- Stored here to make the fiber reloadable.
        main_loop = nil,
        -- Number of `collectgarbage()` calls.
        iterations = 0,
    }
end

M.main_loop = function()
    lfiber.sleep(M.interval)
    collectgarbage()
    M.iterations = M.iterations + 1
    return M.main_loop()
end

local function set_state(active, interval)
    assert(type(interval) == 'number')
    M.interval = interval
    if active and not M.bg_fiber then
        M.bg_fiber = lfiber.create(M.main_loop)
        M.bg_fiber:name('vshard.lua_gc')
    end
    if not active and M.bg_fiber then
        M.bg_fiber:cancel()
        M.bg_fiber = nil
    end
    if active then
        M.bg_fiber:wakeup()
    end
end

if not rawget(_G, MODULE_INTERNALS) then
    rawset(_G, MODULE_INTERNALS, M)
end

return {
    set_state = set_state,
    internal = M,
}
