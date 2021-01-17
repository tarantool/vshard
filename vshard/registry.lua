local MODULE_INTERNALS = '__module_vshard_registry'

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {}
    rawset(_G, MODULE_INTERNALS, M)
end

return M
