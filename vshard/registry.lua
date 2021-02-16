--
-- Registry is a way to resolve cyclic dependencies which normally can exist
-- between files of the same module/library.
--
-- Files, which want to expose their API to the other files, which in turn can't
-- require the formers directly, should put their API to the registry.
--
-- The files should use the registry to get API of the other files. They don't
-- require() and use the latter directly if there is a known loop dependency
-- between them.
--
-- At runtime, when all require() are done, the registry is full, and all the
-- files see API of each other.
--
-- Having the modules accessed via the registry adds at lest +1 indexing
-- operation at runtime when need to get a function from there. But sometimes it
-- can be cached to reduce the effect in perf-sensitive code. For example, like
-- this:
--
--     local lreg = require('vshard.registry')
--
--     local storage_func
--
--     local function storage_func_no_cache(...)
--         storage_func = lreg.storage.func
--         return storage_func(...)
--     end
--
--     storage_func = storage_func_no_cache
--
-- The code will always call storage_func(), but will load it from the registry
-- only on first invocation.
--
-- However in case reload is important, it is not possible - the original
-- function object in the registry may change. In such situation still makes
-- sense to cache at least 'lreg.storage' to save 1 indexing operation.
--
--     local lreg = require('vshard.registry')
--
--     local lstorage
--     local storage_func
--
--     local function storage_func_cache(...)
--         return lstorage.storage_func(...)
--     end
--
--     local function storage_func_no_cache(...)
--         lstorage = lref.storage
--         storage_func = storage_func_cache
--         return lstorage.storage_func(...)
--     end
--
--     storage_func = storage_func_no_cache
--
-- A harder way would be to use the first approach + add triggers on reload of
-- the cached module to update the cached function refs. If the code is
-- extremely perf-critical (which should not be Lua then).
--

local MODULE_INTERNALS = '__module_vshard_registry'

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {}
    rawset(_G, MODULE_INTERNALS, M)
end

return M
