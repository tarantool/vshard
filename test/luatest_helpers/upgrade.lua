--
-- Helpers for tests that switch between vshard versions.
--
local os = require('os')
local fio = require('fio')

local function vshard_lua_path(path)
    return string.format('%s/?.lua;%s/?/init.lua;', path, path) ..
           (os.getenv('LUA_PATH') or '')
end

local function vshard_copy_new(sourcedir, copy_path)
    local copy_path_load = fio.dirname(copy_path)
    assert(fio.mkdir(copy_path_load))
    assert(fio.mkdir(copy_path))
    assert(fio.mkdir(copy_path .. '/.git'))
    assert(fio.copytree(sourcedir .. '/.git', copy_path .. '/.git'))
    return vshard_lua_path(copy_path_load)
end

return {
    vshard_copy_new = vshard_copy_new,
    vshard_lua_path = vshard_lua_path,
}
