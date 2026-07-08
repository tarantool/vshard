--
-- Lua bridge for some of the git commands.
--
local os = require('os')
local fio = require('fio')

--
-- Exec a git command.
-- @param cmd Git command to run.
-- @param params Table of parameters:
--        * options - git options.
--        * args - command arguments.
--        * dir - working directory.
--        * fout - write output to the file.
local function exec(cmd, params)
    params.options = params.options or ''
    params.args = params.args or ''
    local shell_cmd = string.format('git %s %s %s', params.options, cmd,
                                    params.args)
    if params.fout then
        shell_cmd = string.format('%s >%s', shell_cmd, params.fout)
    end
    if params.dir then
        shell_cmd = string.format('cd %s && %s', params.dir, shell_cmd)
    end
    local res = os.execute(shell_cmd)
    assert(res == 0, 'Git cmd error: ' .. res)
end

local function log_hashes(params)
    params.args = "--format='%h' " .. params.args
    -- Store log to the file.
    local temp_file = os.tmpname()
    params.fout = temp_file
    exec('log', params)
    local lines = {}
    for line in io.lines(temp_file) do
        table.insert(lines, line)
    end
    os.remove(temp_file)
    return lines
end

local function vshard_lua_path(path)
    return string.format('%s/?.lua;%s/?/init.lua;', path, path) ..
           (os.getenv('LUA_PATH') or '')
end

local function vshard_copy_new(sourcedir, copy_path)
    local copy_path_load = fio.dirname(copy_path)
    assert(fio.mkdir(copy_path_load) == true)
    assert(fio.mkdir(copy_path) == true)
    assert(fio.mkdir(copy_path .. '/.git') == true)
    assert(fio.copytree(sourcedir .. '/.git', copy_path .. '/.git') == true)
    return vshard_lua_path(copy_path_load)
end

return {
    exec = exec,
    log_hashes = log_hashes,
    vshard_copy_new = vshard_copy_new,
    vshard_lua_path = vshard_lua_path,
}
