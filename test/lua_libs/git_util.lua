--
-- Lua bridge for some of the git commands.
--
local os = require('os')

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

return {
    exec = exec,
    log_hashes = log_hashes
}
