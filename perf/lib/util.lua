local json = require('json')
local popen = require('popen')

local function read_json(path)
    local file = assert(io.open(path, 'rb'))
    local result = file:read('*a')
    file:close()
    return json.decode(result)
end

local function write_json(path, value)
    local contents = json.encode(value) .. '\n'
    local file = assert(io.open(path, 'wb'))
    assert(file:write(contents))
    assert(file:close())
end

local function capture(command)
    local process = assert(popen.shell(command, 'r'))
    local chunks = {}
    repeat
        local chunk = process:read()
        if not chunk then
            process:close()
            return nil
        end
        table.insert(chunks, chunk)
    until chunk == ''
    local status = process:wait()
    process:close()
    if not status or status.exit_code ~= 0 then
        return nil
    end
    local output = table.concat(chunks):gsub('%s+$', '')
    return output
end

local function print_log(path)
    local file = io.open(path, 'r')
    if not file then
        return
    end
    local lines = {}
    for line in file:lines() do
        table.insert(lines, line)
        if #lines > 20 then
            table.remove(lines, 1)
        end
    end
    file:close()
    io.stderr:write(path .. ':\n' .. table.concat(lines, '\n') .. '\n')
end

return {
    read_json = read_json,
    write_json = write_json,
    capture = capture,
    print_log = print_log,
}
