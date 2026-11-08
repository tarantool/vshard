#!/usr/bin/env tarantool
local fio = require('fio')
local root = fio.abspath(fio.pathjoin(fio.dirname(arg[0]), '../..'))
package.path = root .. '/?.lua;' .. root .. '/?/init.lua;' .. package.path

local argparse = require('internal.argparse')
local util = require('perf.lib.util')
local opts = argparse.parse(arg, {
    {'base', 'string'},
    {'head', 'string'},
})
assert(#opts == 0, 'Unexpected positional arguments')
assert(opts.base and opts.head, 'Usage: compare.lua --base FILE --head FILE')

local function read_results(path)
    local report = util.read_json(path)
    local rows = {}
    assert(#report.benchmarks > 0, 'No benchmarks in ' .. path)
    for _, row in ipairs(report.benchmarks) do
        local rps = row.items_per_second
        assert(rps > 0 and rps < math.huge, 'Invalid RPS in ' .. path)
        assert(not rows[row.name], 'Duplicate benchmark: ' .. row.name)
        rows[row.name] = rps
    end
    return rows
end

local base, head = read_results(opts.base), read_results(opts.head)
local failed = false
print(('%-20s %12s %12s %10s'):format('Operation', 'Base RPS', 'PR RPS',
                                    'Change'))
local names = {}
for name in pairs(base) do
    table.insert(names, name)
end
table.sort(names)
for _, name in ipairs(names) do
    local before, after = base[name], head[name]
    assert(after, 'Missing benchmark in PR results: ' .. name)
    print(('%-20s %12.0f %12.0f %+9.2f%%'):format(
        name, before, after, (after / before - 1) * 100))
    if before >= 1.05 * after then
        io.stderr:write(name .. ': base RPS is at least 5% higher\n')
        failed = true
    end
    head[name] = nil
end
assert(next(head) == nil, 'Unexpected benchmarks in PR results')
os.exit(failed and 1 or 0)
