#!/usr/bin/env tarantool
local fio = require('fio')
local root = fio.abspath(fio.pathjoin(fio.dirname(arg[0]), '..'))
package.path = root .. '/?.lua;' .. root .. '/?/init.lua;' .. package.path

local argparse = require('internal.argparse')
local datetime = require('datetime')
local util = require('perf.lib.util')

local opts = argparse.parse(arg, {
    {'input-dir', 'string'},
    {'output', 'string'},
})
assert(#opts == 0, 'Unexpected positional arguments')
local input_dir = opts['input-dir']
assert(input_dir and fio.path.is_dir(input_dir), 'Invalid --input-dir')
assert(opts.output, 'Missing --output')

local function escape_tag(value)
    return (value:gsub('[ ,=]', '\\%1'))
end

local function quote_field(value)
    return '"' .. value:gsub('[\\"]', '\\%1') .. '"'
end

local fmt = 'vshard,operation=%s,%s items_per_second=%.17g,%s %.0f\n'
local lines = {}
local scenarios = assert(fio.listdir(input_dir))
table.sort(scenarios)
for _, scenario in ipairs(scenarios) do
    local path = fio.pathjoin(input_dir, scenario, 'results.json')
    if not fio.path.is_file(path) then
        goto continue
    end
    local report = util.read_json(path)
    if report.status ~= 'complete' then
        goto continue
    end
    local context = report.context
    local date = datetime.parse(context.date)
    local tags = ('configuration=%s,branch=%s'):format(
        escape_tag(scenario), escape_tag(context.vshard_branch))
    local fields = ('commit=%s,tarantool_version=%s'):format(
        quote_field(context.vshard_commit),
        quote_field(context.tarantool_version))
    for _, bench in ipairs(report.benchmarks) do
        local line = fmt:format(escape_tag(bench.name), tags,
                               bench.items_per_second, fields, date.epoch)
        table.insert(lines, line)
    end
    ::continue::
end

local output = assert(io.open(opts.output, 'wb'))
assert(output:write(table.concat(lines)))
assert(output:close())
