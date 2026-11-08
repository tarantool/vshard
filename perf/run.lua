#!/usr/bin/env tarantool
io.stdout:setvbuf('line')

local fio = require('fio')
local uuid = require('uuid')
local root = fio.abspath(fio.pathjoin(fio.dirname(arg[0]), '..'))
package.path = root .. '/?.lua;' .. root .. '/?/init.lua;' .. package.path

local cluster = require('perf.lib.cluster')
local config = require('perf.lib.config')
local report = require('perf.lib.report')
local util = require('perf.lib.util')

local help = [[
Usage: tarantool perf/run.lua [options]
  --replicasets N               Number of storage replica sets
  --instances-per-replicaset N  Storage instances including the master
  --routers N                   Separate router processes
  --bucket-count N              Total number of buckets
  --generators N                Separate load generator processes
  --payload-bytes N             Payload size for get/replace
  --output-dir DIR              New or empty output directory
  --diagnostics                 Save process IDs and measurement windows
  --help                        Show this help
]]

local current, output, result
local function save()
    util.write_json(fio.pathjoin(output, 'results.json'), result)
end

box.ctl.on_shutdown(function()
    if current then
        current:stop()
        current = nil
        result.status, result.failure = 'failed', 'Interrupted'
        save()
    end
end)

local function main()
    local opts, cfg = config.parse(arg)
    if opts.help then
        print(help)
        return
    end
    local binary = fio.readlink('/proc/self/exe') or fio.abspath(arg[-1])
    local suffix = os.date('!%Y%m%dT%H%M%S') .. '-' .. uuid.str()
    local directory = opts['output-dir'] and fio.abspath(opts['output-dir']) or
                      fio.pathjoin(root, 'perf/output', suffix)
    assert(not fio.path.exists(directory) or
           (fio.path.is_dir(directory) and #fio.listdir(directory) == 0),
           'Output directory must be new or empty: ' .. directory)
    assert(fio.mktree(directory))
    output = directory
    result = report.new(cfg, root)
    print('Results: ' .. output)
    save()
    for _, case in ipairs(cfg.cases) do
        local directory = fio.pathjoin(output, case)
        current = cluster.new(cfg, root, binary, directory)
        current:start()
        local warmup = current:measure(case, cfg.warmup_seconds)
        for name, generator in pairs(warmup) do
            assert(generator.errors == 0,
                   'Warmup failed on ' .. name .. ': ' ..
                   tostring(generator.error))
        end
        current:sync()
        local generators = current:measure(case, cfg.duration_seconds,
                                           opts.diagnostics)
        local row = report.add(result, case, generators)
        save()
        assert(row.errors == 0, row.error)
        for name, generator in pairs(generators) do
            assert(generator.iterations > 0,
                   name .. ': no operations completed in the window')
        end
        current:sync()
        current:stop()
        current:cleanup()
        current = nil
        print(('%s: %.0f RPS'):format(case, row.items_per_second))
    end
    result.status = 'complete'
    save()
end

local ok, err = xpcall(main, function(value)
    return debug.traceback(tostring(value), 2)
end)
if not ok then
    if current then
        current:stop()
        current = nil
    end
    if result and output and fio.path.is_dir(output) then
        result.status, result.failure = 'failed', err
        save()
    end
    io.stderr:write(err .. '\n')
end
os.exit(ok and 0 or 1)
