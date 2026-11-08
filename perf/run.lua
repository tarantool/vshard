#!/usr/bin/env tarantool
io.stdout:setvbuf('line')

local argparse = require('internal.argparse')
local clock = require('clock')
local fio = require('fio')
local log = require('log')
local uuid = require('uuid')
local root = fio.abspath(fio.pathjoin(fio.dirname(arg[0]), '..'))
package.path = root .. '/?.lua;' .. root .. '/?/init.lua;' .. package.path

local cluster = require('perf.lib.cluster')
local consts = require('vshard.consts')
local report = require('perf.lib.report')
local util = require('perf.lib.util')

local defaults = {
    replicasets = 2,
    instances_per_replicaset = 1,
    routers = 1,
    bucket_count = consts.DEFAULT_BUCKET_COUNT,
    generators = 1,
    payload_bytes = 128,
    repetitions = 1,
}

local help = [[
Usage: tarantool perf/run.lua [options]
  --replicasets N               Number of storage replica sets (default: %d)
  --instances-per-replicaset N  Instances including the master (default: %d)
  --routers N                   Separate router processes (default: %d)
  --bucket-count N              Total number of buckets (default: %d)
  --generators N                Separate load generator processes (default: %d)
  --payload-bytes N             Payload size for get/replace (default: %d)
  --repetitions N               Measurements per operation (default: %d)
  --no-warmup                   Disable warmup (enabled by default)
  --output FILE                 JSON output file (default: perf/output/...)
  --help                        Show this help
]]

local function parse_args(args)
    local opts = argparse.parse(args, {
        {'replicasets', 'number'},
        {'instances-per-replicaset', 'number'},
        {'routers', 'number'},
        {'bucket-count', 'number'},
        {'generators', 'number'},
        {'payload-bytes', 'number'},
        {'repetitions', 'number'},
        {'no-warmup', 'boolean'},
        {'output', 'string'},
        {'help', 'boolean'},
    })
    assert(#opts == 0, 'Unexpected positional arguments')
    local cfg = {
        replicasets = opts.replicasets or defaults.replicasets,
        instances_per_replicaset = opts['instances-per-replicaset'] or
                                  defaults.instances_per_replicaset,
        routers = opts.routers or defaults.routers,
        bucket_count = opts['bucket-count'] or defaults.bucket_count,
        generators = opts.generators or defaults.generators,
        payload_bytes = opts['payload-bytes'] or defaults.payload_bytes,
        repetitions = opts.repetitions or defaults.repetitions,
        fibers_per_generator = 256,
        cases = {'callro_get', 'callrw_replace'},
        rows = 100000,
        seed = 1,
        warmup = not opts['no-warmup'],
        duration_seconds = 20,
        base_port = 13300,
        startup_timeout_seconds = 60,
        rpc_timeout_seconds = 5,
    }
    for key in pairs(defaults) do
        local value = cfg[key]
        assert(value >= 0 and value < math.huge and value == math.floor(value),
               'Invalid ' .. key)
        assert(value > 0 or key == 'payload_bytes', key .. ' must be positive')
    end
    assert(cfg.bucket_count >= cfg.replicasets and cfg.bucket_count <= cfg.rows,
           'Invalid bucket_count')
    local node_count = cfg.replicasets * cfg.instances_per_replicaset +
                       cfg.routers + cfg.generators
    assert(cfg.base_port + node_count - 1 <= 65535,
           'The topology exceeds the available port range')
    return opts, cfg
end

local current, directory, result
local function save()
    util.write_json(fio.pathjoin(directory, 'results.json'), result)
end

local function print_logs()
    if directory then
        util.print_log(fio.pathjoin(directory, 'controller.log'))
    end
    if current then
        for _, node in ipairs(current.nodes) do
            util.print_log(fio.pathjoin(current.directory, node.name .. '.log'))
        end
    end
end

box.ctl.set_on_shutdown_timeout(10)
box.ctl.on_shutdown(function()
    if current then
        current:stop()
        result.status, result.failure = 'failed', 'Interrupted'
        save()
        io.stderr:write('Interrupted\n')
        print_logs()
        current = nil
    end
end)

local function run_case(cfg, binary, case, repetition)
    print(('# %s, repetition %d/%d'):format(case, repetition, cfg.repetitions))
    local path = fio.pathjoin(directory, case, tostring(repetition))
    current = cluster.new(cfg, root, binary, path)
    print('# Starting cluster...')
    local started = clock.monotonic()
    current:start()
    print(('# Cluster ready in %.2f s'):format(clock.monotonic() - started))
    if cfg.warmup then
        print('# Warmup...')
        local count = current:warmup(case)
        current:sync()
        print(('# Warmup done: %d ops'):format(count))
    end
    print(('# Measuring for %d s...'):format(cfg.duration_seconds))
    local generators = current:measure(case, cfg.duration_seconds)
    local row = report.add(result, case, generators)
    row.repetition_index = repetition
    util.write_json(fio.pathjoin(path, 'results.json'), {
        context = result.context,
        benchmarks = {row},
    })
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
    print(('# Completed %d ops in %d s'):format(row.iterations, row.real_time))
    print(('%s %.0f rps'):format(case, row.items_per_second))
end

local function main()
    local opts, cfg = parse_args(arg)
    if opts.help then
        print(help:format(defaults.replicasets,
                          defaults.instances_per_replicaset, defaults.routers,
                          defaults.bucket_count, defaults.generators,
                          defaults.payload_bytes, defaults.repetitions))
        return
    end
    local binary = fio.readlink('/proc/self/exe') or fio.abspath(arg[-1])
    local suffix = os.date('!%Y%m%dT%H%M%S') .. '-' .. uuid.str()
    local output = opts.output and fio.abspath(opts.output) or
                   fio.pathjoin(root, 'perf/output', suffix, 'vshard.json')
    assert(output:match('%.json$'), 'Output must be a .json file')
    local path = fio.pathjoin(fio.dirname(output),
                              fio.basename(output, '.json'))
    assert(not fio.path.exists(output), 'Output already exists: ' .. output)
    assert(not fio.path.exists(path), 'Log directory already exists: ' .. path)
    directory = path
    assert(fio.mktree(directory))
    log.cfg({log = fio.pathjoin(directory, 'controller.log')})
    result = report.new(cfg, root)
    print(('# %d replica sets, %d instances per replica set'):format(
        cfg.replicasets, cfg.instances_per_replicaset))
    print(('# %d routers, %d generators, %d fibers per generator'):format(
        cfg.routers, cfg.generators, cfg.fibers_per_generator))
    print(('# %d buckets, %d rows, %d payload bytes'):format(
        cfg.bucket_count, cfg.rows, cfg.payload_bytes))
    print('# Results: ' .. output)
    save()
    for _, case in ipairs(cfg.cases) do
        for repetition = 1, cfg.repetitions do
            run_case(cfg, binary, case, repetition)
        end
    end
    report.aggregate(result)
    if cfg.repetitions > 1 then
        print(('# Mean of %d repetitions:'):format(cfg.repetitions))
        for _, row in ipairs(result.benchmarks) do
            print(('%s %.0f rps'):format(row.name, row.items_per_second))
        end
    end
    result.status = 'complete'
    save()
    util.write_json(output, {
        context = result.context,
        benchmarks = result.benchmarks,
    })
end

local ok, err = xpcall(main, function(value)
    return debug.traceback(tostring(value), 2)
end)
if not ok then
    if current then
        current:stop()
    end
    if result then
        result.status, result.failure = 'failed', err
        save()
    end
    io.stderr:write(err .. '\n')
    print_logs()
    current = nil
end
os.exit(ok and 0 or 1)
