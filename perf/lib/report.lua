local tarantool = require('tarantool')
local util = require('perf.lib.util')

local function report_new(cfg, root)
    local git = "git -C '" .. root:gsub("'", "'\\''") .. "' rev-parse "
    return {
        status = 'running',
        context = {
            date = os.date('!%Y-%m-%dT%H:%M:%SZ'),
            config = cfg,
            tarantool_version = tarantool.version,
            vshard_commit = os.getenv('PERF_COMMIT') or util.capture(
                git .. 'HEAD 2>/dev/null'),
            vshard_branch = os.getenv('PERF_BRANCH') or util.capture(
                git .. '--abbrev-ref HEAD 2>/dev/null'),
        },
        benchmarks = {},
    }
end

local function report_add(report, case, generators)
    local cfg = report.context.config
    local row = {
        name = case,
        iterations = 0,
        errors = 0,
    }
    for name, generator in pairs(generators) do
        row.iterations = row.iterations + generator.iterations
        row.errors = row.errors + generator.errors
        if generator.errors > 0 and not row.error then
            row.error = name .. ': ' .. tostring(generator.error)
        end
    end
    row.items_per_second = row.iterations / cfg.duration_seconds
    table.insert(report.benchmarks, row)
    return row
end

return {
    new = report_new,
    add = report_add,
}
