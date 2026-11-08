local clock = require('clock')
local fiber = require('fiber')

local function workload_operation(cfg, case)
    local args, opts = {}, {timeout = cfg.rpc_timeout_seconds}
    local buckets = cfg.bucket_count
    local request = {0, 'perf.noop', args, opts}
    if case == 'callrw_noop' then
        return function(connection, key)
            request[1] = (key - 1) % buckets + 1
            return connection:call('vshard.router.callrw', request, opts)
        end
    elseif case == 'callro_get' then
        request[2] = 'box.space.perf_data:get'
        return function(connection, key)
            request[1] = (key - 1) % buckets + 1
            args[1] = key
            local tuple, err = connection:call('vshard.router.callro',
                                               request, opts)
            if not tuple then
                return nil, err or 'Missing tuple'
            end
            if tuple[1] ~= key then
                return nil, 'Unexpected tuple key'
            end
            return tuple
        end
    elseif case == 'callrw_replace' then
        local tuple = {0, 0, string.rep('x', cfg.payload_bytes)}
        args[1] = tuple
        request[2] = 'box.space.perf_data:replace'
        return function(connection, key)
            local bid = (key - 1) % buckets + 1
            tuple[1], tuple[2] = key, bid
            request[1] = bid
            return connection:call('vshard.router.callrw', request, opts)
        end
    else
        error('Unknown workload: ' .. tostring(case))
    end
end

local function workload_run(routers, cfg, case, generator_index, start_at,
                            duration)
    math.randomseed(cfg.seed + generator_index - 1)
    local result = {
        iterations = 0,
        errors = 0,
    }
    local workers, deadline = {}, start_at + duration
    for index = 1, cfg.fibers_per_generator do
        local op = workload_operation(cfg, case)
        local target = ((generator_index - 1) *
                        cfg.fibers_per_generator + index - 1) % #routers + 1
        local worker = fiber.new(function()
            repeat
                fiber.sleep(math.max(0, start_at - clock.monotonic()))
            until clock.monotonic() >= start_at
            while clock.monotonic() < deadline do
                local key = math.random(cfg.rows)
                local ok, value, err = pcall(op, routers[target], key)
                local finish = clock.monotonic()
                target = target % #routers + 1
                if not ok or value == nil or err ~= nil then
                    result.errors = result.errors + 1
                    if result.errors == 1 then
                        local cause
                        if ok then
                            cause = err
                        else
                            cause = value
                        end
                        if type(cause) == 'table' then
                            cause = cause.message or cause
                        end
                        result.error = tostring(cause)
                    end
                elseif finish < deadline then
                    result.iterations = result.iterations + 1
                end
            end
        end)
        worker:set_joinable(true)
        table.insert(workers, worker)
        if clock.monotonic() >= start_at then
            for _, pending in ipairs(workers) do
                pending:cancel()
            end
            for _, pending in ipairs(workers) do
                pending:join()
            end
            error('Generator missed the measurement barrier')
        end
    end
    for _, worker in ipairs(workers) do
        local ok, err = worker:join()
        if not ok then
            error(err)
        end
    end
    return result
end

return {
    run = workload_run,
}
