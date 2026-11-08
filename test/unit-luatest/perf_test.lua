local t = require('luatest')
local clock = require('clock')
local fiber = require('fiber')
local workload = require('perf.lib.workload')

local g = t.group('perf_unit')

g.test_early_generator_wakeup = function()
    local cfg = {
        fibers_per_generator = 1,
        bucket_count = 1,
        rows = 1,
        seed = 1,
        rpc_timeout_seconds = 1,
    }
    local monotonic = clock.monotonic
    local sleep = fiber.sleep
    local now, sleeps = 0, 0
    local requests = {}
    clock.monotonic = function() return now end
    fiber.sleep = function()
        sleeps = sleeps + 1
        now = sleeps == 1 and 9 or 10
    end
    local connection = {
        call = function()
            table.insert(requests, now)
            now = now + 0.5
            return {1}
        end,
    }
    local ok, result = pcall(workload.run, {connection}, cfg, 'callro_get',
                             1, 10, 1)
    clock.monotonic = monotonic
    fiber.sleep = sleep
    t.assert(ok, tostring(result))
    t.assert_equals(requests, {10, 10.5})
    t.assert_equals(result.iterations, 1)
    t.assert_equals(result.errors, 0)
end

g.test_warmup_all_buckets = function()
    local cfg = {
        fibers_per_generator = 3,
        bucket_count = 7,
        rpc_timeout_seconds = 1,
        payload_bytes = 0,
    }
    for _, case in ipairs({'callro_get', 'callrw_replace'}) do
        local routers, visits = {}, {}
        for index = 1, 2 do
            visits[index] = {}
            routers[index] = {
                call = function(_, method, request)
                    fiber.sleep(0)
                    local bucket = request[1]
                    t.assert_equals(method, case == 'callro_get' and
                                    'vshard.router.callro' or
                                    'vshard.router.callrw')
                    local key = case == 'callro_get' and request[3][1] or
                                request[3][1][1]
                    t.assert_equals(key, bucket)
                    visits[index][bucket] = (visits[index][bucket] or 0) + 1
                    return {key}
                end,
            }
        end
        t.assert_equals(workload.warmup(routers, cfg, case), 14)
        for _, buckets in ipairs(visits) do
            t.assert_equals(buckets, {1, 1, 1, 1, 1, 1, 1})
        end
    end
end
