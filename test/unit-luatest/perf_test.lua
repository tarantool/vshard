local t = require('luatest')
local clock = require('clock')
local fiber = require('fiber')
local config = require('perf.lib.config')
local workload = require('perf.lib.workload')

local g = t.group('perf_unit')

g.test_early_generator_wakeup = function()
    local _, cfg = config.parse({})
    cfg.fibers_per_generator = 1
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
            return true
        end,
    }
    local ok, result = pcall(workload.run, {connection}, cfg, 'callrw_noop',
                             1, 10, 1)
    clock.monotonic = monotonic
    fiber.sleep = sleep
    t.assert(ok, tostring(result))
    t.assert_equals(requests, {10, 10.5})
    t.assert_equals(result.iterations, 1)
    t.assert_equals(result.errors, 0)
end

g.test_late_generator_preparation = function()
    local _, cfg = config.parse({})
    cfg.fibers_per_generator = 4
    local monotonic = clock.monotonic
    local new_fiber = fiber.new
    local now, requests = 0, 0
    local workers = {}
    local connection = {
        call = function()
            requests = requests + 1
            return true
        end,
    }
    clock.monotonic = function() return now end
    fiber.new = function(...)
        local worker = new_fiber(...)
        table.insert(workers, worker)
        if #workers == 2 then
            now = 2
        end
        return worker
    end
    local ok, err = pcall(workload.run, {connection}, cfg, 'callrw_noop',
                          1, 1, 1)
    clock.monotonic = monotonic
    fiber.new = new_fiber
    local unjoined = 0
    for _, worker in ipairs(workers) do
        if worker:status() ~= 'dead' then
            worker:cancel()
        end
        if pcall(worker.join, worker) then
            unjoined = unjoined + 1
        end
    end
    t.assert_not(ok)
    t.assert_str_contains(tostring(err), 'missed the measurement barrier')
    t.assert_equals(#workers, 2)
    t.assert_equals(requests, 0)
    t.assert_equals(unjoined, 0)
end
