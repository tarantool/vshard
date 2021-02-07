test_run = require('test_run').new()
fiber = require('fiber')
log = require('log')
test_util = require('util')
util = require('vshard.util')

test_run:cmd("setopt delimiter ';'")
fake_M = {
    reloadable_func = nil,
    module_version = 1,
};
test_run:cmd("setopt delimiter ''");
function slow_fail() fiber.sleep(0.01) error('Error happened.') end
-- Check autoreload on function change during failure.
fake_M.reloadable_function = function () fake_M.reloadable_function = slow_fail; slow_fail() end

fib = util.reloadable_fiber_create('Worker_name', fake_M, 'reloadable_function')
while not test_run:grep_log('default', 'reloadable function reloadable_function has been changed') do fiber.sleep(0.01); end
fib:cancel()
test_run:grep_log('default', 'module is reloaded, restarting')
test_run:grep_log('default', 'reloadable_function has been started')
log.info(string.rep('a', 1000))

-- Check reload feature.
fake_M.reloadable_function = function () fiber.sleep(0.01); return true end
fib = util.reloadable_fiber_create('Worker_name', fake_M, 'reloadable_function')
while not test_run:grep_log('default', 'module is reloaded, restarting') do fiber.sleep(0.01) end
test_run:grep_log('default', 'reloadable_function has been started', 1000)
fib:cancel()

-- Yielding table minus.
minus_yield = util.table_minus_yield
minus_yield({}, {}, 1)
minus_yield({}, {k = 1}, 1)
minus_yield({}, {k = 1}, 0)
minus_yield({k = 1}, {k = 1}, 0)
minus_yield({k1 = 1, k2 = 2}, {k1 = 1, k3 = 3}, 10)
minus_yield({k1 = 1, k2 = 2}, {k1 = 1, k2 = 2}, 10)
-- Mismatching values are not deleted.
minus_yield({k1 = 1}, {k1 = 2}, 10)
minus_yield({k1 = 1, k2 = 2, k3 = 3}, {k1 = 1, k2 = 222}, 10)

do                                                                              \
    t = {k1 = 1, k2 = 2, k3 = 3, k4 = 4}                                        \
    f = fiber.create(function()                                                 \
        minus_yield(t, {k2 = 2, k3 = 3, k5 = 5, k4 = 444}, 2)                   \
    end)                                                                        \
    yield_count = 0                                                             \
    while f:status() ~= 'dead' do                                               \
        yield_count = yield_count + 1                                           \
        fiber.yield()                                                           \
    end                                                                         \
end
yield_count
t

-- Yielding table copy.
copy_yield = util.table_copy_yield
copy_yield({}, 1)
copy_yield({k = 1}, 1)
copy_yield({k1 = 1, k2 = 2}, 1)

do                                                                              \
    t = {k1 = 1, k2 = 2, k3 = 3, k4 = 4}                                        \
    res = nil                                                                   \
    f = fiber.create(function()                                                 \
        res = copy_yield(t, 2)                                                  \
    end)                                                                        \
    yield_count = 0                                                             \
    while f:status() ~= 'dead' do                                               \
        yield_count = yield_count + 1                                           \
        fiber.yield()                                                           \
    end                                                                         \
end
yield_count
t
res
