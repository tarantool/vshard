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
