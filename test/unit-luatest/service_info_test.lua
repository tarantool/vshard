local t = require('luatest')
local fiber = require('fiber')
local vinfo = require('vshard.service_info')
local vtest = require('test.luatest_helpers.vtest')

local g = t.group('service_info')

local function make_service_default(service)
    service.status_idx = 0
    service:set_activity('activity_1')
    service:set_status_error('error_1')
end

local function test_info_equals_default(service_info)
    t.assert_equals(service_info.status, 'error')
    t.assert_equals(service_info.status_idx, 1)
    t.assert_equals(service_info.activity, 'activity_1')
    t.assert_equals(service_info.error, 'error_1')
end

g.before_all(function(g)
    g.default_service = vinfo.new()
end)

g.before_each(function(g)
    -- Restore service to default values
    make_service_default(g.default_service)
end)

g.test_basic = function(g)
    local info = g.default_service:info()
    -- Test that a copy of data is returned and initial one cannot
    -- be changed via the returned value of service_info:info().
    info.status = 'some status'
    info = g.default_service:info()
    test_info_equals_default(info)

    -- Cannot set a new error as a first one was already set and
    -- the iteration wasn't reset (next_iter()). Only first occurred
    -- error on every service iteration must be saved.
    g.default_service:set_status_error('error_2')
    info = g.default_service:info()
    t.assert_equals(info.error, 'error_1')
    t.assert_equals(info.status_idx, 1)

    -- Let's assume, that on the second iteration (after resetting with
    -- next_iter()) 'error_1' didn't occur, but 'error_2' did.
    g.default_service:next_iter()
    g.default_service:set_status_error('error_2')
    info = g.default_service:info()
    t.assert_equals(info.error, 'error_2')
    t.assert_equals(info.status_idx, 2)

    -- Drop error, everyhting is all right
    g.default_service:set_status_ok()
    info = g.default_service:info()
    t.assert_equals(info.status, 'ok')
    t.assert_equals(info.status_idx, 3)
    t.assert_equals(info.error, '')
end

g.test_helpers = function(g)
    -- Scan for already occurred error
    vtest.service_wait_for_error(g.default_service, 'error_1')

    -- Wait for the new error (first occurred is the requested one)
    g.default_service:set_status_ok()
    fiber.new(function(service)
        service:set_status_error('error_3')
    end, g.default_service)
    vtest.service_wait_for_new_error(g.default_service, 'error_3')

    -- Wait until everything is good
    fiber.new(function(service)
        service:set_status_ok()
    end, g.default_service)
    vtest.service_wait_for_new_ok(g.default_service)
end
