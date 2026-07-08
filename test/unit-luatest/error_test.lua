local t = require('luatest')
local vutil = require('vshard.util')
local verror = require('vshard.error')

local g = t.group('error')

g.test_box_error_prev = function()
    t.run_only_if(vutil.feature.error_stack)

    local code = box.error.PROC_LUA
    local e1 = box.error.new(code, 'err1')
    local e2 = box.error.new(code, 'err2')
    local e3 = box.error.new(code, 'err3')
    e1:set_prev(e2)
    e2:set_prev(e3)

    local ve1 = verror.box(e1)
    local ve2 = ve1.prev
    ve1.prev = nil
    local ve3 = ve2.prev
    ve2.prev = nil
    t.assert_type(ve1, 'table')
    t.assert_type(ve2, 'table')
    t.assert_type(ve3, 'table')

    e1 = e1:unpack()
    e1.prev = nil
    e2 = e2:unpack()
    e2.prev = nil
    e3 = e3:unpack()

    t.assert_equals(e1, ve1)
    t.assert_equals(e2, ve2)
    t.assert_equals(e3, ve3)
end

g.test_wrap_legacy_storage_call_error = function()
    for _, error_type in ipairs({'LuajitError', 'ClientError'}) do
        local prev = {
            type = error_type,
            code = box.error.PROC_LUA,
            message = '/vshard/storage/init.lua:123: ' ..
                      'attempt to call a nil value',
        }
        local err = verror.wrap_storage_call('new_service', prev)
        t.assert_equals(err.code, box.error.UNSUPPORTED)
        t.assert_equals(err.message,
                        'vshard.storage._call does not support new_service')
        t.assert(rawequal(err.prev, prev))
    end
end

g.test_wrap_storage_call_error_ignores_other_errors = function()
    local errors = {
        {
            type = 'LuajitError',
            code = box.error.PROC_LUA,
            message = 'some other error',
        },
        {
            type = 'ClientError',
            code = box.error.ACCESS_DENIED,
            message = 'attempt to call a nil value',
        },
    }
    for _, err in ipairs(errors) do
        t.assert(rawequal(verror.wrap_storage_call('new_service', err), err))
    end
end
