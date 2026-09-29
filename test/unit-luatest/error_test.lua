local t = require('luatest')
local json = require('json')
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

--
-- gh-651: __tostring of an error is json, and it never throws.
--
g.test_tostring = function()
    local errs = {
        verror.make('string error'),
        verror.make(box.error.new(box.error.PROC_LUA, 'box error')),
        verror.vshard(verror.code.NO_SUCH_REPLICASET, 'rs1'),
        verror.make({message = 'table error', type = 'CustomError'}),
    }
    for _, err in pairs(errs) do
        t.assert_equals(tostring(err), json.encode(err))
        t.assert_equals(json.decode(tostring(err)).message, err.message)
    end
end

g.test_tostring_unencodable = function()
    -- Function can't be encoded.
    local err = verror.make({message = 'msg', context = function() end})
    t.assert_equals(json.decode(tostring(err)), {message = 'msg'})
    -- Table key must be a number or a string.
    err = verror.make({message = 'msg', [false] = 'value'})
    t.assert_equals(json.decode(tostring(err)), {message = 'msg'})
    -- The nested errors are not lost, when some other part is unencodable.
    err = verror.make({
        message = 'msg',
        payload = {[false] = 'value'},
        prev = {message = 'prev', context = function() end},
    })
    t.assert_equals(json.decode(tostring(err)), {
        message = 'msg',
        payload = {},
        prev = {message = 'prev'},
    })
end

g.test_tostring_never_throws = function()
    -- NaN can be encoded only when the json is configured to do so.
    local old_cfg = json.cfg.encode_invalid_numbers
    json.cfg{encode_invalid_numbers = false}
    local ok, res = pcall(function()
        local err = verror.make({message = 'msg', invalid = 0 / 0})
        return tostring(err)
    end)
    json.cfg{encode_invalid_numbers = old_cfg}
    t.assert(ok, res)
    t.assert_str_contains(res, 'msg')
end
