local t = require('luatest')
local luuid = require('uuid')
local vutil = require('vshard.util')

local g = t.group('util')

g.test_uri_eq = function()
    local uri_eq = vutil.uri_eq
    --
    -- Equal.
    --
    t.assert(uri_eq(1, 1))
    t.assert(uri_eq('1', '1'))
    t.assert(uri_eq(1, '1'))
    t.assert(uri_eq('1', 1))

    --
    -- Not equal.
    --
    t.assert(not uri_eq(1, 2))
    t.assert(not uri_eq('1', '2'))
    t.assert(not uri_eq(1, '2'))
    t.assert(not uri_eq('1', 2))

    if not vutil.feature.multilisten then
        return
    end

    --
    -- Equal.
    --
    t.assert(uri_eq({1}, 1))
    t.assert(uri_eq(1, {1}))
    t.assert(uri_eq('1', {'1'}))
    t.assert(uri_eq({'1'}, '1'))
    t.assert(uri_eq({'1'}, {'1'}))
    t.assert(uri_eq({1}, {1}))

    t.assert(uri_eq({'1'}, 1))
    t.assert(uri_eq('1', {1}))
    t.assert(uri_eq({1}, '1'))
    t.assert(uri_eq(1, {'1'}))
    t.assert(uri_eq({1}, {'1'}))
    t.assert(uri_eq({'1'}, {1}))

    t.assert(uri_eq({
        1, params = {key1 = 10, key2 = 20}
    }, {
        1, params = {key1 = 10, key2 = 20}
    }))

    t.assert(uri_eq({
        '1', params = {key1 = 10, key2 = '20'}
    }, {
        1, params = {key1 = 10, key2 = 20}
    }))

    t.assert(uri_eq({
        1, params = {key1 = {'10', 20}, key2 = 30}
    }, {
        '1', params = {key1 = {10, 20}, key2 = 30}
    }))

    t.assert(uri_eq(
        'localhost:1?key1=10&key1=20&key2=30',
        {'localhost:1', params = {key1 = {10, 20}, key2 = 30}}
    ))

    --
    -- Not equal.
    --
    t.assert(not uri_eq({
        1, params = {key1 = 10, key2 = 20}
    }, {
        1, params = {key1 = 20, key2 = 10}
    }))

    t.assert(not uri_eq({
        '1', params = {key1 = 10}
    }, {
        1, params = {key1 = 10, key2 = 20}
    }))

    t.assert(not uri_eq({
        1, params = {key1 = {10}, key2 = 30}
    }, {
        1, params = {key1 = {10, 20}, key2 = 30}
    }))

    t.assert(not uri_eq(
        'localhost:1?key1=10&key1=20&key2=30',
        {'localhost:1', params = {key1 = {20, 10}, key2 = 30}}
    ))

    t.assert_error(uri_eq, 1, luuid.new())
    t.assert_error(uri_eq, nil, 1)
end

g.test_table_extend = function()
    local function test_extend(dst, src, res)
        local orig_src = table.copy(src)
        t.assert(vutil.table_extend(dst, src) == dst)
        t.assert_equals(dst, res)
        t.assert_equals(src, orig_src)
    end
    test_extend({}, {}, {})
    test_extend({1}, {}, {1})
    test_extend({}, {1}, {1})
    test_extend({1, 2}, {3}, {1, 2, 3})
    test_extend({1}, {2, 3}, {1, 2, 3})
    test_extend({}, {1, 2, 3}, {1, 2, 3})
    test_extend({1, 2, 3}, {}, {1, 2, 3})
end
