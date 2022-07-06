local t = require('luatest')
local luuid = require('uuid')
local server = require('test.luatest_helpers.server')
local vutil = require('vshard.util')

local test_group = t.group('util')

test_group.before_all(function(g)
    g.server = server:new({alias = 'node'})
    g.server:start()
    g.server:exec(function()
        rawset(_G, 'ivutil', require('vshard.util'))
    end)
end)

test_group.after_all(function(g)
    g.server:stop()
end)

test_group.test_uri_eq = function()
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

test_group.test_table_extend = function()
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

local function test_index_templ(g, engine)
    g.server:exec(function(engine)
        ilt.assert_not_equals(engine, nil)
        local index_min = ivutil.index_min
        local index_has = ivutil.index_has

        local s = box.schema.create_space('test', {engine = engine})
        local pk = s:create_index('pk')
        local sk = s:create_index('sk', {
            parts = {{2, 'unsigned'}},
            unique = false,
        })

        ilt.assert_equals(index_min(pk), nil)
        ilt.assert_equals(index_min(pk, {}), nil)
        ilt.assert_equals(index_min(pk, 1), nil)
        ilt.assert_equals(index_min(pk, {1}), nil)
        ilt.assert_not(index_has(pk))
        ilt.assert_not(index_has(pk, {}))
        ilt.assert_not(index_has(pk, 1))
        ilt.assert_not(index_has(pk, {1}))

        ilt.assert_equals(index_min(sk), nil)
        ilt.assert_equals(index_min(sk, {}), nil)
        ilt.assert_equals(index_min(sk, 1), nil)
        ilt.assert_equals(index_min(sk, {1}), nil)
        ilt.assert_not(index_has(sk))
        ilt.assert_not(index_has(sk, {}))
        ilt.assert_not(index_has(sk, 1))
        ilt.assert_not(index_has(sk, {1}))

        s:replace{1, 4}
        s:replace{2, 3}
        s:replace{3, 2}
        s:replace{4, 1}
        s:replace{5, 4}
        s:replace{6, 3}
        s:replace{7, 2}
        s:replace{8, 1}

        ilt.assert_equals(index_min(pk), {1, 4})
        ilt.assert_equals(index_min(pk, {}), {1, 4})
        ilt.assert_equals(index_min(pk, 2), {2, 3})
        ilt.assert_equals(index_min(pk, {3}), {3, 2})
        ilt.assert_equals(index_min(pk, 9), nil)
        ilt.assert_equals(index_min(pk, {9}), nil)
        ilt.assert_equals(index_min(pk, 0), nil)
        ilt.assert_equals(index_min(pk, {0}), nil)

        ilt.assert(index_has(pk))
        ilt.assert(index_has(pk, {}))
        ilt.assert(index_has(pk, 2))
        ilt.assert(index_has(pk, {2}))
        ilt.assert_not(index_has(pk, 9))
        ilt.assert_not(index_has(pk, {9}))
        ilt.assert_not(index_has(pk, 0))
        ilt.assert_not(index_has(pk, {0}))

        ilt.assert_equals(index_min(sk), {4, 1})
        ilt.assert_equals(index_min(sk, {}), {4, 1})
        ilt.assert_equals(index_min(sk, 2), {3, 2})
        ilt.assert_equals(index_min(sk, {3}), {2, 3})
        ilt.assert_equals(index_min(sk, 5), nil)
        ilt.assert_equals(index_min(sk, {5}), nil)
        ilt.assert_equals(index_min(sk, 0), nil)
        ilt.assert_equals(index_min(sk, {0}), nil)

        ilt.assert(index_has(sk))
        ilt.assert(index_has(sk, {}))
        ilt.assert(index_has(sk, 2))
        ilt.assert(index_has(sk, {2}))
        ilt.assert_not(index_has(sk, 5))
        ilt.assert_not(index_has(sk, {5}))
        ilt.assert_not(index_has(sk, 0))
        ilt.assert_not(index_has(sk, {0}))

        s:drop()
    end, {engine})
end

test_group.test_index = function(g)
    test_index_templ(g, 'memtx')
    test_index_templ(g, 'vinyl')
end
