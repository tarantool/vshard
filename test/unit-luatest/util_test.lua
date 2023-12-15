local t = require('luatest')
local luuid = require('uuid')
local server = require('test.luatest_helpers.server')
local vutil = require('vshard.util')
local verror = require('vshard.error')
local net = require('net.box')

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

test_group.test_future_wait_exception = function(g)
    -- Before 2.10.3 executing future_wait in trigger cause silent hang.
    t.run_only_if(vutil.version_is_at_least(2, 10, 3, nil, 0, 0))
    local conn = net.connect(g.server.net_box_uri, {wait_connected = false})
    t.assert_equals(conn.state, 'initial')

    -- Test that no exceptions're thrown from future_wait
    local _, err
    conn:on_connect(function()
        local f = conn:eval('return true', {}, {is_async = true})
        _, err = vutil.future_wait(f)
    end)

    conn:wait_connected()
    t.assert_str_contains(err.message, 'Synchronous requests are not allowed')
end

test_group.test_future_wait_timeout = function(g)
    g.server:exec(function()
        local fiber = require('fiber')
        rawset(_G, 'do_sleep', true)
        rawset(_G, 'fiber_sleep', function()
            while _G.do_sleep do
                fiber.sleep(0.001)
            end
            return true
        end)
    end)

    local function stop_sleep()
        g.server:exec(function()
            _G.do_sleep = false
        end)
    end

    local conn = net.connect(g.server.net_box_uri)
    local f = conn:call('_G.fiber_sleep', {}, {is_async = true})

    -- Default timeout error
    local _, err = vutil.future_wait(f, 1e-6)
    t.assert_equals(verror.is_timeout(err), true)

    -- Negative timeout should return timeout error too
    _, err = vutil.future_wait(f, -1)
    t.assert_equals(verror.is_timeout(err), true)

    stop_sleep()
    -- Everything is all right, wait for result
    t.assert_equals(vutil.future_wait(f)[1], true)
end

test_group.test_replicaset_uuid = function(g)
    g.server:exec(function()
        local _schema = box.space._schema
        local t = _schema:get{'replicaset_uuid'}
        t = t ~= nil and t or _schema:get{'cluster'}
        ilt.assert_equals(ivutil.replicaset_uuid(), t[2])
    end)
end
