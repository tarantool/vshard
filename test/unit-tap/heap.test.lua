#!/usr/bin/env tarantool

local tap = require('tap')
local heap = require('vshard.heap')

--
-- Max number of heap elements to test. Number of iterations in the test grows
-- as a factorial of this value. At a bigger value the test becomes too long
-- already.
--
local heap_size = 8

--
-- Type of the object stored in the intrusive heap.
--
local function min_heap_cmp(l, r)
    return l.value < r.value
end

local function max_heap_cmp(l, r)
    return l.value > r.value
end

local function new_object(value)
    return {value = value}
end

local function heap_check_indexes(heap)
    local count = heap:count()
    local data = heap.data
    for i = 1, count do
        assert(data[i].index == i)
    end
end

local function reverse(values, i1, i2)
    while i1 < i2 do
        values[i1], values[i2] = values[i2], values[i1]
        i1 = i1 + 1
        i2 = i2 - 1
    end
end

--
-- Implementation of std::next_permutation() from C++.
--
local function next_permutation(values)
    local count = #values
    if count <= 1 then
        return false
    end
    local i = count
    while true do
        local j = i
        i = i - 1
        if values[i] < values[j] then
            local k = count
            while values[i] >= values[k] do
                k = k - 1
            end
            values[i], values[k] = values[k], values[i]
            reverse(values, j, count)
            return true
        end
        if i == 1 then
            reverse(values, 1, count)
            return false
        end
    end
end

local function range(count)
    local res = {}
    for i = 1, count do
        res[i] = i
    end
    return res
end

--
-- Min heap fill and empty.
--
local function test_min_heap_basic(test)
    test:plan(1)

    local h = heap.new(min_heap_cmp)
    assert(not h:pop())
    assert(h:count() == 0)
    local values = {}
    for i = 1, heap_size do
        values[i] = new_object(i)
    end
    for counti = 1, heap_size do
        local indexes = range(counti)
        repeat
            for i = 1, counti do
                h:push(values[indexes[i]])
            end
            heap_check_indexes(h)
            assert(h:count() == counti)
            for i = 1, counti do
                assert(h:top() == values[i])
                assert(h:pop() == values[i])
                heap_check_indexes(h)
            end
            assert(not h:pop())
            assert(h:count() == 0)
        until not next_permutation(indexes)
    end

    test:ok(true, 'no asserts')
end

--
-- Max heap fill and empty.
--
local function test_max_heap_basic(test)
    test:plan(1)

    local h = heap.new(max_heap_cmp)
    assert(not h:pop())
    assert(h:count() == 0)
    local values = {}
    for i = 1, heap_size do
        values[i] = new_object(heap_size - i + 1)
    end
    for counti = 1, heap_size do
        local indexes = range(counti)
        repeat
            for i = 1, counti do
                h:push(values[indexes[i]])
            end
            heap_check_indexes(h)
            assert(h:count() == counti)
            for i = 1, counti do
                assert(h:top() == values[i])
                assert(h:pop() == values[i])
                heap_check_indexes(h)
            end
            assert(not h:pop())
            assert(h:count() == 0)
        until not next_permutation(indexes)
    end

    test:ok(true, 'no asserts')
end

--
-- Min heap update top element.
--
local function test_min_heap_update_top(test)
    test:plan(1)

    local h = heap.new(min_heap_cmp)
    for counti = 1, heap_size do
        local indexes = range(counti)
        repeat
            local values = {}
            for i = 1, counti do
                values[i] = new_object(0)
                h:push(values[i])
            end
            heap_check_indexes(h)
            for i = 1, counti do
                h:top().value = indexes[i]
                h:update_top()
            end
            heap_check_indexes(h)
            assert(h:count() == counti)
            for i = 1, counti do
                assert(h:top().value == i)
                assert(h:pop().value == i)
                heap_check_indexes(h)
            end
            assert(not h:pop())
            assert(h:count() == 0)
        until not next_permutation(indexes)
    end

    test:ok(true, 'no asserts')
end

--
-- Min heap update all elements in all possible positions.
--
local function test_min_heap_update(test)
    test:plan(1)

    local h = heap.new(min_heap_cmp)
    for counti = 1, heap_size do
        for srci = 1, counti do
            local endv = srci * 10 + 5
            for newv = 5, endv, 5 do
                local values = {}
                for i = 1, counti do
                    values[i] = new_object(i * 10)
                    h:push(values[i])
                end
                heap_check_indexes(h)
                local obj = values[srci]
                obj.value = newv
                h:update(obj)
                assert(obj.index >= 1)
                assert(obj.index <= counti)
                local prev = -1
                for i = 1, counti do
                    obj = h:pop()
                    assert(obj.index == -1)
                    assert(obj.value >= prev)
                    assert(obj.value >= 1)
                    prev = obj.value
                    obj.value = -1
                    heap_check_indexes(h)
                end
                assert(not h:pop())
                assert(h:count() == 0)
            end
        end
    end

    test:ok(true, 'no asserts')
end

--
-- Max heap delete all elements from all possible positions.
--
local function test_max_heap_delete(test)
    test:plan(1)

    local h = heap.new(max_heap_cmp)
    local inf = heap_size + 1
    for counti = 1, heap_size do
        for srci = 1, counti do
            local values = {}
            for i = 1, counti do
                values[i] = new_object(i)
                h:push(values[i])
            end
            heap_check_indexes(h)
            local obj = values[srci]
            obj.value = inf
            h:remove(obj)
            assert(obj.index == -1)
            local prev = inf
            for i = 2, counti do
                obj = h:pop()
                assert(obj.index == -1)
                assert(obj.value < prev)
                assert(obj.value >= 1)
                prev = obj.value
                obj.value = -1
                heap_check_indexes(h)
            end
            assert(not h:pop())
            assert(h:count() == 0)
        end
    end

    test:ok(true, 'no asserts')
end

local function test_min_heap_remove_top(test)
    test:plan(1)

    local h = heap.new(min_heap_cmp)
    for i = 1, heap_size do
        h:push(new_object(i))
    end
    for i = 1, heap_size do
        assert(h:top().value == i)
        h:remove_top()
    end
    assert(h:count() == 0)

    test:ok(true, 'no asserts')
end

local function test_max_heap_remove_try(test)
    test:plan(1)

    local h = heap.new(max_heap_cmp)
    local obj = new_object(1)
    assert(obj.index == nil)
    h:remove_try(obj)
    assert(h:count() == 0)

    h:push(obj)
    h:push(new_object(2))
    assert(obj.index == 2)
    h:remove(obj)
    assert(obj.index == -1)
    h:remove_try(obj)
    assert(obj.index == -1)
    assert(h:count() == 1)

    test:ok(true, 'no asserts')
end

local test = tap.test('heap')
test:plan(7)

test:test('min_heap_basic', test_min_heap_basic)
test:test('max_heap_basic', test_max_heap_basic)
test:test('min_heap_update_top', test_min_heap_update_top)
test:test('min heap update', test_min_heap_update)
test:test('max heap delete', test_max_heap_delete)
test:test('min heap remove top', test_min_heap_remove_top)
test:test('max heap remove try', test_max_heap_remove_try)

os.exit(test:check() and 0 or 1)
