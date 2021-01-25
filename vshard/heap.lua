local table_insert = table.insert
local table_remove = table.remove
local math_floor = math.floor

--
-- Implementation of a typical algorithm of the binary heap.
-- The heap is intrusive - it stores index of each element inside of it. It
-- allows to update and delete elements in any place in the heap, not only top
-- elements.
--

local function heap_parent_index(index)
    return math_floor(index / 2)
end

local function heap_left_child_index(index)
    return index * 2
end

local function heap_new()
    return {
        data = {}
    }
end

--
-- Template to generate min and max heaps from the same code. In future it can
-- be extended with more parameters in case it will be necessary, for instance,
-- to change 'index' key name in the objects.
--
-- The implementation is targeted on as few index accesses as possible. Both
-- numeric and string. Almost everything what is used more than once is saved on
-- the stack.
--
local function heap_create_meta(heap_is_above)

    local function heap_update_index_up(heap, idx)
        if idx == 1 then
            return false
        end

        local orig_idx = idx
        local data = heap.data
        local value = data[idx]
        local pidx = heap_parent_index(idx)
        local parent = data[pidx]
        while heap_is_above(value, parent) do
            data[idx] = parent
            parent.index = idx
            idx = pidx
            if idx == 1 then
                break
            end
            pidx = heap_parent_index(idx)
            parent = data[pidx]
        end

        if idx == orig_idx then
            return false
        end
        data[idx] = value
        value.index = idx
        return true
    end

    local function heap_update_index_down(heap, idx)
        local left_idx = heap_left_child_index(idx)
        local data = heap.data
        local count = #data
        if left_idx > count then
            return false
        end

        local orig_idx = idx
        local left
        local right
        local right_idx = left_idx + 1
        local top
        local top_idx
        local value = data[idx]
        repeat
            right_idx = left_idx + 1
            if right_idx > count then
                top = data[left_idx]
                if heap_is_above(value, top) then
                    break
                end
                top_idx = left_idx
            else
                left = data[left_idx]
                right = data[right_idx]
                if heap_is_above(left, right) then
                    if heap_is_above(value, left) then
                        break
                    end
                    top_idx = left_idx
                    top = left
                else
                    if heap_is_above(value, right) then
                        break
                    end
                    top_idx = right_idx
                    top = right
                end
            end

            data[idx] = top
            top.index = idx
            idx = top_idx
            left_idx = heap_left_child_index(idx)
        until left_idx > count

        if idx == orig_idx then
            return false
        end
        data[idx] = value
        value.index = idx
        return true
    end

    local function heap_update_index(heap, idx)
        if not heap_update_index_up(heap, idx) then
            heap_update_index_down(heap, idx)
        end
    end

    local function heap_push(heap, value)
        table_insert(heap.data, value)
        local idx = #heap.data
        value.index = idx
        heap_update_index_up(heap, idx)
    end

    local function heap_update_top(heap)
        heap_update_index_down(heap, 1)
    end

    local function heap_update(heap, value)
        heap_update_index(heap, value.index)
    end

    local function heap_remove_top(heap)
        local data = heap.data
        local count = #data
        if count == 0 then
            return
        end
        local value = data[1]
        value.index = -1
        if count == 1 then
            table_remove(data)
            return
        end
        value = data[count]
        data[1] = value
        value.index = 1
        table_remove(data)
        heap_update_index_down(heap, 1)
    end

    local function heap_remove(heap, value)
        local idx = value.index
        local data = heap.data
        local count = #data
        value.index = -1
        if idx == count then
            table_remove(data)
            return
        end
        value = data[count]
        data[idx] = value
        value.index = idx
        table_remove(data)
        heap_update_index(heap, idx)
    end

    local function heap_remove_try(heap, value)
        if value.index and value.index > 0 then
            return heap_remove(heap, value)
        end
    end

    local function heap_pop(heap)
        local data = heap.data
        if #data == 0 then
            return
        end
        local res = data[1]
        heap_remove_top(heap)
        return res
    end

    local function heap_top(heap)
        return heap.data[1]
    end

    local function heap_count(heap)
        return #heap.data
    end

    return {
        __index = {
            push = heap_push,
            update_top = heap_update_top,
            remove_top = heap_remove_top,
            pop = heap_pop,
            update = heap_update,
            remove = heap_remove,
            remove_try = heap_remove_try,
            top = heap_top,
            count = heap_count,
        }
    }
end

local function heap_min_is_above(l, r)
    return l < r
end

local heap_min_meta = heap_create_meta(heap_min_is_above)

local function heap_min_new(...)
    return setmetatable(heap_new(...), heap_min_meta)
end

local function heap_max_is_above(l, r)
    return l > r
end

local heap_max_meta = heap_create_meta(heap_max_is_above)

local function heap_max_new(...)
    return setmetatable(heap_new(...), heap_max_meta)
end

return {
    new_min = heap_min_new,
    new_max = heap_max_new,
}
