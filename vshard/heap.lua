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

--
-- Generate a new heap.
--
-- The implementation is targeted on as few index accesses as possible.
-- Everything what could be is stored as upvalue variables instead of as indexes
-- in a table. What couldn't be an upvalue and is used in a function more than
-- once is saved on the stack.
--
local function heap_new(is_left_above)
    -- Having it as an upvalue allows not to do 'self.data' lookup in each
    -- function.
    local data = {}
    -- Saves #data calculation. In Lua it is not just reading a number.
    local count = 0

    local function heap_update_index_up(idx)
        if idx == 1 then
            return false
        end

        local orig_idx = idx
        local value = data[idx]
        local pidx = heap_parent_index(idx)
        local parent = data[pidx]
        while is_left_above(value, parent) do
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

    local function heap_update_index_down(idx)
        local left_idx = heap_left_child_index(idx)
        if left_idx > count then
            return false
        end

        local orig_idx = idx
        local left
        local right
        local right_idx
        local top
        local top_idx
        local value = data[idx]
        repeat
            right_idx = left_idx + 1
            if right_idx > count then
                top = data[left_idx]
                if is_left_above(value, top) then
                    break
                end
                top_idx = left_idx
            else
                left = data[left_idx]
                right = data[right_idx]
                if is_left_above(left, right) then
                    if is_left_above(value, left) then
                        break
                    end
                    top_idx = left_idx
                    top = left
                else
                    if is_left_above(value, right) then
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

    local function heap_update_index(idx)
        if not heap_update_index_up(idx) then
            heap_update_index_down(idx)
        end
    end

    local function heap_push(self, value)
        count = count + 1
        data[count] = value
        value.index = count
        heap_update_index_up(count)
    end

    local function heap_update_top(self)
        heap_update_index_down(1)
    end

    local function heap_update(self, value)
        heap_update_index(value.index)
    end

    local function heap_remove_top(self)
        if count == 0 then
            return
        end
        data[1].index = -1
        if count == 1 then
            data[1] = nil
            count = 0
            return
        end
        local value = data[count]
        data[count] = nil
        data[1] = value
        value.index = 1
        count = count - 1
        heap_update_index_down(1)
    end

    local function heap_remove(self, value)
        local idx = value.index
        value.index = -1
        if idx == count then
            data[count] = nil
            count = count - 1
            return
        end
        value = data[count]
        data[idx] = value
        data[count] = nil
        value.index = idx
        count = count - 1
        heap_update_index(idx)
    end

    local function heap_remove_try(self, value)
        local idx = value.index
        if idx and idx > 0 then
            heap_remove(self, value)
        end
    end

    local function heap_pop(self)
        if count == 0 then
            return
        end
        -- Some duplication from remove_top, but allows to save a few
        -- condition checks, index accesses, and a function call.
        local res = data[1]
        res.index = -1
        if count == 1 then
            data[1] = nil
            count = 0
            return res
        end
        local value = data[count]
        data[count] = nil
        data[1] = value
        value.index = 1
        count = count - 1
        heap_update_index_down(1)
        return res
    end

    local function heap_top(self)
        return data[1]
    end

    local function heap_count(self)
        return count
    end

    return {
        -- Expose the data. For testing.
        data = data,
        -- Methods are exported as members instead of __index so as to save on
        -- not taking a metatable and going through __index on each method call.
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
end

return {
    new = heap_new,
}
