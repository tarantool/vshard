--
-- A subset of rlist methods from the main repository. Rlist is a
-- doubly linked list, and is used here to implement a queue of
-- routes in the parallel rebalancer.
--
local rlist_index = {}

function rlist_index.add_tail(rlist, object)
    local last = rlist.last
    if last then
        last.next = object
        object.prev = last
    else
        rlist.first = object
    end
    rlist.last = object
    rlist.count = rlist.count + 1
end

function rlist_index.remove(rlist, object)
    local prev = object.prev
    local next = object.next
    local belongs_to_list = false
    if prev then
        belongs_to_list = true
        prev.next = next
    end
    if next then
        belongs_to_list = true
        next.prev = prev
    end
    object.prev = nil
    object.next = nil
    if rlist.last == object then
        belongs_to_list = true
        rlist.last = prev
    end
    if rlist.first == object then
        belongs_to_list = true
        rlist.first = next
    end
    if belongs_to_list then
        rlist.count = rlist.count - 1
    end
end

local rlist_mt = {
    __index = rlist_index,
}

local function rlist_new()
    return setmetatable({count = 0}, rlist_mt)
end

return {
    new = rlist_new,
}
