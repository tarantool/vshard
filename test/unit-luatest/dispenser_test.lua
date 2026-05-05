local t = require('luatest')
local test_group = t.group('dispenser-unit')
local route_dispenser = require('vshard.storage.route_dispenser')

--
-- gh-161: parallel rebalancer. One of the most important part of
-- the latter is a dispenser. It is a structure which hands out
-- destination UUIDs in a round-robin manner to worker fibers.
--
test_group.test_dispenser_basic = function()
    local d = route_dispenser.new({uuid = 15})
    for _ = 1, 15 do
        t.assert_equals(d:pop(), 'uuid')
    end
    for _ = 1, 3 do
        t.assert_equals(d.rlist.count, 0)
        t.assert_equals(d.map.uuid.bucket_count, 0)
        t.assert_equals(d.map.uuid.progress, 0)
        t.assert_equals(d.map.uuid.need_to_send, 15)
        t.assert_equals(d:pop(), nil)
    end

    -- Test throttle.
    d = route_dispenser.new({uuid1 = 5, uuid2 = 5})
    local uuid = d:pop()
    t.assert_equals(uuid, 'uuid2')
    t.assert_equals(d.rlist.last.bucket_count, 4)
    t.assert_equals(d.rlist.first.bucket_count, 5)
    d:put(uuid)
    t.assert_equals(d.rlist.last.bucket_count, 5)
    t.assert(d:throttle(uuid))
    t.assert_not(d:throttle(uuid))
    for _ = 1, 5 do
        t.assert_equals(d:pop(), 'uuid1')
        t.assert_equals(d:pop(), 'uuid2')
    end

end

test_group.test_skip = function()
    -- Double skip should be ok. It happens, if there were several
    -- workers on one destination, and all of them received an error.
    local d = route_dispenser.new({uuid1 = 1})
    d:skip('uuid1')
    d:skip('uuid1')
end
