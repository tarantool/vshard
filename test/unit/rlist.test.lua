--
-- gh-161: parallel rebalancer. One of the most important part of the latter is
-- a dispenser. It is a structure which hands out destination UUIDs in a
-- round-robin manner to worker fibers. It uses rlist data structure.
--
rlist = require('vshard.rlist')

list = rlist.new()
list

obj1 = {i = 1}
list:remove(obj1)
list

list:add_tail(obj1)
list

list:remove(obj1)
list
obj1

list:add_tail(obj1)
obj2 = {i = 2}
list:add_tail(obj2)
list
obj3 = {i = 3}
list:add_tail(obj3)
list

list:remove(obj2)
list
list:remove(obj1)
list
