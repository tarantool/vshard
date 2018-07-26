#!/usr/bin/env tarantool

require('strict').on()
test_run = require('test_run').new()

local fio = require('fio')
local name = fio.basename(arg[0], '.lua')
cfg = require('config')
vshard = require('vshard')
os = require('os')
fiber = require('fiber')
local names = require('names')
local log = require('log')
rs_uuid = names.rs_uuid
replica_uuid = names.replica_uuid
local port
zone = nil
if name == 'router_1' then
	port = 3333
	zone = 1
elseif name == 'router_2' then
	port = 3334
	zone = 2
elseif name == 'router_3' then
	port = 3335
	zone = 3
else
	port = 3336
	zone = 4
end
cfg.zone = zone

box.cfg{listen = port}

function wait_state(state)
	log.info(string.rep('a', 1000))
	while test_run:grep_log(name, state, 1000) == nil do
		fiber.sleep(0.1)
	end
end

function priority_order()
	local ret = {}
	for _, uuid in pairs(rs_uuid) do
		local rs = vshard.router.static.replicasets[uuid]
		local sorted = {}
		for _, replica in pairs(rs.priority_list) do
			local z
			if replica.zone == nil then
				z = 'unknown zone'
			else
				z = replica.zone
			end
			table.insert(sorted, z)
		end
		table.insert(ret, sorted)
	end
	return ret
end

require('console').listen(os.getenv('ADMIN'))
