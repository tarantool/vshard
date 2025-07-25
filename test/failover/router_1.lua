#!/usr/bin/env tarantool

require('strict').on()
test_run = require('test_run').new()

local fio = require('fio')
local name = fio.basename(arg[0], '.lua')
cfg = dofile('config.lua')
vshard = require('vshard')
os = require('os')
fiber = require('fiber')
local names = dofile('names.lua')
log = require('log')
rs_uuid = names.rs_uuid
replica_uuid = names.replica_uuid
zone = nil
if name == 'router_1' then
	zone = 1
elseif name == 'router_2' then
	zone = 2
elseif name == 'router_3' then
	zone = 3
else
	zone = 4
end
cfg.zone = zone

box.cfg{}

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

function failover_wakeup(router)
    local router = router or vshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.worker:wakeup_service('replicaset_failover')
        for _, r in pairs(rs.replicas) do
            r.worker:wakeup_service('replica_failover')
        end
    end
end

require('console').listen(os.getenv('ADMIN'))
