#!/usr/bin/env tarantool

local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')

test_run = require('test_run').new()
require('console').listen(os.getenv('ADMIN'))

bad_uuid_config = require('bad_uuid_config')
shard_cfg = bad_uuid_config.cfg
replicaset_uuid = bad_uuid_config.replicaset_uuid
name_to_uuid = bad_uuid_config.name_to_uuid
vshard = require('vshard')

if NAME == 'bad_uuid_2_a' then
	local rs2 = shard_cfg.sharding[replicaset_uuid[2]]
	local uuid = name_to_uuid.bad_uuid_2_a
	local bad_uuid_2_a = rs2.replicas[uuid]
	rs2.replicas[uuid] = nil
	-- Change UUID on a single server. Other replicas do not
	-- see this change.
	uuid = '2d92ae8a-afc0-4e91-ba34-843a356b8ed7'
	rs2.replicas[uuid] = bad_uuid_2_a
	name_to_uuid.bad_uuid_2_a = uuid
end
if NAME == 'bad_uuid_2_a_repaired' then
	NAME = 'bad_uuid_2_a'
end
vshard.storage.cfg(shard_cfg, name_to_uuid[NAME])
