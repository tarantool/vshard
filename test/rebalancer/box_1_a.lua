#!/usr/bin/env tarantool
-- Get instance name
require('strict').on()
local fio = require('fio')
NAME = fio.basename(arg[0], '.lua')
log = require('log')
require('console').listen(os.getenv('ADMIN'))
fiber = require('fiber')

echo_count = 0

vshard = require('vshard')
names = require('names')
cfg = require('config')
if NAME == 'box_3_a' or NAME == 'box_3_b' or
   NAME == 'box_4_a' or NAME == 'box_4_b' or
   string.match(NAME, 'fullbox') then
	add_replicaset()
end
if NAME == 'box_4_a' or NAME == 'box_4_b' or
   string.match(NAME, 'fullbox') then
	add_second_replicaset()
end
vshard.storage.cfg(cfg, names.replica_uuid[NAME])

-- Bootstrap storage.
require('lua_libs.bootstrap')

function switch_rs1_master()
	local replica_uuid = names.replica_uuid
	local rs_uuid = names.rs_uuid
	cfg.sharding[rs_uuid[1]].replicas[replica_uuid.box_1_a].master = nil
	cfg.sharding[rs_uuid[1]].replicas[replica_uuid.box_1_b].master = true
end

function nullify_rs_weight()
	cfg.sharding[names.rs_uuid[1]].weight = 0
end
