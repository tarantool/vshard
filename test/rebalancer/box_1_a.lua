#!/usr/bin/env tarantool
NAME = require('fio').basename(arg[0], '.lua')
cfg = require('config')
util = require('util')
if NAME == 'box_3_a' or NAME == 'box_3_b' or
   NAME == 'box_4_a' or NAME == 'box_4_b' or
   string.match(NAME, 'fullbox') then
	add_replicaset()
end
if NAME == 'box_4_a' or NAME == 'box_4_b' or
   string.match(NAME, 'fullbox') then
	add_second_replicaset()
end

function switch_rs1_master()
	cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.box_1_a].master = nil
	cfg.sharding[util.replicasets[1]].replicas[util.name_to_uuid.box_1_b].master = true
end

function nullify_rs_weight()
	cfg.sharding[util.replicasets[1]].weight = 0
end

require('storage_template')
