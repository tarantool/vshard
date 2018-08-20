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

function create_simple_space(...)
	local s = box.schema.create_space(...)
	s:create_index('pk')
	s:create_index(cfg.shard_index or 'bucket_id',
		       {parts = {{2, 'unsigned'}}, unique = false})
end

finish_refs = false
function make_ref() while not finish_refs do fiber.sleep(0.01) end end

require('storage_template')
