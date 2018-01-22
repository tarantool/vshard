#!/usr/bin/env tarantool
-- Get instance name
require('strict').on()
local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')
local log = require('log')
require('console').listen(os.getenv('ADMIN'))
fiber = require('fiber')

echo_count = 0

vshard = require('vshard')
names = require('names')
cfg = require('config')
if NAME == 'box_3_a' or NAME == 'box_3_b' then
	add_replicaset()
end
vshard.storage.cfg(cfg, names.replica_uuid[NAME])

function init_schema()
	local format = {}
	format[1] = {name = 'field', type = 'unsigned'}
	format[2] = {name = 'bucket_id', type = 'unsigned'}
	local s = box.schema.create_space('test', {format = format})
	local pk = s:create_index('pk')
	local bucket_id_idx =
		s:create_index('bucket_id', {parts = {'bucket_id'},
					     unique = false})
end

box.once('schema', function()
	box.schema.func.create('do_replace')
	box.schema.role.grant('public', 'execute', 'function', 'do_replace')
	box.schema.func.create('do_select')
	box.schema.role.grant('public', 'execute', 'function', 'do_select')
	init_schema()
end)

function do_replace(...)
	box.space.test:replace(...)
	return true
end

function do_select(...)
	return box.space.test:select(...)
end

function check_consistency()
	for _, tuple in box.space.test:pairs() do
		assert(box.space._bucket:get{tuple.bucket_id})
	end
	return true
end

function switch_rs1_master()
	local replica_uuid = names.replica_uuid
	local rs_uuid = names.rs_uuid
	cfg.sharding[rs_uuid[1]].replicas[replica_uuid.box_1_a].master = nil
	cfg.sharding[rs_uuid[1]].replicas[replica_uuid.box_1_b].master = true
end

function nullify_rs_weight()
	cfg.sharding[names.rs_uuid[1]].weight = 0
end

function wait_rebalancer_state(state, test_run)
	log.info(string.rep('a', 1000))
	vshard.storage.rebalancer_wakeup()
	while not test_run:grep_log(NAME, state, 1000) do
		fiber.sleep(0.1)
		vshard.storage.rebalancer_wakeup()
	end
end
