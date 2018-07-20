local log = require('log')
local fiber = require('fiber')

function init_schema()
	local format = {}
	format[1] = {name = 'field', type = 'unsigned'}
	format[2] = {name = 'bucket_id', type = 'unsigned'}
	local s = box.schema.create_space('test', {format = format})
	local pk = s:create_index('pk')
	local bucket_id_idx =
		s:create_index('vbucket', {parts = {'bucket_id'},
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

--
-- Wait a specified log message.
-- Requirements:
-- * Should be executed from a storage with a rebalancer.
-- * NAME - global variable, name of instance should be set.
function wait_rebalancer_state(state, test_run)
	log.info(string.rep('a', 1000))
	vshard.storage.rebalancer_wakeup()
	while not test_run:grep_log(NAME, state, 1000) do
		fiber.sleep(0.1)
		vshard.storage.rebalancer_wakeup()
	end
end
