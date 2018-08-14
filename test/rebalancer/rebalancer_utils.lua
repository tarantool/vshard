local write_iterations = 0
local read_iterations = 0
local write_fiber = 'none'
local read_fiber = 'none'
local bucket_count = 200

local log = require('log')

local function do_write_load()
	while true do
		local bucket = write_iterations % bucket_count + 1
		while not vshard.router.call(bucket, 'write', 'do_replace',
					     {{write_iterations, bucket}}) do
		end
		write_iterations = write_iterations + 1
		fiber.sleep(0.05)
	end
end

local function do_read_load()
	while true do
		while read_iterations == write_iterations do
			fiber.sleep(0.1)
		end
		local bucket = read_iterations % bucket_count + 1
		local tuples = {}
		local err = nil
		-- Read requests are repeated on replicaset level,
		-- and here while loop is not necessary.
		while #tuples == 0 do
			tuples, err =
				vshard.router.call(bucket, 'read', 'do_select',
						   {{read_iterations}},
						   {timeout = 100})
			if not tuples then
				log.info('Error during read loading: %s', err)
				tuples = {}
			end
		end
		assert(tuples[1][1] == read_iterations)
		assert(tuples[1][2] == bucket)
		read_iterations = read_iterations + 1
	end
end

local function stop_loading()
	write_fiber:cancel()
	assert(write_iterations > 0)
	while write_iterations ~= read_iterations do fiber.sleep(0.1) end
	read_fiber:cancel()
end

local function start_loading()
	write_iterations = 0
	read_iterations = 0
	read_fiber = fiber.create(do_read_load)
	write_fiber = fiber.create(do_write_load)
end

local function check_loading_result()
	for i = 0, write_iterations do
		local bucket = i % bucket_count + 1
		local tuples, err =
			vshard.router.call(bucket, 'read', 'do_select', {{i}},
					   {timeout = 100})
		if (not tuples or #tuples ~= 1) and i ~= write_iterations then
			return i, err
		end
	end
	return true
end

return {
	stop_loading = stop_loading,
	start_loading = start_loading,
	check_loading_result = check_loading_result,
}
