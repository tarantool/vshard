local clock = require('clock')
local log = require('log')
local net_box = require('net.box')
local fiber = require('fiber')

local USAGELINE = [[

 Usage: tarantool generate_load.lua [options]

]]

local HELP = [[
   bucket_count <number, 3000>   - number of buckets in cluster
   fibers <number, 50>           - number of fibers to run simultaneously
   help (same as -h)             - print this message
   ops <number, 10000000>        - total amount of operations to be performed
   op_type <string, replace>     - which operation is used
   uri <string, localhost:3305>  - uri of the router
   warmup <number, bucket_count> - percent of ops to skip before measurement
]]

local parsed_params = {
    {'bucket_count', 'number'},
    {'fibers', 'number'},
    {'ops', 'number'},
    {'op_type', 'string'},
    {'uri', 'string'},
    {'warmup', 'number'},
    {'h', 'boolean'},
    {'help', 'boolean'},
}

--------------------------------------------------------------------------------
-- Parse command line arguments
--------------------------------------------------------------------------------

local params = require('internal.argparse').parse(arg, parsed_params)
if params.h or params.help then
    print(USAGELINE .. HELP)
    os.exit(0)
end

-- Default values.
local bucket_count = params.bucket_count or 3000
local fibers_num = params.fibers or 50
local ops_num = params.ops or 1000000
local op_type = params.op_type or 'replace'
local uri = params.uri or 'localhost:3305'
local warmup = params.warmup or bucket_count

--------------------------------------------------------------------------------
-- Connect to instances.
--------------------------------------------------------------------------------

--
-- <instance> = {
--     uri = URI,
--     conn = netbox,
--     stats = {
--         latency_sum = number,
--         error_num = number,
--     },
-- }
--
local instances = {
    -- Currently one is enough, more in future.
    router_1 = {
        uri = uri,
        stats = {
            latency_sum = 0,
            error_num = 0,
        },
    },
}

local function nb_connect(uri)
    local conn_opts = {reconnect_after = 0.5, wait_connected = 10}
    local c = net_box.connect(uri, conn_opts)
    local is_connected = c:wait_connected(30)
    if not is_connected then
        error('Could not connect to the router %s', instance.uri)
    end
    return c
end

for _, instance in pairs(instances) do
    instance.conn = nb_connect(instance.uri)
end

-- TODO: make it more general.
local function vshard_op_func(op, conn)
    local func_name, request_type, tuple
    if op == 'replace' then
        request_type = 'vshard.router.callrw'
        func_name = 'box.space.customer:replace'
    elseif op == 'get' then
        request_type = 'vshard.router.callro'
        func_name = 'box.space.customer:get'
    else
        error('Unknown operation')
    end

    return function(bucket_id)
        tuple = op == 'replace'
                and {{bucket_id, bucket_id, 'name'}} or {{bucket_id}}
        return conn:call(request_type, {bucket_id, func_name, tuple})
    end
end

--------------------------------------------------------------------------------
-- Warmup
--------------------------------------------------------------------------------

for _, instance in pairs(instances) do
    for bid = 1, warmup do
        local warmup_func = vshard_op_func('replace', instance.conn)
        warmup_func(bid)
    end
end

--------------------------------------------------------------------------------
-- Load fibers.
--------------------------------------------------------------------------------

local ops_per_fiber = ops_num / fibers_num
local buckets_per_fiber = bucket_count / fibers_num

-- Start timer.
local timer_begin = {
    clock.time(),
    clock.proc()
}

local function fiber_load(instance, start)
    local c = nb_connect(instance.uri)
    local op = vshard_op_func(op_type, c)
    local bid = start
    for _ = 1, ops_per_fiber do
        local start_ts = clock.time()
        local _, err = op(bid)
        local latency = clock.time() - start_ts
        bid = (bid + 1) % bucket_count
        if err then
            log.warn(err)
            instance.stats.error_num = instance.stats.error_num + 1
        else
            instance.stats.latency_sum = instance.stats.latency_sum + latency
        end
    end
end

local fibers_storage = {}
for i = 1, fibers_num do
    local f = fiber.create(fiber_load, instances.router_1,
                           (i - 1) * buckets_per_fiber)
    f:set_joinable(true)
    fibers_storage[i] = f
end

for i = 1, fibers_num do
    fibers_storage[i]:join()
end

--------------------------------------------------------------------------------
-- Results.
--------------------------------------------------------------------------------

local real_time = clock.time() - timer_begin[1]
local cpu_time = clock.proc() - timer_begin[2]
local ops_done = ops_num
for _, instance in pairs(instances) do
    ops_done = ops_done - instance.stats.error_num
end

log.info('# cluster done %d ops in time: %f, cpu: %f',
         ops_done, real_time, cpu_time)
log.info('# cluster average speed: %f', ops_done / real_time)
for name, instance in pairs(instances) do
    local average_latency = instance.stats.latency_sum / ops_done
    log.info('# average latency on %s: %f', name,  average_latency)
end
