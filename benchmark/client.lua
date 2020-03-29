local common = require('common')
local netbox = require('net.box')
local fiber = require('fiber')
local clock = require('clock')
local log = require('log')
local params = common.client_params
vshard = require('vshard')

log.info('Start bench client')

assert(params.worker_count ~= nil)
assert(params.load ~= nil)
assert(params.is_direct ~= nil)
assert(params.arg_size ~= nil)
assert(params.arg_count ~= nil)
assert(params.timeout ~= nil)

if params.load == 'select' then
    assert(params.select_limit ~= nil)
end

bucket_id = 1
local bucket_count = common.config.bucket_count
local argument = string.rep('a', params.arg_size)
local arguments = {}
for i = 1, params.arg_count do
    table.insert(arguments, argument)
end
local select_limit = {params.select_limit}

local function client_bench_next_bid()
    local id = bucket_id
    bucket_id = bucket_id + 1
    if bucket_id > bucket_count then
        bucket_id = 1
    end
    return id
end

client_next_conn = nil
client_first_conn = nil

local function client_bench_next_conn()
    local conn = client_next_conn
    client_next_conn = client_next_conn.next
    if client_next_conn == nil then
        client_next_conn = client_first_conn
    end
    return conn
end

local client_bench_call
local timeout = {timeout = params.timeout}

if params.is_direct then
    log.info('The client will go to storages directly')
    vshard.router.cfg(common.config)
    vshard.router.bootstrap({if_not_bootstrapped = true})

    client_bench_call = function(func, args)
        return vshard.router.callrw(client_bench_next_bid(), func, args,
                                    timeout)
    end
else
    log.info('The client will go to storages though third routers')
    box.cfg{}

    local connect_count = 0
    local routers = common.router_listen
    assert(params.connect_count_per_router)
    for i = 1, params.connect_count_per_router do
        router_count = 0
        for _, router in pairs(routers) do
            local conn = netbox.connect(router)
            if not client_first_conn then
                client_first_conn = conn
            else
                client_next_conn.next = conn
            end
            client_next_conn = conn
            connect_count = connect_count + 1
        end
    end
    log.info('%d connections to %d routers are established', connect_count,
             connect_count / params.connect_count_per_router)
    client_bench_call = function(func, args)
        local conn = client_bench_next_conn()
        return conn:call('vshard.router.callrw',
                         {client_bench_next_bid(), func, arg, timeout})
    end
end

local function client_bench_yield()
    return client_bench_call('bench_call_yield')
end

local function client_bench_echo()
    return client_bench_call('bench_call_echo', arguments)
end

local function client_bench_select()
    return client_bench_call('bench_call_select', select_limit)
end

local function client_bench_random()
    return client_bench_call('bench_call_random')
end

local function client_bench_yield()
    return client_bench_call('bench_call_yield')
end

local bench_func = {
    yield = client_bench_yield,
    echo = client_bench_echo,
    select = client_bench_select,
    random = client_bench_random,
}
bench_func = bench_func[params.load]

success_count = 0
total_success_count = 0
worker_table = {}
workers = {}
rate_fiber = nil

-- Print rate_meter variable to see request count per second.
rate_meter = common.create_rate_meter()

local function rate_f()
    while true do
        fiber.sleep(0.1)
        rate_meter:feed(success_count)
        success_count = 0
    end
end

local function client_worker_ff(id)
    while true do
        bench_func()
        if not worker_table[id] then
            worker_table[id] = 0
        else
            worker_table[id] = worker_table[id] + 1
        end
        success_count = success_count + 1
        total_success_count = total_success_count + 1
        fiber.yield()
    end
end

local function client_worker_f(id)
    while true do
        pcall(client_worker_ff, id)
        fiber.yield()
    end
end

local start_ts

function client_restart_load()
    log.info('Start load with %d workers', params.worker_count)
    worker_table = {}
    start_ts = clock.monotonic64()
    for i = 1, params.worker_count do
        if workers[i] then
            workers[i]:cancel()
        end
        workers[i] = fiber.new(client_worker_ff, i)
    end
    if rate_fiber then
        rate_fiber:cancel()
    end
    rate_fiber = fiber.new(rate_f)
    fiber.yield()
end

-- Function to check which workers work much more than the others
-- somewhy. Helps to debug if most of the workers died, or stuck.
function top_workers(limit)
    local sorted = {}
    for k, v in pairs(worker_table) do
        table.insert(sorted, {k, v})
    end
    table.sort(sorted, function(a, b) return a[2] > b[2] end)
    local res = {}
    local count = 0
    for _, v in pairs(sorted) do
        if count >= limit then
            break
        end
        table.insert(res, v)
        count = count + 1
    end
    return res
end
