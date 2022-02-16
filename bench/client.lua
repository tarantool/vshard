local netbox = require('net.box')
local log = require('log')
local yaml = require('yaml')
local clock = require('clock')
local fiber = require('fiber')
local msgpack = require('msgpack')

local arg_big_json = require('json').decode([[
{
    "random": 100,
    "random float": 25.498,
    "bool": true,
    "date": "1992-04-24",
    "regEx": "hellooooooooooooooooooooooooooooo world",
    "enum": "json",
    "firstname": "Elsie",
    "lastname": "Elsinore",
    "city": "Quetzaltenango",
    "country": "Holy See (Vatican City State)",
    "countryCode": "MG",
    "email uses current data": "Elsie.Elsinore@gmail.com",
    "email from expression": "Elsie.Elsinore@yopmail.com",
    "array": [
        "Iseabal",
        "Siana",
        "Dianemarie",
        "Sharai",
        "Teddie"
    ],
    "array of objects": [
        {
            "index": 0,
            "index start at 5": 5
        },
        {
            "index": 1,
            "index start at 5": 6
        },
        {
            "index": 2,
            "index start at 5": 7
        }
    ],
    "Lacie": {
        "age": 22
    }
}
]])
local arg_big_number = 11352534231

local func = 'echo_normal'
local bucket_id = 1
local arg_count = 4
local arg_depth = 1
local arg_value = arg_big_json
local args_are_one_array = true
local client_count = 100
local request_count = 4000
local host = '127.0.0.1:3300'

log.info('Start %s clients', client_count)
local clients = {}
for i = 1, client_count do
    table.insert(clients, netbox.connect(host, {wait_connected = false}))
end
for _, c in pairs(clients) do
    c:wait_connected()
end

log.info('Create arguments')
local storage_args = {}
for i = 1, arg_count do
    local arg = arg_value
    for j = 2, arg_depth do
        arg = {arg}
    end
    table.insert(storage_args, arg)
end
if args_are_one_array then
    storage_args = {storage_args}
end
log.info('Arguments to use:')
log.info(yaml.encode(storage_args))

local args = msgpack.object({bucket_id, storage_args})

local total_request_count = client_count * request_count
log.info('Send %s requests', total_request_count)
local opts = {is_async = true, return_raw = true}
local futures = table.new(total_request_count, 0)
local idx = 1
for _, c in pairs(clients) do
    for i = 1, request_count do
        futures[idx] = c:call(func, args, opts)
        idx = idx + 1
        if idx % 100 == 0 then
            fiber.yield()
        end
    end
end

log.info('Wait responses')
local first_res
local ts1 = clock.monotonic64()
for _, f in pairs(futures) do
    local res, err = f:wait_result()
    if not res then
        log.info(require('yaml').encode({res, err}))
        assert(false)
    end
    if not first_res then
        first_res = res
    end
    assert(not err)
end
local ts2 = clock.monotonic64()

log.info('Received:')
log.info(yaml.encode(first_res:decode()))
log.info('Finished in %s sec', tonumber(ts2 - ts1) / 1000000000)

os.exit(0)
