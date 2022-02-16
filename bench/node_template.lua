local msgpack = require('msgpack')
local cfg = require('config')
local name = require('fio').basename(arg[0])

cfg.replication_connect_quorum = 0

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

local select_size = 10
local tuple_size = 1
local field_value = arg_big_json
local field_depth = 1

local select_result = {}
for i = 1, select_size do
    local tuple = {}
    for j = 1, tuple_size do
        local field = field_value
        for j = 2, field_depth do
            field = {field}
        end
        table.insert(tuple, field)
    end
    table.insert(select_result, tuple)
end
local select_raw_result = msgpack.object(select_result)

local instance_uuid
for _, rs in pairs(cfg.sharding) do
    for uuid, rep in pairs(rs.replicas) do
        if rep.name == name then
            instance_uuid = uuid
            break
        end
    end
    if instance_uuid then
        break
    end
end

vshard = require('vshard')
vshard.storage.cfg(cfg, instance_uuid)
if not box.cfg.read_only then
    box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
end

echo_count = 0
select_count = 0

function echo(...)
    echo_count = echo_count + 1
    return ...
end

function select_some()
    select_count = select_count + 1
    return select_raw_result
end
