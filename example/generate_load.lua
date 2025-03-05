local log = require('log')
local net_box = require('net.box')
local fiber = require('fiber')

local BUCKET_COUNT = 3000

local function vshard_operations(instance, operation, count)
    if count <= 0 then
        return
    end

    local request_type
    if operation == 'CALLRW_ECHO' then
        request_type = 'vshard.router.callrw'
    elseif operation == 'CALLRO_ECHO' then
        request_type = 'vshard.router.callro'
    elseif operation == 'CALLBRO_ECHO' then
        request_type = 'vshard.router.callbro'
    elseif operation == 'CALLRE_ECHO' then
        request_type = 'vshard.router.callre'
    elseif operation == 'CALLBRE_ECHO' then
        request_type = 'vshard.router.callbre'
    end

    if not request_type then
        error('Unknown request type')
    end

    for _ = 1, count do
        instance.net_box:call(request_type,
            {math.random(1, BUCKET_COUNT), 'echo', 'hello'})
    end
end

local function generate_vshard_load(instance)
    local vshard_load = {}
    vshard_load['CALLRW_ECHO'] = math.random(1, 10)
    vshard_load['CALLRO_ECHO'] = math.random(1, 10)
    vshard_load['CALLBRO_ECHO'] = math.random(1, 10)
    vshard_load['CALLRE_ECHO'] = math.random(1, 10)
    vshard_load['CALLBRE_ECHO'] = math.random(1, 10)

    for operation, count in pairs(vshard_load) do
        vshard_operations(instance, operation, count)
    end
end

local load_generators = {
    generate_vshard_load,
}

local instances = {
    ['router'] = {
        advertise_uri = 'localhost:3305',
    },
}

for _, instance in pairs(instances) do
    instance.net_box = net_box.connect(instance.advertise_uri,
        {reconnect_after = 5, wait_connected = 10})
end

while true do
    for name, instance in pairs(instances) do
        for _, load_generator in ipairs(load_generators) do
            local _, err = pcall(load_generator, instance)
            if err ~= nil then
                log.error(err)
                fiber.sleep(1)
            end
            fiber.yield()
        end
    end
end
