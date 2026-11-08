local root = arg[3]
package.path = root .. '/?.lua;' .. root .. '/?/init.lua;' .. package.path

local fio = require('fio')
local util = require('perf.lib.util')
local spec = util.read_json(arg[1])
local node = assert(spec.nodes[tonumber(arg[2])], 'Unknown node index')
local cfg, directory = spec.config, fio.dirname(arg[1])
local api = {}
_G.perf = api

local sharding = node.role == 'generator' and {} or spec.sharding
sharding.work_dir = fio.pathjoin(directory, node.name)
sharding.listen = node.uri
sharding.log = fio.pathjoin(directory, node.name .. '.log')

if node.role == 'storage' then
    local vutil = require('vshard.util')
    local master = spec.nodes[(node.rs - 1) * cfg.instances_per_replicaset + 1]
    sharding.replication = node.master and {} or {
        'storage:storage@' .. master.uri,
    }
    if vutil.version_is_at_least(3, 0, 0, nil, 0, 0) then
        sharding.bootstrap_strategy = 'config'
        sharding.bootstrap_leader = master.uuid
    else
        -- The master is already running; each replica has one upstream.
        sharding.bootstrap_strategy = 'legacy'
    end
    local storage = require('vshard.storage')
    _G.vshard = {storage = storage}
    storage.cfg(sharding, node.uuid)
    if node.master then
        box.schema.user.grant('guest', 'super')
        local space = box.schema.space.create('perf_data', {
            format = {
                {'id', 'unsigned'},
                {'bucket_id', 'unsigned'},
                {'payload', 'string'},
            },
        })
        space:create_index('pk')
        space:create_index('bucket_id', {
            parts = {'bucket_id'},
            unique = false,
        })
    end

    local function noop()
        return true
    end

    local function preload()
        local payload = string.rep('x', cfg.payload_bytes)
        local buckets = storage.buckets_discovery()
        for _, bid in ipairs(buckets) do
            for key = bid, cfg.rows, cfg.bucket_count do
                box.space.perf_data:replace{key, bid, payload}
            end
        end
        return {
            rows = box.space.perf_data:count(),
            buckets = storage.buckets_count(),
        }
    end

    api.noop = noop
    api.preload = preload
elseif node.role == 'router' then
    local router = require('vshard.router')
    _G.vshard = {router = router}
    router.cfg(sharding)
    box.schema.user.grant('guest', 'super')
elseif node.role == 'generator' then
    box.cfg(sharding)
    box.schema.user.grant('guest', 'super')
    local net_box = require('net.box')
    local workload = require('perf.lib.workload')
    local routers = {}
    for _, target in ipairs(spec.nodes) do
        if target.role == 'router' then
            local connection = net_box.connect(target.uri, {
                wait_connected = cfg.startup_timeout_seconds,
            })
            assert(connection:is_connected(),
                   'Cannot connect to ' .. target.name .. ': ' ..
                   tostring(connection.error))
            table.insert(routers, connection)
        end
    end

    local function run(case, start_at, duration)
        return workload.run(routers, cfg, case, node.index, start_at, duration)
    end

    api.run = run
else
    error('Unknown instance role: ' .. tostring(node.role))
end

local function ready()
    return {name = node.name}
end

api.ready = ready
