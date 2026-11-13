local argparse = require('internal.argparse')
local uuid = require('uuid')
local consts = require('vshard.consts')

local defaults = {
    replicasets = 2,
    instances_per_replicaset = 1,
    routers = 1,
    bucket_count = consts.DEFAULT_BUCKET_COUNT,
    generators = 1,
    payload_bytes = 128,
}

local function config_parse(args)
    local options = {
        {'output-dir', 'string'},
        {'diagnostics', 'boolean'},
        {'help', 'boolean'},
    }
    for key in pairs(defaults) do
        table.insert(options, {key:gsub('_', '-'), 'number'})
    end
    local opts = argparse.parse(args, options)
    assert(#opts == 0, 'Unexpected positional arguments')
    local cfg = {
        fibers_per_generator = 256,
        cases = {'callrw_noop', 'callro_get', 'callrw_replace'},
        rows = 100000,
        seed = 1,
        warmup_seconds = 5,
        duration_seconds = 15,
        base_port = 13300,
        startup_timeout_seconds = 60,
        rpc_timeout_seconds = 5,
    }
    for key, default in pairs(defaults) do
        local value = opts[key:gsub('_', '-')] or default
        assert(value >= 0 and value < math.huge and value == math.floor(value),
               'Invalid ' .. key)
        assert(value > 0 or key == 'payload_bytes', key .. ' must be positive')
        cfg[key] = value
    end
    assert(cfg.bucket_count >= cfg.replicasets and cfg.bucket_count <= cfg.rows,
           'Invalid bucket_count')
    local node_count = cfg.replicasets * cfg.instances_per_replicaset +
                       cfg.routers + cfg.generators
    assert(cfg.base_port + node_count - 1 <= 65535,
           'The topology exceeds the available port range')
    return opts, cfg
end

local function config_topology(cfg)
    local nodes = {}
    local function add(node)
        node.uri = ('127.0.0.1:%d'):format(cfg.base_port + #nodes)
        table.insert(nodes, node)
    end
    for rs = 1, cfg.replicasets do
        local rs_uuid = uuid.str()
        for instance = 1, cfg.instances_per_replicaset do
            add({
                name = ('storage_%d_%d'):format(rs, instance),
                role = 'storage',
                rs = rs,
                master = instance == 1,
                uuid = uuid.str(),
                rs_uuid = rs_uuid,
            })
        end
    end
    for router = 1, cfg.routers do
        add({name = 'router_' .. router, role = 'router'})
    end
    for generator = 1, cfg.generators do
        add({
            name = 'generator_' .. generator,
            role = 'generator',
            index = generator,
        })
    end
    return nodes
end

local function config_sharding(cfg, nodes)
    local result = {
        bucket_count = cfg.bucket_count,
        rebalancer_mode = 'off',
        sharding = {},
    }
    for _, node in ipairs(nodes) do
        if node.role == 'storage' then
            local rs = result.sharding[node.rs_uuid]
            if not rs then
                rs = {replicas = {}}
                result.sharding[node.rs_uuid] = rs
            end
            rs.replicas[node.uuid] = {
                uri = 'storage:storage@' .. node.uri,
                name = node.name,
                master = node.master,
            }
        end
    end
    return result
end

return {
    parse = config_parse,
    topology = config_topology,
    sharding = config_sharding,
}
