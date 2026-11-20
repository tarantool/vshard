local uuid = require('uuid')

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
    topology = config_topology,
    sharding = config_sharding,
}
