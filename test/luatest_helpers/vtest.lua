local helpers = require('test.luatest_helpers')
local cluster = require('test.luatest_helpers.cluster')

local uuid_idx = 1

--
-- New UUID unique per this process. Generation is not random - for simplicity
-- and reproducibility.
--
local function uuid_next()
    local last = tostring(uuid_idx)
    uuid_idx = uuid_idx + 1
    assert(#last <= 12)
    return '00000000-0000-0000-0000-'..string.rep('0', 12 - #last)..last
end

--
-- Build a valid vshard config by a template. A template does not specify
-- anything volatile such as URIs, UUIDs - these are installed at runtime.
--
local function config_new(templ)
    local res = table.deepcopy(templ)
    local sharding = {}
    res.sharding = sharding
    for _, replicaset_templ in pairs(templ.sharding) do
        local replicaset_uuid = uuid_next()
        local replicas = {}
        local replicaset = table.deepcopy(replicaset_templ)
        replicaset.replicas = replicas
        for replica_name, replica_templ in pairs(replicaset_templ.replicas) do
            local replica_uuid = uuid_next()
            local replica = table.deepcopy(replica_templ)
            replica.name = replica_name
            replica.uri = 'storage:storage@'..helpers.instance_uri(replica_name)
            replicas[replica_uuid] = replica
        end
        sharding[replicaset_uuid] = replicaset
    end
    return res
end

--
-- Build new cluster by a given config.
--
local function storage_new(g, cfg)
    if not g.cluster then
        g.cluster = cluster:new({})
    end
    local all_servers = {}
    local masters = {}
    local replicas = {}
    for replicaset_uuid, replicaset in pairs(cfg.sharding) do
        -- Luatest depends on box.cfg being ready and listening. Need to
        -- configure it before vshard.storage.cfg().
        local box_repl = {}
        for _, replica in pairs(replicaset.replicas) do
            table.insert(box_repl, replica.uri)
        end
        local box_cfg = {
            replication = box_repl,
            -- Speed retries up.
            replication_timeout = 0.1,
        }
        for replica_uuid, replica in pairs(replicaset.replicas) do
            local name = replica.name
            box_cfg.instance_uuid = replica_uuid
            box_cfg.replicaset_uuid = replicaset_uuid
            box_cfg.listen = helpers.instance_uri(replica.name)
            -- Need to specify read-only explicitly to know how is master.
            box_cfg.read_only = not replica.master
            local server = g.cluster:build_server({
                alias = name,
                box_cfg = box_cfg,
            }, 'storage.lua')
            g[name] = server
            g.cluster:add_server(server)

            table.insert(all_servers, server)
            if replica.master then
                table.insert(masters, server)
            else
                table.insert(replicas, server)
            end
        end
    end
    for _, replica in pairs(all_servers) do
        replica:start({wait_for_readiness = false})
    end
    for _, master in pairs(masters) do
        master:wait_for_readiness()
        master:exec(function(cfg)
            -- Logged in as guest with 'super' access rights. Yet 'super' is not
            -- enough to grant 'replication' privilege. The simplest way - login
            -- as admin for that temporary.
            local user = box.session.user()
            box.session.su('admin')

            vshard.storage.cfg(cfg, box.info.uuid)
            box.schema.user.grant('storage', 'super')

            box.session.su(user)
        end, {cfg})
    end
    for _, replica in pairs(replicas) do
        replica:wait_for_readiness()
        replica:exec(function(cfg)
            vshard.storage.cfg(cfg, box.info.uuid)
        end, {cfg})
    end
end

--
-- Create a new router in the cluster.
--
local function router_new(g, name, cfg)
    if not g.cluster then
        g.cluster = cluster:new({})
    end
    local server = g.cluster:build_server({
        alias = name,
    }, 'router.lua')
    g[name] = server
    g.cluster:add_server(server)
    server:start()
    server:exec(function(cfg)
        vshard.router.cfg(cfg)
    end, {cfg})
    return server
end

return {
    config_new = config_new,
    storage_new = storage_new,
    router_new = router_new,
}
