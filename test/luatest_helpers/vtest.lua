local t = require('luatest')
local helpers = require('test.luatest_helpers')
local cluster = require('test.luatest_helpers.cluster')
local vconst = require('vshard.consts')

local uuid_idx = 1
--
-- The maps help to preserve the same UUID for replicas and replicasets during
-- reconfiguration. Reconfig means an update of a cfg template which doesn't
-- contain UUIDs + generation of a new real cfg to apply on nodes. The real cfg
-- needs to have same UUIDs for the nodes used in the old versions of the
-- template.
--
local replica_name_to_uuid_map = {}
local replicaset_name_to_uuid_map = {}

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

local function name_to_uuid(map, name)
    local res = map[name]
    if not res then
        res = uuid_next()
        map[name] = res
    end
    return res
end

local function replica_name_to_uuid(name)
    return name_to_uuid(replica_name_to_uuid_map, name)
end

local function replicaset_name_to_uuid(name)
    return name_to_uuid(replicaset_name_to_uuid_map, name)
end

--
-- Build a valid vshard config by a template. A template does not specify
-- anything volatile such as URIs, UUIDs - these are installed at runtime.
--
local function config_new(templ)
    local res = table.deepcopy(templ)
    local sharding = {}
    res.sharding = sharding
    -- Is supposed to intensify reconnects when replication and listen URIs
    -- change.
    res.replication_timeout = 0.1
    for i, replicaset_templ in pairs(templ.sharding) do
        local replicaset_uuid = replicaset_name_to_uuid(i)
        local replicas = {}
        local replicaset = table.deepcopy(replicaset_templ)
        replicaset.replicas = replicas
        for replica_name, replica_templ in pairs(replicaset_templ.replicas) do
            local replica_uuid = replica_name_to_uuid(replica_name)
            local replica = table.deepcopy(replica_templ)
            replica.port_uri = nil
            replica.port_count = nil
            replica.name = replica_name

            local port_count = replica_templ.port_count
            local creds = 'storage:storage@'
            if port_count == nil then
                replica.uri = creds..helpers.instance_uri(replica_name)
            else
                local listen = table.new(port_count, 0)
                for j = 1, port_count do
                    listen[j] = creds..helpers.instance_uri(replica_name..j)
                end
                replica.listen = listen
                replica.uri = listen[replica_templ.port_uri]
            end
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
            -- VShard specific details to use in various helper functions.
            server.vtest = {
                name = name,
                is_storage = true,
            }
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
-- Apply the config to all vshard storages in the cluster.
--
local function storage_cfg(g, cfg)
    -- No support yet for dynamic node addition and removal. Only reconfig.
    local fids = {}
    local storages = {}
    -- Map-reduce. It should make reconfig not only faster but also not depend
    -- on which order would be non-blocking. For example, there might be a
    -- config which makes the master hang until some replica is configured
    -- first. When all are done in parallel, it won't matter.
    for _, storage in pairs(g.cluster.servers) do
        if storage.vtest and storage.vtest.is_storage then
            table.insert(storages, storage)
            table.insert(fids, storage:exec(function(cfg)
                local f = fiber.new(vshard.storage.cfg, cfg, box.info.uuid)
                f:set_joinable(true)
                return f:id()
            end, {cfg}))
        end
    end
    local errors = {}
    for i, storage in pairs(storages) do
        local ok, err = storage:exec(function(fid)
            return fiber.find(fid):join()
        end, {fids[i]})
        if not ok then
            errors[storage.vtest.name] = err
        end
    end
    t.assert_equals(errors, {}, 'storage reconfig')
end

--
-- Find first active bucket on the storage. In tests it helps not to assume
-- where the buckets are located by hardcoded numbers and uuids.
--
local function storage_first_bucket(storage)
    return storage:exec(function(status)
        local res = box.space._bucket.index.status:min(status)
        return res ~= nil and res.id or nil
    end, {vconst.BUCKET.ACTIVE})
end

--
-- Apply the config on the given router.
--
local function router_cfg(router, cfg)
    router:exec(function(cfg)
        vshard.router.cfg(cfg)
    end, {cfg})
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
    router_cfg(server, cfg)
    return server
end

--
-- Disconnect the router from all storages.
--
local function router_disconnect(router)
    router:exec(function()
        local replicasets = vshard.router.static.replicasets
        for _, rs in pairs(replicasets) do
            for _, r in pairs(rs.replicas) do
                local c = r.conn
                if c then
                    c:close()
                end
            end
        end
    end)
end

return {
    config_new = config_new,
    storage_new = storage_new,
    storage_cfg = storage_cfg,
    storage_first_bucket = storage_first_bucket,
    router_new = router_new,
    router_cfg = router_cfg,
    router_disconnect = router_disconnect,
}
