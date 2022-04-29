local t = require('luatest')
local helpers = require('test.luatest_helpers')
local cluster = require('test.luatest_helpers.cluster')
local fio = require('fio')
local fiber = require('fiber')
local uuid = require('uuid')
local yaml = require('yaml')
local vrepset = require('vshard.replicaset')

local wait_timeout = 120
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

local cert_dir = fio.pathjoin(fio.cwd(), './test/certs')
local ssl_ca_file = fio.pathjoin(cert_dir, 'ca.crt')
local ssl_server_cert_file = fio.pathjoin(cert_dir, 'server.crt')
local ssl_server_key_file = fio.pathjoin(cert_dir, 'server.key')

local function uuid_str_from_int(i)
    i = tostring(i)
    assert(#i <= 12)
    return '00000000-0000-0000-0000-'..string.rep('0', 12 - #i)..i
end

local function uuid_from_int(i)
    return uuid.fromstr(uuid_str_from_int(i))
end

--
-- New UUID unique per this process. Generation is not random - for simplicity
-- and reproducibility.
--
local function uuid_str_next()
    local i = uuid_idx
    uuid_idx = uuid_idx + 1
    return uuid_str_from_int(i)
end

local function name_to_uuid(map, name)
    local res = map[name]
    if not res then
        res = uuid_str_next()
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
        replicaset.is_ssl = nil
        local is_ssl = replicaset_templ.is_ssl
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
            if is_ssl then
                if not replica.listen then
                    replica.listen = {replica.uri}
                end
                for j, listen in pairs(replica.listen) do
                    replica.listen[j] = {
                        listen,
                        params = {
                            transport = 'ssl',
                            ssl_cert_file = ssl_server_cert_file,
                            ssl_key_file = ssl_server_key_file,
                        },
                    }
                end
                replica.uri = {
                    replica.uri,
                    params = {
                        transport = 'ssl',
                        ssl_ca_file = ssl_ca_file,
                    }
                }
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
            box_cfg.memtx_use_mvcc_engine = cfg.memtx_use_mvcc_engine
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

            ivshard.storage.cfg(cfg, box.info.uuid)
            box.schema.user.grant('storage', 'super')

            box.session.su(user)
        end, {cfg})
    end
    for _, replica in pairs(replicas) do
        replica:wait_for_readiness()
        replica:exec(function(cfg)
            ivshard.storage.cfg(cfg, box.info.uuid)
        end, {cfg})
    end
end

--
-- Find all vshard storages in the cluster.
--
local function storage_find_all(g)
    local result = {}
    for _, storage in pairs(g.cluster.servers) do
        if storage.vtest and storage.vtest.is_storage then
            table.insert(result, storage)
        end
    end
    return result
end

--
-- Execute func(storage) in parallel for all the given storages.
--
local function storage_for_each_in(storages, func)
    local fibers = table.new(0, #storages)
    -- Map-reduce. Parallel execution not only is faster but also helps not to
    -- depend on which order would be non-blocking. For example, at storage
    -- reconfiguration there might be a config which makes the master hang until
    -- some replica is configured first. When all are done in parallel, it won't
    -- matter.
    for _, storage in pairs(storages) do
        local name = storage.vtest.name
        local f = fiber.new(func, storage)
        f:set_joinable(true)
        fibers[name] = f
    end
    local result = table.new(0, #storages)
    local errors = {}
    for name, f in pairs(fibers) do
        local ok, res = f:join()
        if not ok then
            errors[name] = res
        else
            result[name] = res
        end
    end
    if not next(errors) then
        return result
    end
    return nil, errors
end

--
-- Execute func(storage) in parallel for all storages.
--
local function storage_for_each(g, func)
    return storage_for_each_in(storage_find_all(g), func)
end

--
-- Execute storage:exec(func, args) in parallel for all storages.
--
local function storage_exec_each(g, func, args)
    return storage_for_each(g, function(storage)
        return storage:exec(func, args)
    end)
end

--
-- Find all vshard storage masters in the cluster.
--
local function storage_find_all_masters(g)
    local res, err = storage_for_each(g, function(storage)
        return storage:call('vshard.storage._call', {'info'}).is_master
    end)
    if not res then
        return nil, err
    end
    local masters = {}
    for name, is_master in pairs(res) do
        if is_master then
            local server = g[name]
            t.assert_not_equals(server, nil, 'find master instance')
            table.insert(masters, server)
        end
    end
    return masters
end

--
-- Execute func(storage) in parallel for all master storages.
--
local function storage_for_each_master(g, func)
    local masters, err = storage_find_all_masters(g)
    if not masters then
        return nil, err
    end
    return storage_for_each_in(masters, func)
end

--
-- Execute storage:exec(func, args) in parallel for all master storages.
--
local function storage_exec_each_master(g, func, args)
    return storage_for_each_master(g, function(storage)
        return storage:exec(func, args)
    end)
end

local function storage_boot_one_f(first, count)
    return ivshard.storage.bucket_force_create(first, count)
end

--
-- Bootstrap the cluster without a router by a given config. In theory the
-- config could be fetched from the storages, but it would force to check its
-- consistency.
--
local function storage_bootstrap(g, cfg)
    local masters = {}
    local etalon_balance = {}
    local replicaset_count = 0
    for rs_uuid, rs in pairs(cfg.sharding) do
        local is_master_found = false
        for _, rep in pairs(rs.replicas) do
            if rep.master then
                t.assert(not is_master_found, 'only one master')
                local server = g[rep.name]
                t.assert_not_equals(server, nil, 'find master instance')
                t.assert_equals(server:replicaset_uuid(), rs_uuid,
                                'replicaset uuid')
                masters[rs_uuid] = server
                is_master_found = true
            end
        end
        t.assert(is_master_found, 'found master')
        local weight = rs.weight
        if weight == nil then
            weight = 1
        end
        etalon_balance[rs_uuid] = {
            weight = weight
        }
        replicaset_count = replicaset_count + 1
    end
    t.assert_not_equals(masters, {}, 'have masters')
    vrepset.calculate_etalon_balance(etalon_balance, cfg.bucket_count)
    local fibers = table.new(0, replicaset_count)
    local bid = 1
    for rs_uuid, rs in pairs(etalon_balance) do
        local master = masters[rs_uuid]
        local count = rs.etalon_bucket_count
        local f = fiber.new(master.exec, master, storage_boot_one_f,
                            {bid, count})
        f:set_joinable(true)
        fibers[master.vtest.name] = f
        bid = bid + count
    end
    local errors = {}
    for name, f in pairs(fibers) do
        local ok, res1, res2 = f:join()
        if not ok then
            errors[name] = res1
        elseif res1 == nil then
            errors[name] = res2
        else
            t.assert_equals(res2, nil, 'boot_one no error')
            t.assert(res1, 'boot_one success')
        end
    end
    t.assert_equals(errors, {}, 'storage bootstrap')
end

--
-- Apply the config to all vshard storages in the cluster.
--
local function storage_cfg(g, cfg)
    -- No support yet for dynamic node addition and removal. Only reconfig.
    local _, err = storage_exec_each(g, function(cfg)
        return ivshard.storage.cfg(cfg, box.info.uuid)
    end, {cfg})
    t.assert_equals(err, nil, 'storage reconfig')
end

--
-- Find first active bucket on the storage. In tests it helps not to assume
-- where the buckets are located by hardcoded numbers and uuids.
--
local function storage_first_bucket(storage)
    return storage:exec(function()
        return _G.get_first_bucket()
    end)
end

--
-- Disable rebalancer on all storages.
--
local function storage_rebalancer_disable(g)
    local _, err =  storage_exec_each(g, function()
        ivshard.storage.rebalancer_disable()
    end)
    t.assert_equals(err, nil, 'rebalancer disable')
end

--
-- Enable rebalancer on all storages.
--
local function storage_rebalancer_enable(g)
    local _, err =  storage_exec_each(g, function()
        ivshard.storage.rebalancer_enable()
    end)
    t.assert_equals(err, nil, 'rebalancer enable')
end

--
-- Wait vclock sync in each replicaset between all its replicas.
--
local function storage_wait_vclock_all(g)
    local replicasets = {}
    for _, storage in pairs(storage_find_all(g)) do
        local uuid = storage:replicaset_uuid()
        local replicaset = replicasets[uuid]
        if not replicaset then
            replicasets[uuid] = {storage}
        else
            table.insert(replicaset, storage)
        end
    end
    for _, replicaset in pairs(replicasets) do
        for i = 1, #replicaset do
            local s1 = replicaset[i]
            for j = i + 1, #replicaset do
                local s2 = replicaset[j]
                s1:wait_vclock_of(s2)
                s2:wait_vclock_of(s1)
            end
        end
    end
end

--
-- Wait until the instance follows the master having the given instance ID.
--
local function storage_wait_follow_f(timeout, id)
    local deadline = ifiber.clock() + timeout
    local last_err
    while true do
        local info = box.info.replication[id]
        local stream, status
        if info == nil then
            last_err = 'Not found replication info'
            goto retry
        end
        stream = info.upstream
        if stream == nil then
            last_err = 'Not found upstream'
            goto retry
        end
        status = stream.status
        if status == nil then
            last_err = 'Not found upstream status'
            goto retry
        end
        if status ~= 'follow' then
            last_err = 'Upstream status is not follow'
            goto retry
        end
        do return end
    ::retry::
        if ifiber.clock() > deadline or status == 'stopped' then
            ilt.fail(yaml.encode({
                err = last_err,
                dst_id = id,
                replication_info = box.info.replication,
                replication_cfg = box.cfg.replication,
            }))
        end
        ifiber.sleep(0.01)
    end
end

--
-- Wait full synchronization between the given servers: same vclock and mutual
-- following.
--
local function storage_wait_pairsync(s1, s2)
    s1:wait_vclock_of(s2)
    s2:wait_vclock_of(s1)
    s1:exec(storage_wait_follow_f, {wait_timeout, s2:instance_id()})
    s2:exec(storage_wait_follow_f, {wait_timeout, s1:instance_id()})
end

--
-- Wait full synchronization between all nodes in each replication of the
-- cluster.
--
local function storage_wait_fullsync(g)
    local replicasets = {}
    for _, storage in pairs(storage_find_all(g)) do
        local uuid = storage:replicaset_uuid()
        local replicaset = replicasets[uuid]
        if not replicaset then
            replicasets[uuid] = {storage}
        else
            table.insert(replicaset, storage)
        end
    end
    for _, replicaset in pairs(replicasets) do
        for i = 1, #replicaset do
            local s1 = replicaset[i]
            for j = i + 1, #replicaset do
                storage_wait_pairsync(s1, replicaset[j])
            end
        end
    end
end

--
-- Apply the config on the given router.
--
local function router_cfg(router, cfg)
    router:exec(function(cfg)
        ivshard.router.cfg(cfg)
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
        local replicasets = ivshard.router.static.replicasets
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
    storage_for_each = storage_for_each,
    storage_for_each_master = storage_for_each_master,
    storage_exec_each_master = storage_exec_each_master,
    storage_bootstrap = storage_bootstrap,
    storage_first_bucket = storage_first_bucket,
    storage_rebalancer_disable = storage_rebalancer_disable,
    storage_rebalancer_enable = storage_rebalancer_enable,
    storage_wait_vclock_all = storage_wait_vclock_all,
    storage_wait_fullsync = storage_wait_fullsync,
    router_new = router_new,
    router_cfg = router_cfg,
    router_disconnect = router_disconnect,
    uuid_from_int = uuid_from_int,
}
