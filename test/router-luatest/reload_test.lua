local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local git_util = require('test.lua_libs.git_util')
local fio = require('fio')

local g = t.group('reload_router')
local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 100
}
local global_cfg

g.before_all(function()
    global_cfg = vtest.config_new(cfg_template)

    -- The test works in the following directory
    local vardir = vtest.vardir or fio.tempdir()
    g.vshard_copy_path =  vardir .. '/vshard_copy'
    t.assert_equals(fio.mkdir(g.vshard_copy_path), true)
    -- Copy source to the temporary directory
    t.assert_equals(fio.mkdir(g.vshard_copy_path .. '/.git'), true)
    t.assert_equals(fio.copytree(vtest.sourcedir .. '/.git',
        g.vshard_copy_path .. '/.git'), true)

    -- Hash of the latest commit for testing router on the latest version.
    g.latest_hash = git_util.log_hashes({args = '-1', dir = vtest.sourcedir})[1]

    -- No need to reload storages. Just run them on the latest version.
    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)

    -- Basic storage configuration
    vtest.cluster_exec_each_master(g, function()
        local test = box.schema.space.create('test', {format = {
            {'id', 'unsigned'},
            {'bucket_id', 'unsigned'},
        }})

        test:create_index('primary')
        test:create_index('bucket_id', {unique = false, parts = {2}})
    end)

    vtest.cluster_exec_each(g, function()
        rawset(_G, 'insert', function(space_name, tuple)
            return box.space[space_name]:insert(tuple)
        end)
        rawset(_G, 'get', function(space_name, key)
            return box.space[space_name]:get(key)
        end)
    end)
end)

g.after_all(function()
    g.cluster:drop()
end)

--
-- Reload test template:
--     1. Invoke create_router_at:
--         * Checkout to old version;
--         * Create and start a router;
--     3. Test smth on the old version;
--     4. Invoke reload_router():
--         * Checkout to the latest version;
--         * Reload the module.
--     5. Test smth on the new version
--     6. Drop a router with vtest.drop_instance
--

local function create_router_at(hash)
    git_util.exec('checkout', {args = hash .. ' -f', dir = g.vshard_copy_path})
    local router = vtest.router_new(g, 'router')
    router:exec(function(vshard_path)
        -- Force 'require' to use new directory
        package.path = string.format('%s/?.lua;%s/?/init.lua;',
                                     vshard_path, vshard_path)
        for package_name in pairs(package.loaded) do
            if package_name:startswith('vshard') then
                package.loaded[package_name] = nil
            end
        end
    end, {g.vshard_copy_path})
    vtest.router_cfg(router, global_cfg)
    return router
end

--
-- Reloads router. If service_name is provided, then
-- the function also waits until the service is restarted.
local function reload_router(router, service_name)
    git_util.exec('checkout', {args = g.latest_hash .. ' -f',
                               dir = g.vshard_copy_path})
    router:exec(function(service_name)
        local service
        local internal = ivshard.router.internal
        if service_name ~= nil then
            service = internal.static_router[service_name]
            ilt.assert_not_equals(service, nil)
        end

        ilt.assert_equals(ivshard.router.module_version(), 0)
        package.loaded['vshard.router'] = nil
        ivshard.router = require('vshard.router')
        ilt.assert_equals(ivshard.router.module_version(), 1)

        if service ~= nil then
            ilt.helpers.retrying({timeout = ivtest.wait_timeout,
                                  delay = ivtest.busy_step}, function()
                if service == internal.static_router[service_name] then
                    error('Service have not been reloaded yet')
                end
            end)
        end
    end, {service_name})
end

local function test_basic_template(router)
    router:exec(function()
        -- Basic test of callrw and callro
        local bucket_id = 1
        ivshard.router.callrw(bucket_id, 'insert', {'test', {1, bucket_id}})
        ivshard.router.callrw(bucket_id, 'vshard.storage.sync', {})
        local res, err = ivshard.router.callro(bucket_id, 'get', {'test', {1}})
        ilt.assert_equals(err, nil)
        ilt.assert_equals(res, {1, bucket_id})
    end)
end

g.test_basic = function(g)
    -- Latest meaningful commit:
    --     "router: fix reload problem with global function refs".
    local hash = '139223269cddefe2ba4b8e9f6e44712f099f4b35'
    local router = create_router_at(hash)
    test_basic_template(router)
    reload_router(router)
    test_basic_template(router)
    vtest.drop_instance(g, router)
end

local function test_discovery_template(g, router)
    router:exec(function(uuid)
        local router = ivshard.router.internal.static_router
        local discovery = router.discovery_service

        -- Everything is all right
        ivtest.service_wait_for_ok(discovery,
            {on_yield = ivshard.router.discovery_wakeup})
        ilt.assert_equals(router.known_bucket_count, 100)

        -- Break connection and the number of known buckets
        local conn = router.replicasets[uuid].master.conn
        local known = router.known_bucket_count
        router.replicasets[uuid].master.conn = nil
        router.known_bucket_count = known - 1

        ivtest.service_wait_for_error(discovery, 'Error during discovery',
            {on_yield = ivshard.router.discovery_wakeup})

        -- Restore everything
        router.replicasets[uuid].master.conn = conn
        router.known_bucket_count = known

        -- It's all right again
        ivtest.service_wait_for_ok(discovery,
            {on_yield = ivshard.router.discovery_wakeup})
    end, {g.replica_2_a:replicaset_uuid()})
end

g.test_discovery = function(g)
    -- Service_info was introduced in the following commit,
    -- the test cannot be run without it:
    --     "router: add saving of background service statuses".
    local hash = 'f5f386a5a35e6e5efd8f4f2ed1b3d208fdae9095'
    local router = create_router_at(hash)
    test_discovery_template(g, router)
    reload_router(router, 'discovery_service')
    test_discovery_template(g, router)
    vtest.drop_instance(g, router)
end

local function test_master_search_template(g, router, auto_master_cfg)
    vtest.router_cfg(router, auto_master_cfg)

    -- Working with first replicaset (2 instances)
    local rs_uuid = g.replica_1_a:replicaset_uuid()
    local master_uuid = g.replica_1_a:instance_uuid()
    local replica_uuid = g.replica_1_b:instance_uuid()

    router:exec(function()
        local router = ivshard.router.internal.static_router
        ivtest.service_wait_for_ok(router.master_search_service,
            {on_yield = ivshard.router.master_search_wakeup})
        for _, rs in pairs(router.replicasets) do
            ilt.assert_not_equals(rs.master, nil)
        end

        -- Stop master search
        ivshard.router.internal.errinj.ERRINJ_MASTER_SEARCH_DELAY = true
        ilt.helpers.retrying({timeout = ivtest.wait_timeout,
                              delay = ivtest.busy_step}, function()
            local errinj = ivshard.router.internal.errinj
            if errinj.ERRINJ_MASTER_SEARCH_DELAY ~= 'in' then
                error('Master search is not stopped yet')
            end
        end)
    end)

    -- Change master and wait, until it's found
    global_cfg.sharding[rs_uuid].replicas[master_uuid].master = false
    global_cfg.sharding[rs_uuid].replicas[replica_uuid].master = true
    vtest.cluster_cfg(g, global_cfg)

    router:exec(function(rs, master)
        -- Continue and check master search
        ivshard.router.internal.errinj.ERRINJ_MASTER_SEARCH_DELAY = false
        local router = ivshard.router.internal.static_router
        ivtest.service_wait_for_new_ok(router.master_search_service,
            {on_yield = ivshard.router.master_search_wakeup})
        ilt.assert_equals(master, router.replicasets[rs].master.uuid)
    end, {rs_uuid, replica_uuid})

    -- Restore configuration of the cluster
    global_cfg.sharding[rs_uuid].replicas[master_uuid].master = true
    global_cfg.sharding[rs_uuid].replicas[replica_uuid].master = false
    vtest.cluster_cfg(g, global_cfg)
end

g.test_master_search = function(g)
    -- Service_info was introduced in the following commit,
    -- the test cannot be run without it:
    --     "router: add saving of background service statuses".
    local hash = 'f5f386a5a35e6e5efd8f4f2ed1b3d208fdae9095'
    local router = create_router_at(hash)

    -- Enable auto master search
    local auto_master_cfg_template = table.deepcopy(cfg_template)
    for _, rs in pairs(auto_master_cfg_template.sharding) do
        rs.master = 'auto'
        for _, r in pairs(rs.replicas) do
            r.master = nil
        end
    end
    local auto_master_cfg = vtest.config_new(auto_master_cfg_template)

    test_master_search_template(g, router, auto_master_cfg)
    reload_router(router)
    test_master_search_template(g, router, auto_master_cfg)
    vtest.drop_instance(g, router)
end
