local t = require('luatest')
local vutil = require('vshard.util')
local vtest = require('test.luatest_helpers.vtest')
local vconsts = require('vshard.consts')
local git_util = require('test.lua_libs.git_util')
local fio = require('fio')

local group_config = {
    { update_mode = 'hot_reload' },
    { update_mode = 'restart' },
}
local g = t.group('reload_storage_router', group_config)
local cfg_template = {
    sharding = {
        {
            master = 'auto',
            replicas = {
                replica_1_a = {
                    read_only = false,
                },
                replica_1_b = {
                    read_only = true,
                },
            },
        },
        {
            master = 'auto',
            replicas = {
                replica_2_a = {
                    read_only = false,
                },
            },
        },
    },
    bucket_count = 100,
}
local global_cfg
local update_mode
local storage_names = { 'replica_1_a', 'replica_1_b', 'replica_2_a' }
-- Oldest commit that supports master = 'auto' on storage.
local legacy_storage_reload_hash = '775f5e67a02c3f71b1954c397765ed99dbf4ff7d'

local function checkout_vshard(path, hash)
    git_util.exec('checkout', { args = hash .. ' -f', dir = path })
end

g.before_all(function(cg)
    -- Override of the built in modules is available only since 2.11.0.
    t.run_only_if(vutil.version_is_at_least(2, 11, 0, nil, 0, 0))
    update_mode = cg.params.update_mode

    -- The test works in the following directory.
    local vardir = fio.tempdir()

    --
    -- Tarantool searches for compilation units in the following order:
    --   1. preload --> override --> builtin
    --   2. path.cwd.dot
    --   3. cpath.cwd.dot
    --   4. path.cwd.rocks
    --   5. cpath.cwd.rocks
    --   6. package.path
    --   7. package.cpath
    --   8. croot
    --
    -- Since the test is launched from the repository directory, newest
    -- vshard will always be loaded, regardless of package.path or `LUA_PATH`.
    -- So we can use package.setsearchroot() to change cwd, luatest's `chdir`
    -- or override. The last one is used here, since it's the easiest.
    --
    g.storage_vshard_copy_path = vardir .. '/vshard_storage_copy_' ..
                                 update_mode .. '/override'
    g.storage_vshard_lua_path = git_util.vshard_copy_new(
        vtest.sourcedir, g.storage_vshard_copy_path)
    g.router_vshard_copy_path = vardir .. '/vshard_router_copy_' ..
                                update_mode .. '/override'
    g.router_vshard_lua_path = git_util.vshard_copy_new(
        vtest.sourcedir, g.router_vshard_copy_path)

    g.latest_hash = git_util.log_hashes({ args = '-1',
                                          dir = vtest.sourcedir })[1]

    local temp_file = os.tmpname()
    git_util.exec('describe', {
        args = "--tags --abbrev=0 --match '[0-9]*.[0-9]*.[0-9]*' HEAD^",
        dir = vtest.sourcedir,
        fout = temp_file,
    })
    g.previous_version = io.lines(temp_file)()
    os.remove(temp_file)
    t.assert_not_equals(g.previous_version, nil, 'previous release tag')
end)

local function assert_versions(router, storage_versions, router_version)
    for _, storage_version in ipairs(storage_versions) do
        local name = storage_version[1]
        local expected_version = storage_version[2]
        local _, err = g[name]:exec(function(version)
            ilt.assert_equals(ivconst.VERSION, version)
        end, { expected_version })
        t.assert_equals(err, nil)
    end
    router:exec(function(version)
        ilt.assert_equals(ivconst.VERSION, version)
    end, { router_version })
end

local function storage_test_data_create()
    local test = box.schema.space.create('test', { format = {
        { 'id', 'unsigned' },
        { 'bucket_id', 'unsigned' },
    } })
    test:create_index('primary')
    test:create_index('bucket_id', {
        unique = false,
        parts = { 2 },
    })

    box.schema.func.create('replace', {
        body = [[function(space_name, tuple)
            return box.space[space_name]:replace(tuple)
        end]]
    })
    box.schema.func.create('get', {
        body = [[function(space_name, key)
            return box.space[space_name]:get(key)
        end]]
    })
end

g.after_each(function()
    for _, server in ipairs(g.cluster.servers) do
        if server.log_file ~= nil and fio.path.exists(server.log_file) then
            local msg = server:grep_log('[Aa]ssertion failed',
                                        64 * 1024 * 1024, {noreset = true})
            t.assert_equals(msg, nil)
        end
    end

    g.cluster:drop()
    g.cluster = nil
end)

local function storage_cfg_by_name(names, cfg)
    for _, name in ipairs(names) do
        vtest.storage_cfg(g[name], cfg)
    end
end

--
-- Upgrade test workflow:
--     1. Start storages and router on the old vshard version;
--     2. Run all checks;
--     3. Update one storage in a separate replicaset;
--     4. Run all checks;
--     5. Update the future master replica;
--     6. Run all checks;
--     7. Move master to the updated replica;
--     8. Run all checks;
--     9. Update the previous master replica;
--    10. Run all checks.
--    11. Update router;
--    12. Run all checks.
--
local function update_storages(names)
    checkout_vshard(g.storage_vshard_copy_path, g.latest_hash)
    for _, name in ipairs(names) do
        local storage = g[name]
        if update_mode == 'hot_reload' then
            local _, err = storage:exec(function(lua_path)
                package.path = lua_path
                package.loaded['vshard.storage'] = nil
                _G.vshard.storage = require('vshard.storage')
                _G.ivconst = require('vshard.consts')
            end, { git_util.vshard_lua_path(g.storage_vshard_copy_path) })
            t.assert_equals(err, nil)
        else
            vtest.storage_stop(storage)
            vtest.storage_start(storage, global_cfg)
        end
        local _, err = storage:exec(function()
            ivshard.storage.rebalancer_disable()
        end)
        t.assert_equals(err, nil)
    end
end

local function update_router(router)
    checkout_vshard(g.router_vshard_copy_path, g.latest_hash)
    if update_mode == 'hot_reload' then
        router:exec(function()
            ilt.assert_equals(ivshard.router.module_version(), 0)
            package.loaded['vshard.router'] = nil
            ivshard.router = require('vshard.router')
            _G.ivconst = require('vshard.consts')
            ilt.assert_equals(ivshard.router.module_version(), 1)
        end)
    else
        router:restart()
        vtest.router_cfg(router, global_cfg)
    end
end

local function check_cluster(router, master_uuid, move_src, move_dst)
    local bucket_ids = {
        vtest.storage_first_bucket(g.replica_1_a),
        vtest.storage_first_bucket(g.replica_2_a),
    }
    t.assert_not_equals(bucket_ids[1], nil, 'replica_1_a first active bucket')
    t.assert_not_equals(bucket_ids[2], nil, 'replica_2_a first active bucket')

    local rs_uuid = g.replica_1_a:replicaset_uuid()
    router:exec(function(check_rs_uuid, check_master_uuid, check_bucket_ids)
        local static_router = ivshard.router.static
        local function wait_worker_service_ok(worker, name)
            local function wakeup()
                worker:wakeup_service(name)
            end
            local service = worker.services[name]
            ivtest.wait_for_not_nil(service.data, 'info',
                                    { on_yield = wakeup })
            ivtest.service_wait_for_ok(service.data.info,
                                       { on_yield = wakeup })
        end

        -- Wait until master search services finish without errors.
        if static_router.master_search_service ~= nil then
            ivtest.service_wait_for_ok(static_router.master_search_service,
                { on_yield = ivshard.router.master_search_wakeup })
        else
            for _, replicaset in pairs(static_router.replicasets) do
                wait_worker_service_ok(replicaset.worker,
                                       'replicaset_master_search')
            end
        end

        -- Wait until failover services finish without errors.
        if static_router.failover_service ~= nil then
            ivtest.service_wait_for_ok(static_router.failover_service, {
                on_yield = function()
                    static_router.failover_fiber:wakeup()
                end,
            })
        else
            for _, replicaset in pairs(static_router.replicasets) do
                wait_worker_service_ok(replicaset.worker,
                                       'replicaset_failover')
                for _, replica in pairs(replicaset.replicas) do
                    wait_worker_service_ok(replica.worker, 'replica_failover')
                end
            end
        end

        -- Check the router sees the expected master.
        ilt.helpers.retrying({}, function()
            ivshard.router.master_search_wakeup()
            local replicaset = static_router.replicasets[check_rs_uuid]
            ilt.assert_not_equals(replicaset.master, nil)
            ilt.assert_equals(check_master_uuid, replicaset.master.uuid)
        end)

        -- Wait until background discovery has found all buckets.
        ilt.helpers.retrying({}, function()
            ivshard.router.discovery_wakeup()
            ilt.assert_equals(ivshard.router.info().bucket.unknown, 0)
        end)

        -- Check that routing still works for both write and read
        -- requests on both replicasets.
        for _, bucket_id in ipairs(check_bucket_ids) do
            local tuple = { bucket_id, bucket_id }
            local res, err = ivshard.router.callrw(bucket_id, 'replace',
                                                   { 'test', tuple })
            ilt.assert_equals(err, nil)
            ilt.assert_equals(res, tuple)
            local _, sync_err = ivshard.router.callrw(bucket_id,
                                                      'vshard.storage.sync',
                                                      {})
            ilt.assert_equals(sync_err, nil)
            res, err = ivshard.router.callro(bucket_id, 'get',
                                             { 'test', { bucket_id } })
            ilt.assert_equals(err, nil)
            ilt.assert_equals(res, tuple)
        end
    end, { rs_uuid, master_uuid, bucket_ids })

    -- Move a bucket and check that router discovery sees the new owner.
    local moved_bucket_id = vtest.storage_first_bucket(move_src)
    local move_dst_rs_uuid = move_dst:replicaset_uuid()
    local move_dst_uuid = move_dst:instance_uuid()
    local _, send_err = move_src:exec(function(bucket_id_to_move,
                                               dst_rs_uuid, dst_master_uuid)
        -- When the source runs legacy vshard, master = 'auto' works only for
        -- its own replicaset. Tell it the remote master before bucket_send().
        local replicaset = ivshard.storage.internal.replicasets[dst_rs_uuid]
        local master = replicaset.replicas[dst_master_uuid]
        replicaset.master = master

        local ok, err = ivshard.storage.bucket_send(bucket_id_to_move,
                                                    dst_rs_uuid)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
    end, { moved_bucket_id, move_dst_rs_uuid, move_dst_uuid })
    t.assert_equals(send_err, nil)

    local _, route_err = router:exec(function(route_bucket_id,
                                              expected_rs_uuid)
        ilt.helpers.retrying({}, function()
            ivshard.router.discovery_wakeup()
            local routed_rs, err = ivshard.router.route(route_bucket_id)
            ilt.assert_equals(err, nil)
            ilt.assert_equals(routed_rs.uuid, expected_rs_uuid)
        end)
    end, { moved_bucket_id, move_dst_rs_uuid })
    t.assert_equals(route_err, nil)
end

local function create_cluster_at(opts)
    global_cfg = vtest.config_new(cfg_template)
    checkout_vshard(g.storage_vshard_copy_path, opts.hash)
    local env = {
        LUA_PATH = g.storage_vshard_lua_path,
    }
    vtest.cluster_new(g, global_cfg, { env = env })

    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_bootstrap(g, global_cfg)
    -- Create user data and simple functions for router callrw/callro checks.
    for _, name in ipairs({ 'replica_1_a', 'replica_2_a' }) do
        local _, err = g[name]:exec(storage_test_data_create)
        t.assert_equals(err, nil)
    end
    vtest.cluster_wait_vclock_all(g)

    checkout_vshard(g.router_vshard_copy_path, opts.hash)
    local router = vtest.router_new(g, 'router', nil, {
        env = {
            -- Force 'require' to use new directory.
            LUA_PATH = g.router_vshard_lua_path,
        },
    })
    vtest.router_cfg(router, global_cfg)
    return router
end

local function run_rolling_upgrade(opts)

    local router = create_cluster_at(opts)
    local old_master_uuid = g.replica_1_a:instance_uuid()
    local new_master_uuid = g.replica_1_b:instance_uuid()
    local old_master = g.replica_1_a
    local new_master = g.replica_1_b

    local all_old = {
        { 'replica_1_a', opts.storage_version },
        { 'replica_1_b', opts.storage_version },
        { 'replica_2_a', opts.storage_version },
    }
    local one_new = {
        { 'replica_1_a', opts.storage_version },
        { 'replica_1_b', opts.storage_version },
        { 'replica_2_a', vconsts.VERSION },
    }
    local mixed = {
        { 'replica_1_a', opts.storage_version },
        { 'replica_1_b', vconsts.VERSION },
        { 'replica_2_a', vconsts.VERSION },
    }
    local all_new = {
        { 'replica_1_a', vconsts.VERSION },
        { 'replica_1_b', vconsts.VERSION },
        { 'replica_2_a', vconsts.VERSION },
    }

    -- First phase: everything is on old vshard.
    assert_versions(router, all_old, opts.storage_version)
    check_cluster(router, old_master_uuid, g.replica_2_a, g.replica_1_a)

    -- Second phase: one replica is updated on new vshard.
    update_storages({ 'replica_2_a' })
    assert_versions(router, one_new, opts.storage_version)
    check_cluster(router, old_master_uuid, g.replica_2_a, g.replica_1_a)

    -- Third phase: another replica is updated on new vshard.
    update_storages({ 'replica_1_b' })
    assert_versions(router, mixed, opts.storage_version)
    check_cluster(router, old_master_uuid, g.replica_2_a, g.replica_1_a)

    -- Fourth phase: master is moved to the updated replica, previous master
    -- is still on old vshard.
    old_master:update_box_cfg({ read_only = true })
    new_master:wait_vclock_of(old_master)
    new_master:update_box_cfg({ read_only = false })
    local rs_uuid = old_master:replicaset_uuid()
    global_cfg.sharding[rs_uuid].replicas[old_master_uuid].read_only = true
    global_cfg.sharding[rs_uuid].replicas[new_master_uuid].read_only = false
    storage_cfg_by_name({ 'replica_1_a', 'replica_1_b' }, global_cfg)
    vtest.router_cfg(router, global_cfg)
    assert_versions(router, mixed, opts.storage_version)
    check_cluster(router, new_master_uuid, g.replica_2_a, g.replica_1_b)

    -- Fifth phase: previous master is updated.
    update_storages({ 'replica_1_a' })
    storage_cfg_by_name({ 'replica_1_a' }, global_cfg)
    assert_versions(router, all_new, opts.storage_version)
    check_cluster(router, new_master_uuid, g.replica_1_b, g.replica_2_a)

    -- Sixth phase: router is updated.
    update_router(router)
    storage_cfg_by_name(storage_names, global_cfg)
    assert_versions(router, all_new, vconsts.VERSION)
    check_cluster(router, new_master_uuid, g.replica_2_a, g.replica_1_b)
end

g.test_rolling_legacy_storage_then_router_upgrade = function()
    run_rolling_upgrade({
        hash = legacy_storage_reload_hash,
        storage_version = '0.1.24',
    })
end

g.test_rolling_previous_storage_then_router_upgrade = function()
    run_rolling_upgrade({
        hash = g.previous_version,
        storage_version = g.previous_version,
    })
end
