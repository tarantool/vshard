local t = require('luatest')
local vutil = require('vshard.util')
local vtest = require('test.luatest_helpers.vtest')
local vconsts = require('vshard.consts')
local git_util = require('test.lua_libs.git_util')
local upgrade = require('test.luatest_helpers.upgrade')
local fio = require('fio')

local g = t.group('reload_storage_router', {
    { update_mode = 'hot_reload' },
    { update_mode = 'restart' },
})
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
                replica_2_b = {
                    read_only = true,
                },
            },
        },
    },
    bucket_count = 100,
}
local global_cfg
local update_mode
local storage_names = {
    'replica_1_a',
    'replica_1_b',
    'replica_2_a',
    'replica_2_b',
}
-- Oldest commit that supports master = 'auto' on storage.
local legacy_storage_reload_hash = '775f5e67a02c3f71b1954c397765ed99dbf4ff7d'

local function checkout_vshard(path, hash)
    git_util.exec('checkout', { args = hash .. ' -f', dir = path })
end

g.before_all(function(cg)
    -- Tarantool's override loader is available since 2.11.
    t.run_only_if(vutil.version_is_at_least(2, 11, 0, nil, 0, 0))
    update_mode = cg.params.update_mode

    -- The test works in the following directory.
    local vardir = vtest.vardir or fio.tempdir()
    local storage_copy_root = vardir .. '/vshard_storage_copy_' .. update_mode
    fio.rmtree(storage_copy_root)
    g.storage_vshard_copy_path = storage_copy_root .. '/override'
    local router_copy_root = vardir .. '/vshard_router_copy_' .. update_mode
    fio.rmtree(router_copy_root)
    g.router_vshard_copy_path = router_copy_root .. '/override'

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
    g.storage_vshard_lua_path = upgrade.vshard_copy_new(
        vtest.sourcedir, g.storage_vshard_copy_path)
    g.router_vshard_lua_path = upgrade.vshard_copy_new(
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

local function storage_test_functions_install()
    rawset(_G, 'test_replace', function(space_name, tuple)
        return box.space[space_name]:replace(tuple)
    end)
    rawset(_G, 'test_get', function(space_name, key)
        return box.space[space_name]:get(key)
    end)
    rawset(_G, 'test_delete', function(space_name, key)
        return box.space[space_name]:delete(key)
    end)
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

    box.schema.func.create('test_replace')
    box.schema.func.create('test_get')
    box.schema.func.create('test_delete')
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
--     3. Update the storages in a separate replicaset;
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
            end, {
                upgrade.vshard_lua_path(g.storage_vshard_copy_path),
            })
            t.assert_equals(err, nil)
        else
            vtest.storage_stop(storage)
            vtest.storage_start(storage, global_cfg)
        end
        local _, err = storage:exec(storage_test_functions_install)
        t.assert_equals(err, nil, 'install storage test functions')
        _, err = storage:exec(function()
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

-- Wait until router background services finish without errors.
local function check_router_services(router)
    local _, err = router:exec(function()
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

        if static_router.master_search_service ~= nil then
            ivtest.service_wait_for_ok(static_router.master_search_service,
                { on_yield = ivshard.router.master_search_wakeup })
        else
            for _, replicaset in pairs(static_router.replicasets) do
                wait_worker_service_ok(replicaset.worker,
                                       'replicaset_master_search')
            end
        end

        local _, failover_replicaset = next(static_router.replicasets)
        if failover_replicaset.worker == nil then
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
    end)
    t.assert_equals(err, nil, 'router services')
end

-- Check that the router sees the expected master.
local function check_router_master(router, master_uuid)
    local rs_uuid = g.replica_1_a:replicaset_uuid()
    local _, err = router:exec(function(check_rs_uuid, check_master_uuid)
        local static_router = ivshard.router.static
        ilt.helpers.retrying({}, function()
            ivshard.router.master_search_wakeup()
            local replicaset = static_router.replicasets[check_rs_uuid]
            ilt.assert_not_equals(replicaset.master, nil)
            ilt.assert_equals(check_master_uuid, replicaset.master.uuid)
        end)
    end, { rs_uuid, master_uuid })
    t.assert_equals(err, nil, 'router expected master')
end

local function storage_wait_master_sync_legacy_error(storage)
    local _, err = storage:exec(function()
        ilt.helpers.retrying({ timeout = iwait_timeout }, function()
            ivshard.storage.master_sync_wakeup()
            local internal = ivshard.storage.internal
            local service = internal.master_sync_service
            ilt.assert_not_equals(service, nil)
            ilt.assert_equals(service.status, 'error')
            local is_lua_error =
                service.error:find('LuajitError', 1, true) ~= nil or
                service.error:find('ClientError', 1, true) ~= nil
            ilt.assert(is_lua_error)
            ilt.assert_str_contains(service.error,
                                    'attempt to call a nil value')
            ilt.assert_not(internal.is_bucket_in_sync)
        end)
    end)
    t.assert_equals(err, nil, 'master sync legacy error')
end

-- Wait until background discovery has found all buckets.
local function check_router_discovery(router)
    local _, err = router:exec(function()
        ilt.helpers.retrying({}, function()
            ivshard.router.discovery_wakeup()
            ilt.assert_equals(ivshard.router.info().bucket.unknown, 0)
        end)
    end)
    t.assert_equals(err, nil, 'router discovery')
end

-- Check routed write, synchronization, and read calls on both replicasets.
local function check_router_calls(router)
    local bucket_ids = {
        vtest.storage_first_bucket(g.replica_1_a),
        vtest.storage_first_bucket(g.replica_2_a),
    }
    t.assert_not_equals(bucket_ids[1], nil, 'replica_1_a first active bucket')
    t.assert_not_equals(bucket_ids[2], nil, 'replica_2_a first active bucket')

    local _, err = router:exec(function(check_bucket_ids)
        for _, bucket_id in ipairs(check_bucket_ids) do
            local tuple = { bucket_id, bucket_id }
            local res, err = ivshard.router.callrw(bucket_id, 'test_replace',
                                                   { 'test', tuple })
            ilt.assert_equals(err, nil)
            ilt.assert_equals(res, tuple)
            local _, sync_err = ivshard.router.callrw(bucket_id,
                                                      'vshard.storage.sync',
                                                      {})
            ilt.assert_equals(sync_err, nil)
            res, err = ivshard.router.callro(bucket_id, 'test_get',
                                             { 'test', { bucket_id } })
            ilt.assert_equals(err, nil)
            ilt.assert_equals(res, tuple)
            res, err = ivshard.router.callrw(bucket_id, 'test_delete',
                                              { 'test', { bucket_id } })
            ilt.assert_equals(err, nil)
            ilt.assert_equals(res, tuple)
        end
    end, { bucket_ids })
    t.assert_equals(err, nil, 'router read/write calls')
end

local function router_wait_prioritized(router, rs_uuid, expected_uuid)
    local _, err = router:exec(function(check_rs_uuid, check_uuid)
        local static_router = ivshard.router.static
        local replicaset = static_router.replicasets[check_rs_uuid]
        local function failover_wakeup()
            if replicaset.worker == nil then
                static_router.failover_fiber:wakeup()
                return
            end
            replicaset.worker:wakeup_service('replicaset_failover')
            for _, replica in pairs(replicaset.replicas) do
                replica.worker:wakeup_service('replica_failover')
            end
        end
        ilt.helpers.retrying({}, function()
            failover_wakeup()
            ilt.assert_not_equals(replicaset.replica, nil)
            ilt.assert_equals(replicaset.replica.uuid, check_uuid)
        end)
    end, { rs_uuid, expected_uuid })
    t.assert_equals(err, nil, 'wait router prioritized replica')
end

local function failover_router_cfg()
    local cfg = table.deepcopy(global_cfg)
    local rs_uuid = g.replica_1_a:replicaset_uuid()
    local replica_a_uuid = g.replica_1_a:instance_uuid()
    local replica_b_uuid = g.replica_1_b:instance_uuid()

    cfg.sharding[rs_uuid].replicas[replica_a_uuid].zone = 3
    cfg.sharding[rs_uuid].replicas[replica_b_uuid].zone = 2
    cfg.zone = 1
    cfg.weights = {
        [1] = {
            [1] = 0,
            [2] = 1,
            [3] = 2,
        },
    }
    -- Make failed ping attempts complete quickly and deterministically.
    cfg.failover_ping_timeout = 0.1
    return cfg
end

-- Check that router priority changes on a failure and is restored on recovery.
-- Both priority transitions use short timeouts to keep this check lightweight.
local function check_router_failover(router)
    vtest.router_cfg(router, failover_router_cfg())
    local timeout_state
    local ok, check_err = pcall(function()
        local rs_uuid = g.replica_1_a:replicaset_uuid()
        -- replica_1_b has the lowest cost from the router's zone.
        local prioritized_uuid = g.replica_1_b:instance_uuid()
        router_wait_prioritized(router, rs_uuid, prioritized_uuid)
        local prioritized = g.replica_1_b
        local timeout_err
        timeout_state, timeout_err = router:exec(function()
            local old = {
                down = ivconst.FAILOVER_DOWN_TIMEOUT,
                up = ivconst.FAILOVER_UP_TIMEOUT,
            }
            ivconst.FAILOVER_DOWN_TIMEOUT = 0.01
            ivconst.FAILOVER_UP_TIMEOUT = 0.01
            return old
        end)
        t.assert_equals(timeout_err, nil, 'set failover timeouts')

        prioritized:freeze()
        local changed, change_err = pcall(function()
            local fallback_uuid = g.replica_1_a:instance_uuid()
            router_wait_prioritized(router, rs_uuid, fallback_uuid)
        end)
        prioritized:thaw()
        t.assert(changed, change_err)

        -- Failover considers a replica healthy only after it catches up.
        vtest.cluster_wait_fullsync(g)
        router_wait_prioritized(router, rs_uuid, prioritized_uuid)
    end)
    if timeout_state ~= nil then
        local _, restore_timeout_err = router:exec(function(state)
            ivconst.FAILOVER_DOWN_TIMEOUT = state.down
            ivconst.FAILOVER_UP_TIMEOUT = state.up
        end, { timeout_state })
        t.assert_equals(restore_timeout_err, nil, 'restore failover timeouts')
    end
    vtest.router_cfg(router, global_cfg)
    t.assert(ok, check_err)
end

local function storage_send_bucket(src, bucket_id, dst)
    local dst_rs_uuid = dst:replicaset_uuid()
    local dst_uuid = dst:instance_uuid()
    local _, err = src:exec(function(bucket_id_to_move, rs_uuid, master_uuid)
        -- Legacy storages with master = 'auto' need the remote master to be
        -- known before a manual bucket_send().
        local replicaset = ivshard.storage.internal.replicasets[rs_uuid]
        replicaset.master = replicaset.replicas[master_uuid]

        local ok, send_err = ivshard.storage.bucket_send(bucket_id_to_move,
                                                          rs_uuid)
        ilt.assert_equals(send_err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, { bucket_id, dst_rs_uuid, dst_uuid })
    t.assert_equals(err, nil, 'bucket send')
end

local function router_wait_bucket_route(router, bucket_id, rs_uuid)
    local _, err = router:exec(function(check_bucket_id, expected_rs_uuid)
        ilt.helpers.retrying({}, function()
            ivshard.router.discovery_wakeup()
            local routed_rs, route_err = ivshard.router.route(check_bucket_id)
            ilt.assert_equals(route_err, nil)
            ilt.assert_equals(routed_rs.uuid, expected_rs_uuid)
        end)
    end, { bucket_id, rs_uuid })
    t.assert_equals(err, nil, 'bucket route')
end

-- Move a bucket, verify the new route, and return it to its original owner.
local function check_bucket_move(router, src, dst)
    local bucket_id = vtest.storage_first_bucket(src)
    t.assert_not_equals(bucket_id, nil, 'source first active bucket')
    local src_rs_uuid = src:replicaset_uuid()

    storage_send_bucket(src, bucket_id, dst)
    router_wait_bucket_route(router, bucket_id, dst:replicaset_uuid())
    storage_send_bucket(dst, bucket_id, src)
    router_wait_bucket_route(router, bucket_id, src_rs_uuid)
end

local function storage_wait_active_bucket_count(storage, count)
    local _, err = storage:exec(function(expected_count)
        local status = box.space._bucket.index.status
        ilt.helpers.retrying({ timeout = iwait_timeout }, function()
            ilt.assert_equals(
                status:count({ivconst.BUCKET.ACTIVE}), expected_count)
        end)
    end, { count })
    t.assert_equals(err, nil, 'wait active bucket count')
end

local function check_rebalancer(rs1_master)
    local rs2_master = g.replica_2_a
    local balanced_count = cfg_template.bucket_count / 2
    local imbalance = 2

    storage_wait_active_bucket_count(rs1_master, balanced_count)
    storage_wait_active_bucket_count(rs2_master, balanced_count)
    for _ = 1, imbalance do
        local bucket_id = vtest.storage_first_bucket(rs2_master)
        t.assert_not_equals(bucket_id, nil, 'rebalancer source bucket')
        storage_send_bucket(rs2_master, bucket_id, rs1_master)
    end
    storage_wait_active_bucket_count(rs1_master, balanced_count + imbalance)
    storage_wait_active_bucket_count(rs2_master, balanced_count - imbalance)

    local ok, check_err = pcall(function()
        vtest.cluster_rebalancer_enable(g)
        storage_wait_active_bucket_count(rs1_master, balanced_count)
        storage_wait_active_bucket_count(rs2_master, balanced_count)
        local _, err = vtest.cluster_exec_each_master(g, function()
            ilt.helpers.retrying({ timeout = iwait_timeout }, function()
                ilt.assert_not(ivshard.storage.rebalancing_is_in_progress())
            end)
        end)
        t.assert_equals(err, nil, 'wait rebalancer completion')
    end)
    vtest.cluster_rebalancer_disable(g)
    if ok then
        local _, err = vtest.cluster_exec_each_master(g, function()
            _G.bucket_gc_wait()
        end)
        t.assert_equals(err, nil, 'bucket GC after rebalancing')
    end
    t.assert(ok, check_err)
end

local function check_cluster(router, opts)
    check_router_services(router)
    check_router_master(router, opts.master:instance_uuid())
    check_router_discovery(router)
    check_router_calls(router)
    check_router_failover(router)

    for _, move in ipairs(opts.bucket_moves or {}) do
        check_bucket_move(router, move.src, move.dst)
    end
    if opts.check_rebalancer then
        check_rebalancer(opts.master)
    end
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
    for _, name in ipairs(storage_names) do
        local _, err = g[name]:exec(storage_test_functions_install)
        t.assert_equals(err, nil, 'install storage test functions')
    end
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
        { 'replica_2_b', opts.storage_version },
    }
    local one_replicaset_new = {
        { 'replica_1_a', opts.storage_version },
        { 'replica_1_b', opts.storage_version },
        { 'replica_2_a', vconsts.VERSION },
        { 'replica_2_b', vconsts.VERSION },
    }
    local mixed = {
        { 'replica_1_a', opts.storage_version },
        { 'replica_1_b', vconsts.VERSION },
        { 'replica_2_a', vconsts.VERSION },
        { 'replica_2_b', vconsts.VERSION },
    }
    local all_new = {
        { 'replica_1_a', vconsts.VERSION },
        { 'replica_1_b', vconsts.VERSION },
        { 'replica_2_a', vconsts.VERSION },
        { 'replica_2_b', vconsts.VERSION },
    }
    -- First phase: everything is on old vshard.
    assert_versions(router, all_old, opts.storage_version)
    check_cluster(router, {
        master = old_master,
        bucket_moves = {
            { src = g.replica_2_a, dst = g.replica_1_a },
        },
        check_rebalancer = opts.upgrade_from_previous,
    })

    -- Second phase: the separate replicaset is updated on new vshard.
    -- Update its replica before its master in restart mode.
    update_storages({ 'replica_2_b', 'replica_2_a' })
    assert_versions(router, one_replicaset_new, opts.storage_version)
    check_cluster(router, {
        master = old_master,
        -- vshard 0.1.24 does not start the rebalancer when master = 'auto'.
        check_rebalancer = opts.upgrade_from_previous,
        -- Test both directions between fully old and fully new replicasets.
        bucket_moves = {
            { src = g.replica_1_a, dst = g.replica_2_a },
            { src = g.replica_2_a, dst = g.replica_1_a },
        },
    })

    -- Third phase: another replica is updated on new vshard.
    update_storages({ 'replica_1_b' })
    assert_versions(router, mixed, opts.storage_version)
    check_cluster(router, {
        master = old_master,
    })

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
    if opts.master_sync_requires_upgrade then
        check_router_master(router, new_master_uuid)
        storage_wait_master_sync_legacy_error(new_master)
    else
        vtest.storage_wait_bucket_sync(new_master)
        check_cluster(router, {
            master = new_master,
            check_rebalancer = opts.upgrade_from_previous,
        })
    end

    -- Fifth phase: previous master is updated.
    update_storages({ 'replica_1_a' })
    storage_cfg_by_name({ 'replica_1_a' }, global_cfg)
    vtest.storage_wait_bucket_sync(new_master)
    assert_versions(router, all_new, opts.storage_version)
    check_cluster(router, {
        master = new_master,
        check_rebalancer = true,
        bucket_moves = {
            { src = g.replica_1_b, dst = g.replica_2_a },
        },
    })

    -- Sixth phase: router is updated.
    update_router(router)
    storage_cfg_by_name(storage_names, global_cfg)
    assert_versions(router, all_new, vconsts.VERSION)
    check_cluster(router, {
        master = new_master,
    })
end

g.test_rolling_legacy_storage_then_router_upgrade = function()
    run_rolling_upgrade({
        hash = legacy_storage_reload_hash,
        storage_version = '0.1.24',
        master_sync_requires_upgrade = true,
    })
end

g.test_rolling_previous_storage_then_router_upgrade = function()
    run_rolling_upgrade({
        hash = g.previous_version,
        storage_version = g.previous_version,
        upgrade_from_previous = true,
    })
end
