local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local wait_timeout = vtest.wait_timeout

local g = t.group()

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
                replica_1_c = {},
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
                replica_2_b = {},
                replica_2_c = {},
            },
        },
    },
    bucket_count = 100,
    test_user_grant_range = 'super',
}

local global_cfg

g.before_all(function(g)
    global_cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(g, global_cfg)

    t.assert_equals(g.replica_1_a:exec(function()
        return #ivshard.storage.info().alerts
    end), 0, 'no alerts after boot')

    local router = vtest.router_new(g, 'router', global_cfg)
    g.router = router
    local res, err = router:exec(function()
        return ivshard.router.bootstrap({timeout = iwait_timeout})
    end)
    t.assert(res and not err, 'bootstrap buckets')

    vtest.cluster_exec_each_master(g, function()
        local s = box.schema.space.create('test', {
            format = {
                {'id', 'unsigned'},
                {'bucket_id', 'unsigned'},
            },
        })
        s:create_index('id', {parts = {'id'}})
        s:create_index('bucket_id', {
            parts = {'bucket_id'}, unique = false
        })
    end)
    vtest.cluster_wait_fullsync(g)
end)

g.after_all(function(g)
    g.cluster:stop()
end)

local function prepare_failover_health_check(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].replicas.replica_1_a.zone = 4
    new_cfg_template.sharding[1].replicas.replica_1_b.zone = 3
    new_cfg_template.sharding[1].replicas.replica_1_c.zone = 2
    new_cfg_template.zone = 1
    new_cfg_template.weights = {
        [1] = {
            [1] = 0,
            [2] = 1,
            [3] = 2,
            [4] = 3,
        },
    }

    local new_cluster_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_cluster_cfg)
    g.router:exec(function()
        -- Speed up switching replicas.
        rawset(_G, 'old_failover_up_timeout', ivconst.FAILOVER_UP_TIMEOUT)
        ivconst.FAILOVER_UP_TIMEOUT = 0.5
    end)
end

local function router_assert_prioritized(g, replica)
    local uuid = g.router:exec(function(rs_uuid)
        local router = ivshard.router.internal.static_router
        local replica = router.replicasets[rs_uuid].replica
        t.assert_not_equals(replica, nil)
        return replica.uuid
    end, {replica:replicaset_uuid()})
    t.assert_equals(uuid, replica:instance_uuid())
end

local function router_wakeup_failover(g)
    g.router:exec(function()
        _G.failover_wakeup()
    end)
end

local function router_wait_prioritized(g, replica)
    t.helpers.retrying({timeout = wait_timeout}, function()
        router_assert_prioritized(g, replica)
        router_wakeup_failover(g)
    end)
end

local function failover_health_check_missing_upstream(g)
    router_wait_prioritized(g, g.replica_1_c)
    -- Reconfigure box.cfg.replication.
    local box_cfg = g.replica_1_c:get_box_cfg()
    g.replica_1_c:update_box_cfg({replication = {
        g.replica_1_c.net_box_uri,
        g.replica_1_b.net_box_uri,
    }})
    -- Prioritized replica is changed to another one.
    router_wait_prioritized(g, g.replica_1_b)
    local msg = 'Replica %s is unhealthy: Missing upstream'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.assert(g.router:grep_log(msg), msg)
    -- Restore replication. Replica returns.
    g.replica_1_c:update_box_cfg({replication = box_cfg.replication})
    router_wait_prioritized(g, g.replica_1_c)
    msg = 'Replica %s is healthy'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.assert(g.router:grep_log(msg), msg)
end

local function failover_health_check_broken_upstream(g)
    router_wait_prioritized(g, g.replica_1_c)
    -- Break replication. Replica changes.
    g.replica_1_c:exec(function()
        rawset(_G, 'on_replace', function()
            box.error(box.error.READONLY)
        end)
        box.space.test:on_replace(_G.on_replace)
    end)
    g.replica_1_a:exec(function()
        local bid = _G.get_first_bucket()
        box.space.test:replace{1, bid}
    end)
    g.replica_1_c:exec(function(id)
        rawset(_G, 'old_idle', ivconst.REPLICA_MAX_IDLE)
        ivconst.REPLICA_MAX_IDLE = 0.1
        -- On 1.10 and 2.8 upstreaim goes into never ending
        -- retry of connecting and applying the failing row.
        -- The status of upstream is 'loading' in such case.
        -- It should consider itself non-healthy according to
        -- it's idle time.
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            local upstream = box.info.replication[id].upstream
            ilt.assert_not_equals(upstream, nil)
            ilt.assert(upstream.status == 'stopped' or
                       upstream.status == 'loading')
        end)
    end, {g.replica_1_a:instance_id()})
    router_wait_prioritized(g, g.replica_1_b)
    -- Either status or idle, depending on version.
    local msg = 'Replica %s is unhealthy'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.assert(g.router:grep_log(msg), msg)

    -- Drop on_replace trigger. Replica returns.
    g.replica_1_c:exec(function()
        ivconst.REPLICA_MAX_IDLE = _G.old_idle
        box.space.test:on_replace(nil, _G.on_replace)
        local replication = box.cfg.replication
        box.cfg{replication = {}}
        box.cfg{replication = replication}
    end)
    router_wait_prioritized(g, g.replica_1_c)
    msg = 'Replica %s is healthy'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.assert(g.router:grep_log(msg), msg)
    g.replica_1_a:exec(function()
        box.space.test:truncate()
    end)
end

local function failover_health_check_big_lag(g)
    router_wait_prioritized(g, g.replica_1_c)
    g.replica_1_c:exec(function()
        -- There's no other simple way to increase lag without debug build.
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = function(service_name, ...)
            if service_name == 'info' then
                return {
                    health = {
                        master_upstream = {
                            status = 'follow',
                            idle = 0,
                            lag = 100500,
                        },
                    }
                }
            end
            return _G.old_call(service_name, ...)
        end
    end)
    router_wait_prioritized(g, g.replica_1_b)
    local msg = 'Replica %s is unhealthy: Upstream to master has lag'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.assert(g.router:grep_log(msg), msg)

    g.replica_1_c:exec(function()
        ivshard.storage._call = _G.old_call
    end)
    router_wait_prioritized(g, g.replica_1_c)
    msg = 'Replica %s is healthy'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.assert(g.router:grep_log(msg), msg)
end

local function failover_health_check_small_failover_timeout(g)
    local old_cfg = g.router:exec(function()
        return ivshard.router.internal.static_router.current_cfg
    end)
    local new_global_cfg = table.deepcopy(old_cfg)
    new_global_cfg.failover_ping_timeout = 0.0000001
    vtest.router_cfg(g.router, new_global_cfg)
    local uuid = g.replica_1_a:replicaset_uuid()
    g.router:exec(function(uuid)
        local router = ivshard.router.internal.static_router
        for _, r in pairs(router.replicasets[uuid].replicas) do
            local s = r.worker.services['replica_failover']
            local opts = {on_yield = function()
                r.worker:wakeup_service('replica_failover')
            end}
            ivtest.wait_for_not_nil(s.data, 'info', opts)
            ivtest.service_wait_for_error(s.data.info, 'Ping error', opts)
        end
    end, {uuid})
    -- Since all nodes are broken, prioritized replica is not changed.
    router_assert_prioritized(g, g.replica_1_c)
    vtest.router_cfg(g.router, old_cfg)
end

local function failover_health_check_master_down(g)
    router_wait_prioritized(g, g.replica_1_c)
    local old_cfg = g.replica_1_a:exec(function()
        return ivshard.storage.internal.current_cfg
    end)
    g.replica_1_a:stop()

    local rs_uuid = g.replica_1_c:replicaset_uuid()
    local instance_uuid = g.replica_1_c:instance_uuid()
    g.router:exec(function(rs_uuid, uuid)
        rawset(_G, 'wait_for_failover', function(rs_uuid, uuid)
            local router = ivshard.router.internal.static_router
            local rs = router.replicasets[rs_uuid]
            local r = rs.replicas[uuid]
            local s = r.worker.services['replica_failover']
            local opts = {on_yield = function()
                r.worker:wakeup_service('replica_failover')
            end}
            ivtest.wait_for_not_nil(s.data, 'info', opts)
            ivtest.service_wait_for_new_ok(s.data.info, opts)
        end)
        _G.wait_for_failover(rs_uuid, uuid)
    end, {rs_uuid, instance_uuid})
    -- Since all nodes are broken, prioritized replica is not changed.
    router_assert_prioritized(g, g.replica_1_c)
    g.replica_1_a:start()
    g.replica_1_a:exec(function(cfg)
        ivshard.storage.cfg(cfg, box.info.uuid)
    end, {old_cfg})
    router_wait_prioritized(g, g.replica_1_c)
    -- Restore connection
    g.router:exec(function(rs_uuid, uuid)
        _G.wait_for_failover(rs_uuid, uuid)
        _G.wait_for_failover = nil
    end, {rs_uuid, g.replica_1_a:instance_uuid()})
end

local function failover_health_check_missing_master(g)
    router_wait_prioritized(g, g.replica_1_c)
    local old_cfg = g.replica_1_c:exec(function()
        return ivshard.storage.internal.current_cfg
    end)
    local new_cfg = table.deepcopy(old_cfg)
    for _, rs in pairs(new_cfg.sharding) do
        for _, r in pairs(rs.replicas) do
            r.master = false
        end
    end
    g.replica_1_c:exec(function(cfg)
        ivshard.storage.cfg(cfg, box.info.uuid)
    end, {new_cfg})

    local msg = 'The healthiness of the replica %s is unknown:'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.helpers.retrying({timeout = wait_timeout}, function()
        t.assert(g.router:grep_log(msg))
    end)

    -- Not changed. Not enough info.
    router_assert_prioritized(g, g.replica_1_c)
    g.replica_1_c:exec(function(cfg)
        ivshard.storage.cfg(cfg, box.info.uuid)
    end, {old_cfg})
end

local function failover_health_check_master_switch(g)
    router_wait_prioritized(g, g.replica_1_c)
    local box_cfg = g.replica_1_c:get_box_cfg()
    g.replica_1_c:update_box_cfg({replication = {
        g.replica_1_a.net_box_uri,
        g.replica_1_c.net_box_uri,
    }})
    -- Since the connection is not to master, it doesn't affect health.
    router_assert_prioritized(g, g.replica_1_c)
    -- In real life it will take 30 seconds to notice master change and
    -- unhealthiness of itself. Before the patch the non-master never knew,
    -- where the master currently is. Now, since we try to check status of
    -- master's upstream, we need to find this master in service_info via
    -- conn_manager. Since after that replica doesn't do any requests to
    -- master, the connection is collected by conn_manager in
    -- collect_idle_conns after 30 seconds. Then router's failover calls
    -- service_info one more time and non-master locates master, which
    -- may have already changed.
    local function replica_drop_conn_to_master(replica)
        replica:exec(function()
            local old_noactivity = ivconst.REPLICA_NOACTIVITY_TIMEOUT
            ivconst.REPLICA_NOACTIVITY_TIMEOUT = 0.1
            local master = ivshard.storage.internal.this_replicaset.master
            local name = 'replica_collect_idle_conns'
            ivtest.service_wait_for_new_ok(
                master.worker.services[name].data.info,
                {on_yield = function() master.worker:wakeup_service(name) end})
            ivconst.REPLICA_NOACTIVITY_TIMEOUT = old_noactivity
        end)
    end
    -- Change master and check that replica notices, that it's not healthy.
    replica_drop_conn_to_master(g.replica_1_c)
    g.replica_1_a:update_box_cfg{read_only = true}
    g.replica_1_b:update_box_cfg{read_only = false}
    router_wait_prioritized(g, g.replica_1_b)
    replica_drop_conn_to_master(g.replica_1_c)
    g.replica_1_a:update_box_cfg{read_only = false}
    g.replica_1_b:update_box_cfg{read_only = true}
    router_wait_prioritized(g, g.replica_1_c)

    g.replica_1_c:update_box_cfg{replication = box_cfg.replication}
end

--
-- gh-556: assertion in failover service is raised, if router is upgraded
-- earlier than storages.
--
local function failover_health_check_missing_health(g)
    router_wait_prioritized(g, g.replica_1_c)
    g.replica_1_c:exec(function()
        rawset(_G, 'old_call', ivshard.storage._call)
        ivshard.storage._call = function(...)
            local info = _G.old_call(...)
            info.health = nil
            return info
        end
    end)

    router_assert_prioritized(g, g.replica_1_c)
    local msg = 'The healthiness of the replica %s is unknown: ' ..
                'Health state in ping is missing'
    msg = string.format(msg, g.replica_1_c:instance_uuid())
    t.helpers.retrying({timeout = wait_timeout}, function()
        t.assert(g.router:grep_log(msg))
    end)

    g.replica_1_c:exec(function()
        ivshard.storage._call = _G.old_call
    end)
end

--
-- gh-453: router should not route requests to replicas, which
-- are not up to date. This requires properly working connection
-- with the master.
--
local function failover_health_check(g, auto_master)
    prepare_failover_health_check(g)

    if auto_master then
        local current_cfg = g.router:exec(function()
            return ivshard.router.internal.static_router.current_cfg
        end)
        for _, rs in pairs(current_cfg.sharding) do
            rs.master = 'auto'
            for _, r in pairs(rs.replicas) do
                r.master = nil
            end
        end
        vtest.router_cfg(g.router, current_cfg)

        current_cfg.weights = nil
        current_cfg.zone = nil
        vtest.cluster_cfg(g, current_cfg)
        g.replica_1_a:update_box_cfg{read_only = false}
        g.replica_1_b:update_box_cfg{read_only = true}
        g.replica_2_a:update_box_cfg{read_only = false}
        g.replica_2_b:update_box_cfg{read_only = true}
    end

    failover_health_check_missing_upstream(g)
    failover_health_check_broken_upstream(g)
    failover_health_check_big_lag(g)
    failover_health_check_small_failover_timeout(g)
    failover_health_check_missing_health(g)
    failover_health_check_master_down(g)
    if not auto_master then
        failover_health_check_missing_master(g)
    else
        failover_health_check_master_switch(g)
        vtest.cluster_cfg(g, global_cfg)
    end

    vtest.router_cfg(g.router, global_cfg)
    g.router:exec(function()
        ivconst.FAILOVER_UP_TIMEOUT = _G.old_failover_up_timeout
    end)
end

g.test_failover_health_check = function(g)
    failover_health_check(g, false)
end

g.test_failover_health_check_auto_master = function(g)
    failover_health_check(g, true)
end
