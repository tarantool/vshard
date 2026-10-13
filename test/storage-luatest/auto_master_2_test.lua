local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local test_group = t.group('storage')

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
    },
    bucket_count = 20,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_bootstrap = function(g)
    g.replica_1_a:exec(function()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
    g.replica_1_b:exec(function()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
end

test_group.test_change = function(g)
    g.replica_1_a:exec(function()
        box.cfg{read_only = true}
        ifiber.yield()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
    g.replica_1_b:exec(function()
        box.cfg{read_only = false}
        ifiber.yield()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
end

test_group.test_turn_off_and_on = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    local rs_cfg = new_cfg_template.sharding[1]
    rs_cfg.master = nil
    rs_cfg.replicas.replica_1_a.read_only = nil
    rs_cfg.replicas.replica_1_b.read_only = nil
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    local function check_master_is_fully_off()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(not ivshard.storage.internal.this_replicaset.is_master_auto)
        box.cfg{read_only = false}
        ilt.assert(not box.info.ro)
        ilt.assert(not ivshard.storage.internal.is_master)
        box.cfg{read_only = true}
    end
    g.replica_1_a:exec(check_master_is_fully_off)
    g.replica_1_b:exec(check_master_is_fully_off)

    rs_cfg.replicas.replica_1_a.master = true
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    g.replica_1_a:exec(function()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(not ivshard.storage.internal.this_replicaset.is_master_auto)
        box.cfg{read_only = true}
        ilt.assert(box.info.ro)
        ilt.assert(ivshard.storage.internal.is_master)
        box.cfg{read_only = false}
    end)
    g.replica_1_b:exec(check_master_is_fully_off)

    vtest.cluster_cfg(g, global_cfg)
    g.replica_1_a:exec(function()
        ilt.assert(ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
    g.replica_1_b:exec(function()
        ilt.assert(not ivshard.storage.internal.is_master)
        ilt.assert(ivshard.storage.internal.this_replicaset.is_master_auto)
    end)
end

test_group.test_master_search_in_services = function(g)
    -- FIXME: this is probably the bug. Currently, the test fails on
    -- checking, that this_replicaset.master, because master search service
    -- drops master of the current replicaset.
    t.run_only_if(false)
    vtest.cluster_rebalancer_enable(g)
    g.replica_1_b:exec(function(uuid)
        -- Simulate router's ping, which forces the node to search for master.
        ivshard.storage._call('info', {timeout = 10, with_health = true})
        local internal = ivshard.storage.internal
        ilt.assert_not_equals(internal.this_replicaset.master, nil)
        ilt.assert_equals(internal.this_replicaset.master.id, uuid)
    end, {g.replica_1_a:instance_uuid()})

    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        ilt.assert_not_equals(internal.this_replicaset.master, nil)
        ilt.assert_equals(internal.this_replicaset.master.id, box.info.uuid)
        box.cfg{read_only = true}
        ilt.helpers.retrying({}, function()
            internal.instance_watch_fiber:wakeup()
            ilt.assert_not(internal.is_master)
            ilt.assert_equals(internal.this_replicaset.master, nil)
        end)
    end)

    g.replica_1_b:exec(function()
        local internal = ivshard.storage.internal
        box.cfg{read_only = false}
        ilt.helpers.retrying({}, function()
            internal.instance_watch_fiber:wakeup()
            ilt.assert(internal.is_master)
            ilt.assert_not_equals(internal.this_replicaset.master, nil)
            ilt.assert_equals(internal.this_replicaset.master.id, box.info.uuid)
        end)
        internal.rebalancer_fiber:wakeup()
    end)
    t.assert_not(g.replica_1_b:grep_log('reports self as both master'))

    g.replica_1_a:exec(function() box.cfg{read_only = false} end)
    g.replica_1_b:exec(function() box.cfg{read_only = true} end)
    vtest.cluster_rebalancer_disable(g)
end

--
-- If writability is lost while an on_master_enable callback yields, the
-- watcher must notice the new role after the callback returns.
--
test_group.test_master_change_during_enable = function(g)
    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        local was_read_only = box.info.ro
        box.cfg{read_only = true}
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_not(internal.is_master)
        end)

        local entered = ifiber.channel(1)
        local resume = ifiber.channel(1)
        local function on_enable()
            entered:put(true)
            resume:get()
        end
        ivshard.storage.on_master_enable(on_enable)
        box.cfg{read_only = false}
        ilt.assert(entered:get(iwait_timeout))
        box.cfg{read_only = true}
        ivshard.storage.on_master_enable(nil, on_enable)
        resume:put(true)
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_not(internal.is_master)
        end)

        box.cfg{read_only = was_read_only}
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_equals(internal.is_master, not was_read_only)
        end)
    end)
end

--
-- Switching to explicit master configuration must stop the old watcher even
-- while the role callback keeps cfg() suspended. Otherwise it can busy-loop.
--
test_group.test_turn_off_during_enable = function(g)
    g.replica_1_a:exec(function(cfg)
        local internal = ivshard.storage.internal
        box.cfg{read_only = true}
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_not(internal.is_master)
        end)

        local watcher = internal.instance_watch_fiber
        local new_cfg = table.deepcopy(cfg)
        local rs_cfg = new_cfg.sharding[internal.this_replicaset.id]
        rs_cfg.master = nil
        rs_cfg.replicas[box.info.uuid].master = true
        rs_cfg.replicas[box.info.uuid].read_only = false

        local is_checked = false
        local function on_enable()
            ilt.assert(internal.is_cfg_in_progress)
            ilt.assert_not(internal.this_replicaset.is_master_auto)
            ilt.helpers.retrying({timeout = iwait_timeout}, function()
                ilt.assert_equals(watcher:status(), 'dead')
            end)
            is_checked = true
        end
        ivshard.storage.on_master_enable(on_enable)
        ivshard.storage.cfg(new_cfg, box.info.uuid)
        ilt.assert_equals(internal.instance_watch_fiber, nil)
        ivshard.storage.on_master_enable(nil, on_enable)
        ilt.assert(is_checked)
        ivshard.storage.cfg(cfg, box.info.uuid)
    end, {global_cfg})
end

--
-- Failing role callbacks must not leave master services partly started or
-- stopped. The watcher reports the error after completing the role change.
--
test_group.test_master_trigger_errors = function(g)
    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        local service = internal.instance_watch_service
        local was_read_only = box.info.ro
        box.cfg{read_only = true}
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_not(internal.is_master)
        end)

        local function on_enable()
            error('injected master enable failure')
        end
        ivshard.storage.on_master_enable(on_enable)
        box.cfg{read_only = false}
        ivtest.service_wait_for_error(service,
                                      'injected master enable failure')
        ivshard.storage.on_master_enable(nil, on_enable)
        ilt.assert(internal.is_master)
        ilt.assert_not_equals(internal.collect_bucket_garbage_fiber, nil)
        ilt.assert_not_equals(internal.recovery_fiber, nil)
        ilt.assert_not_equals(internal.rebalancer_fiber, nil)
        ilt.assert(internal.master_sync_fiber ~= nil or
                   internal.is_bucket_in_sync)

        local function on_disable()
            error('injected master disable failure')
        end
        ivshard.storage.on_master_disable(on_disable)
        box.cfg{read_only = true}
        ivtest.service_wait_for_error(service,
                                      'injected master disable failure')
        ivshard.storage.on_master_disable(nil, on_disable)
        ilt.assert_not(internal.is_master)
        ilt.assert_equals(internal.collect_bucket_garbage_fiber, nil)
        ilt.assert_equals(internal.recovery_fiber, nil)
        ilt.assert_equals(internal.rebalancer_fiber, nil)
        ilt.assert_equals(internal.master_sync_fiber, nil)
        ilt.assert_not(internal.is_bucket_in_sync)

        box.cfg{read_only = was_read_only}
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_equals(internal.is_master, not was_read_only)
        end)
    end)
end
