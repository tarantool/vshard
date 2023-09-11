local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{engine = 'memtx'}, {engine = 'vinyl'}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {
        engine = 'memtx', memtx_use_mvcc_engine = true
    })
    table.insert(group_config, {
        engine = 'vinyl', memtx_use_mvcc_engine = true
    })
end

local test_group = t.group('storage', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
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
    bucket_count = 10
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Test that services for all background fibers are created
-- and work properly (gh-107).
--
test_group.test_basic_storage_service_info = function(g)
    local uuid = g.replica_1_a:exec(function()
        -- Test that all services save states
        local info = ivshard.storage.info({with_services = true})
        ilt.assert_not_equals(info.services, nil)
        ilt.assert_not_equals(info.services.gc, nil)
        ilt.assert_not_equals(info.services.recovery, nil)
        ilt.assert_not_equals(info.services.rebalancer, nil)
        -- Routes applier service is created as soon as it's needed
        ilt.assert_equals(info.services.routes_applier, nil)

        -- Forbid routes_apply service to die
        local internal = ivshard.storage.internal
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = true
        -- Break timeout in order to get error
        rawset(_G, 'chunk_timeout', ivconst.REBALANCER_CHUNK_TIMEOUT)
        ivconst.REBALANCER_CHUNK_TIMEOUT = 1e-6
        return ivutil.replicaset_uuid()
    end)

    g.replica_2_a:exec(function(uuid)
        -- Send bucket to create disbalance in
        -- order to test routes applier service
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
    end, {uuid})

    vtest.cluster_rebalancer_enable(g)

    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        local applier_name = 'routes_applier_service'
        ivtest.wait_for_not_nil(internal, applier_name)
        local service = internal[applier_name]
        ivtest.service_wait_for_error(service, 'Timed?[Oo]ut')

        -- Restore everything
        ivconst.REBALANCER_CHUNK_TIMEOUT = _G.chunk_timeout
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = false
        ivtest.wait_for_nil(internal, applier_name)
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = true

        -- All buckets must be recovered to the ACTIVE state,
        -- otherwise rebalancer won't work.
        ivshard.storage.recovery_wakeup()
        service = ivshard.storage.internal.recovery_service
        ivtest.service_wait_for_new_ok(service)
    end)

    g.replica_2_a:exec(function()
        ivshard.storage.recovery_wakeup()
        local service = ivshard.storage.internal.recovery_service
        ivtest.service_wait_for_new_ok(service)
    end)

    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        local applier_name = 'routes_applier_service'
        ivtest.wait_for_not_nil(internal, applier_name,
            {on_yield = ivshard.storage.rebalancer_wakeup})

        -- Everything is all right now
        ivtest.service_wait_for_ok(internal[applier_name])
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = false

        ivtest.service_wait_for_ok(internal.rebalancer_service,
            {on_yield = ivshard.storage.rebalancer_wakeup})
    end)

    -- Cleanup
    vtest.cluster_rebalancer_disable(g)
end
