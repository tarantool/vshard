#!/usr/bin/env tarantool

require('strict').on()
fiber = require('fiber')

-- Check if we are running under test-run
if os.getenv('ADMIN') then
    test_run = require('test_run').new()
    require('console').listen(os.getenv('ADMIN'))
end

replicasets = {'cbf06940-0790-498b-948d-042b62cf3d29',
               'ac522f65-aa94-4134-9f64-51ee384f1a54'}

-- Call a configuration provider
cfg = dofile('localcfg.lua')
if arg[1] == 'discovery_disable' then
    cfg.discovery_mode = 'off'
end

-- Start the database with sharding
vshard = require('vshard')

vshard.router.cfg(cfg)

function failover_wakeup(router)
    local router = router or vshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.worker:wakeup_service('failover')
        for _, r in pairs(rs.replicas) do
            r.worker:wakeup_service('failover')
        end
    end
end

if arg[2] == 'failover_disable' then
    local replicasets = vshard.router.internal.static_router.replicasets
    for _, rs in pairs(replicasets) do
        rs.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY = true
        for _, r in pairs(rs.replicas) do
            r.errinj.ERRINJ_REPLICA_FAILOVER_DELAY = true
        end
    end
    -- Wait for stop.
    local all_stopped
    repeat
        all_stopped = true
        failover_wakeup()
        for _, rs in pairs(replicasets) do
            all_stopped = rs.errinj.ERRINJ_REPLICASET_FAILOVER_DELAY == 'in'
            for _, r in pairs(rs.replicas) do
                all_stopped = r.errinj.ERRINJ_REPLICA_FAILOVER_DELAY == 'in'
            end
        end
    until not all_stopped
end
