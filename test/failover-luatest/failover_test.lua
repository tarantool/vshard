local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local g = t.group('failover')

g.after_all(function(g)
    g.cluster:stop()
end)


g.test_failover_priorities = function(g)
    local cfg = {
        sharding = {
            replicaset_1 = {
                replicas = {
                    replica_1 = { master = true, failover_priority = 1 },
                    replica_2 = { failover_priority = 5 },
                    replica_3 = { failover_priority = 3 },
                    replica_4 = { failover_priority = 2 },
                    replica_5 = { failover_priority = 4 },
                },
            },
        }
    }

    local global_cfg = vtest.config_new(cfg)

    vtest.cluster_new(g, global_cfg)
    g.router = vtest.router_new(g, 'router', global_cfg)
    local res, err = g.router:exec(function()
        return ivshard.router.bootstrap({timeout = iwait_timeout})
    end)
    t.assert(res and not err, 'bootstrap buckets')

    vtest.cluster_wait_vclock_all(g)

    local function check_current_replica(storage)
        g.router:exec(function(rs_uuid, instance_uuid)
            local router = ivshard.router.internal.static_router
            local rs = router.replicasets[rs_uuid]
            local new_master = rs.replicas[instance_uuid]
            ilt.helpers.retrying({timeout = 10}, function()
                if rs.replica == new_master then
                    return
                end
                error('Checking new replica')
            end)
        end, {storage:replicaset_uuid(), storage:instance_uuid()})
    end

    g.replica_1:stop()
    check_current_replica(g.replica_4)

    g.replica_4:stop()
    check_current_replica(g.replica_3)

    g.replica_3:stop()
    check_current_replica(g.replica_5)
end
