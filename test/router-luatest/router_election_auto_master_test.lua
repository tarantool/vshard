local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local g = t.group('router')
local cfg_template = {
    sharding = {
        {
            master = 'auto',
            replicas = {
                replica_1_a = {
                    election_mode = 'candidate',
                },
                replica_1_b = {
                    election_mode = 'voter',
                },
                replica_1_c = {
                    election_mode = 'voter',
                },
            },
        },
        {
            master = 'auto',
            replicas = {
                replica_2_a = {
                    election_mode = 'candidate',
                },
                replica_2_b = {
                    election_mode = 'voter',
                },
                replica_2_c = {
                    election_mode = 'voter',
                },
            },
        },
    },
    bucket_count = 30,
    replication_timeout = 0.1,
    failover_ping_timeout = 1,
}

local global_cfg

g.before_all(function(g)
    -- `election_mode` was introduced only in 2.6.1.
    t.run_only_if(vutil.version_is_at_least(2, 6, 1, nil, 0, 0))
    global_cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(g, global_cfg)

    g.router = vtest.router_new(g, 'router', global_cfg)
    local res, err = g.router:exec(function()
        return ivshard.router.bootstrap({timeout = iwait_timeout})
    end)
    t.assert(res and not err, 'bootstrap buckets')

    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
    vtest.cluster_exec_each(g, function()
        ivconst.MASTER_SEARCH_WORK_INTERVAL = ivtest.busy_step
    end)
end)

g.after_all(function()
    g.cluster:drop()
end)

local function router_wait_for_leader(bid, uuid)
    ilt.helpers.retrying({timeout = ivtest.wait_timeout}, function()
        ivshard.router.master_search_wakeup()
        ilt.assert_equals(ivshard.router.route(bid).master.uuid, uuid)
    end)
end

local function test_master_search_template(g, stop, start)
    --
    -- Make replica_2_b a candidate and kill master of rs2.
    -- replica_2_b should become leader automatically.
    --
    local bid = g.replica_2_b:exec(function()
        box.cfg{election_mode = 'candidate', read_only = false}
        return _G.get_first_bucket()
    end)
    stop(g.replica_2_a)
    g.replica_2_b:wait_election_leader()
    g.router:exec(router_wait_for_leader, {bid, g.replica_2_b:instance_uuid()})

    --
    -- Restore previous master of rs2.
    --
    start(g.replica_2_a)
    g.replica_2_a:wait_vclock_of(g.replica_2_b)
    g.replica_2_b:exec(function()
        box.cfg{election_mode = 'voter', read_only = true}
    end)
    g.replica_2_a:wait_election_leader()
    g.router:exec(router_wait_for_leader, {bid, g.replica_2_a:instance_uuid()})

    --
    -- Check that master properly works after restoring.
    --
    g.router:exec(function(bid, master_uuid)
        local res = ivshard.router.callrw(bid, 'get_uuid')
        ilt.assert_equals(res, master_uuid)
    end, {bid, g.replica_2_a:instance_uuid()})
end

g.test_master_search_stop = function(g)
    local function server_stop(server)
        vtest.storage_stop(server)
    end
    local function server_start(server)
        vtest.storage_start(server, global_cfg)
    end
    test_master_search_template(g, server_stop, server_start)
end

g.test_master_search_freeze = function(g)
    local function server_freeze(server)
        server:freeze()
    end
    local function server_thaw(server)
        server:thaw()
    end
    test_master_search_template(g, server_freeze, server_thaw)
end
