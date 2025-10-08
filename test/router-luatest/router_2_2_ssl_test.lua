local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local g = t.group('router_ssl')
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
                replica_2_b = {},
            },
        },
    },
    bucket_count = 100,
}
local global_cfg = vtest.config_new(cfg_template)

g.before_all(function()
    t.run_only_if(vutil.feature.ssl)
    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)

    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].is_ssl = true
    local new_global_cfg = vtest.config_new(new_cfg_template)

    g.router = vtest.router_new(g, 'router', new_global_cfg)
    vtest.cluster_cfg(g, new_global_cfg)
    vtest.router_cfg(g.router, new_global_cfg)

    local replica_cfg = new_global_cfg.sharding[g.replica_1_a:replicaset_uuid()]
                                      .replicas[g.replica_1_a:instance_uuid()]
    g.sensitive_ssl_data = {
        ssl_cert_file = replica_cfg.listen[1].params.ssl_cert_file,
        ssl_key_file = replica_cfg.listen[1].params.ssl_key_file,
        ssl_ca_file = replica_cfg.uri.params.ssl_ca_file,
    }
    vtest.cluster_wait_fullsync(g)
end)

g.after_all(function()
    g.cluster:drop()
end)

local function callrw_get_uuid(bid, timeout)
    timeout = timeout ~= nil and timeout or iwait_timeout
    return ivshard.router.callrw(bid, 'get_uuid', {}, {timeout = timeout})
end

local function assert_no_sensitive_data_in_server_logs(server, sensitive_data)
    assert(type(sensitive_data) == 'table')
    for sensitive_key, sensitive_value in pairs(sensitive_data) do
        t.assert_not(server:grep_log(sensitive_key))
        if type(sensitive_value) == 'table' then
            for _, sub_value in pairs(sensitive_value) do
                t.assert_not(server:grep_log(sub_value))
            end
        else
            t.assert_not(server:grep_log(sensitive_value))
        end
    end
end

g.test_no_ssl_sensitive_data_in_storage_logs = function(g)
    assert_no_sensitive_data_in_server_logs(g.replica_1_a,
                                            g.sensitive_ssl_data)
end

g.test_no_ssl_sensitive_data_in_router_logs = function(g)
    local replica_1_a_uri = g.router:exec(function(replicaset_id, master_uuid)
        local replicasets = ivshard.router.internal.static_router.replicasets
        local replica_1_a = replicasets[replicaset_id].replicas[master_uuid]
        return replica_1_a:safe_uri()
    end, {g.replica_1_a:replicaset_uuid(), g.replica_1_a:instance_uuid()})

    t.assert_not(string.match(replica_1_a_uri, 'password'))
    for sensitive_key, _ in pairs(g.sensitive_ssl_data) do
        t.assert_not(string.match(replica_1_a_uri, sensitive_key))
    end
    assert_no_sensitive_data_in_server_logs(g.router, g.sensitive_ssl_data)
end

g.test_ssl = function(g)
    -- So as not to assume where buckets are located, find first bucket of the
    -- first replicaset.
    local bid1 = vtest.storage_first_bucket(g.replica_1_a)
    local bid2 = vtest.storage_first_bucket(g.replica_2_a)

    -- Enable SSL everywhere.
    local new_cfg_template = table.deepcopy(cfg_template)
    local sharding_templ = new_cfg_template.sharding
    local rs_1_templ = sharding_templ[1]
    local rs_2_templ = sharding_templ[2]
    rs_1_templ.is_ssl = true
    rs_2_templ.is_ssl = true

    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)
    vtest.router_cfg(g.router, new_global_cfg)

    local rep_1_a_uuid = g.replica_1_a:instance_uuid()
    local res, err = g.router:exec(callrw_get_uuid, {bid1})
    t.assert_equals(err, nil)
    t.assert_equals(res, rep_1_a_uuid, 'went to 1_a')

    local rep_2_a_uuid = g.replica_2_a:instance_uuid()
    res, err = g.router:exec(callrw_get_uuid, {bid2})
    t.assert_equals(err, nil)
    t.assert_equals(res, rep_2_a_uuid, 'went to 2_a')

    -- Ensure that non-encrypted connection won't work.
    rs_2_templ.is_ssl = nil
    new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.router_cfg(g.router, new_global_cfg)

    res, err = g.router:exec(callrw_get_uuid, {bid2, 0.01})
    t.assert_equals(res, nil, 'rw failed on non-encrypted connection')
    t.assert_covers(err, {code = box.error.NO_CONNECTION}, 'got error')

    -- Works again when the replicaset also disables SSL.
    vtest.cluster_cfg(g, new_global_cfg)

    -- Force a reconnect right now instead of waiting until it happens
    -- automatically.
    vtest.router_disconnect(g.router)
    res, err = g.router:exec(callrw_get_uuid, {bid2})
    t.assert_equals(err, nil, 'no error')
    t.assert_equals(res, rep_2_a_uuid, 'went to 2_a')

    -- Restore everything back.
    vtest.cluster_cfg(g, global_cfg)
    vtest.router_cfg(g.router, global_cfg)
    vtest.cluster_wait_fullsync(g)
end
