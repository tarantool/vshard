local t = require('luatest')
local vutil = require('vshard.util')
-- cbuilder/cluster may be missing, if old luatest version is used.
local ok_cbuilder, cbuilder = pcall(require, 'luatest.cbuilder')
local ok_cluster, cluster = pcall(require, 'luatest.cluster')

local ROLE = 'roles.recovery-point-manager'

local g = t.group()

local function make_config(rpm_cfg, opts)
    opts = opts or {}
    local builder = cbuilder:new()
        :set_global_option('credentials.users.storage.roles', {'sharding'})
        :set_global_option('credentials.users.storage.password', 'secret')
        :set_global_option('iproto.advertise.sharding.login', 'storage')
        :set_global_option('replication.failover', 'manual')
        :set_global_option('sharding.bucket_count', 10)
        :set_global_option('roles_cfg', {[ROLE] = rpm_cfg})
        :use_group('g')

        -- Storage shard.
        :use_replicaset('storage')
        :set_replicaset_option('sharding.roles', {'storage'})
        :set_replicaset_option('leader', 'storage1')
        :add_instance('storage1', {})

        -- Router.
        :use_replicaset('router')
        :set_replicaset_option('sharding.roles', {'router'})
        :set_replicaset_option('leader', 'router1')
        :add_instance('router1', {})

    -- Enable the recovery point manager role. By default on the router.
    if opts.role ~= false then
        builder:use_replicaset(opts.role_replicaset or 'router')
               :set_replicaset_option('roles', {ROLE})
    end
    return builder:config()
end

-- The sorted names of the managers the role created on the router.
local function manager_names()
    return g.cluster.router1:exec(function()
        local names = {}
        for name in pairs(box.backup.recovery_point.managers) do
            table.insert(names, name)
        end
        table.sort(names)
        return names
    end)
end

-- Wait until a manager has created at least one point.
local function wait_point(name)
    g.cluster.router1:exec(function(name)
        t.helpers.retrying({timeout = 30}, function()
            local m = box.backup.recovery_point.managers[name]
            t.assert_not_equals(m, nil)
            t.assert_not_equals(m:info().last_point, nil)
        end)
    end, {name})
end

local function reload(rpm_cfg, opts)
    g.cluster:reload(make_config(rpm_cfg, opts))
end

g.before_all(function()
    t.run_only_if(ok_cbuilder and ok_cluster, 'cbuilder is not available')
    t.run_only_if(vutil.feature.recovery_point, 'no recovery_point API')
    g.cluster = cluster:new(make_config({managers = {}}), nil,
                            {auto_cleanup = false})
    g.cluster:start()
    -- Bootstrap the buckets and wait until they are all discovered, so a
    -- full-cluster map_callrw (which the backend uses) can ref every storage.
    g.cluster.router1:exec(function()
        require('vshard').router.bootstrap({timeout = 30})
        t.helpers.retrying({timeout = 30}, function()
            local info = require('vshard').router.info()
            t.assert_equals(info.bucket.unknown, 0)
            t.assert_gt(info.bucket.available_rw, 0)
        end)
    end)
end)

g.after_all(function()
    g.cluster:drop()
end)

g.after_each(function()
    t.assert_equals(manager_names(), {})
end)

g.test_default_manager = function()
    reload({
        backend = 'vshard-router',
        create = {by = {interval = 0.1}},
    })
    t.assert_equals(manager_names(), {'default'})
    wait_point('default')

    -- The backend surfaces its type and the last cluster point under the
    -- manager info; the point is keyed by replicaset id.
    local info = g.cluster.router1:exec(function()
        return box.backup.recovery_point.managers.default:info()
    end)
    t.assert_equals(info.backend.backend_type, 'vshard-router')
    t.assert_not_equals(info.last_point, nil)
    t.assert_not_equals(next(info.last_point), nil)

    -- The point actually landed in the _recovery_point space.
    local count = g.cluster.storage1:exec(function()
        return box.space._recovery_point:count()
    end)
    t.assert_gt(count, 0)

    reload({managers = {}})
end

g.test_requires_router_role = function()
    t.assert_error_msg_contains('sharding role is required', function()
        reload({backend = 'vshard-router'}, {role_replicaset = 'storage'})
    end)
    -- Restore a healthy config so the other tests still see the cluster.
    reload({managers = {}})
end

g.test_stop = function()
    local cfg = {
        backend = 'vshard-router',
        create = {by = {interval = 0.1}},
    }
    reload(cfg)
    t.assert_equals(manager_names(), {'default'})
    reload(cfg, {role = false})
    t.assert_equals(manager_names(), {})
    reload({managers = {}})
end
