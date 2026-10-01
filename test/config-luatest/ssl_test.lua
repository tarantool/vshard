local t = require('luatest')
local fio = require('fio')
local vutil = require('vshard.util')
-- cbuilder/cluster may be missing, if old luatest version is used.
local ok_cbuilder, cbuilder = pcall(require, 'luatest.cbuilder')
local ok_cluster, cluster = pcall(require, 'luatest.cluster')

local g = t.group('sharding_ssl')

local function ssl_uri_params(cert_dir, ca_name, cert_name)
    return {
        transport = 'ssl',
        ssl_ca_file = fio.pathjoin(cert_dir, ca_name .. '.crt'),
        ssl_cert_file = fio.pathjoin(cert_dir, cert_name .. '.crt'),
        ssl_key_file = fio.pathjoin(cert_dir, cert_name .. '.key'),
    }
end

local function ssl_config_params(cert_dir, ca_name, cert_name)
    return {
        ca_file = fio.pathjoin(cert_dir, ca_name .. '.crt'),
        ssl_cert = fio.pathjoin(cert_dir, cert_name .. '.crt'),
        ssl_key = fio.pathjoin(cert_dir, cert_name .. '.key'),
    }
end

local function assert_vshard_uris(instance, expected, is_router)
    instance:exec(function(expected, is_router)
        local vshard = require('vshard')
        local cfg
        if is_router then
            cfg = vshard.router.internal.static_router.current_cfg
        else
            cfg = vshard.storage.internal.current_cfg
        end
        for _, replicaset in pairs(cfg.sharding) do
            for _, replica in pairs(replicaset.replicas) do
                t.assert_equals(replica.uri.params, expected)
            end
        end
    end, {expected, is_router})
end

local function make_config(cert_dir)
    return cbuilder:new()
        :set_global_option('credentials.users.guest.roles', {'super'})
        :set_global_option('credentials.users.client.password', 'secret')
        :set_global_option('credentials.users.client.roles', {'super'})
        :set_global_option('credentials.users.storage.password', 'storage')
        :set_global_option('credentials.users.storage.roles', {'sharding'})
        :set_global_option('iproto.listen', {{
            uri = 'unix/:./{{ instance_name }}.iproto',
            params = {transport = 'ssl'},
        }})
        :set_global_option('iproto.advertise.sharding', {
            uri = 'unix/:./{{ instance_name }}.iproto',
            login = 'storage',
            params = {transport = 'ssl'},
        })
        :set_global_option('sharding.bucket_count', 10)
        :use_group('group')
        :use_replicaset('storage-1')
        :set_replicaset_option('sharding.roles', {'storage'})
        :add_instance('storage-a', {database = {mode = 'rw'}})
        :set_instance_option('storage-a', 'iproto.ssl',
                             ssl_config_params(cert_dir, 'ca', 'storage-a'))
        :add_instance('storage-b', {})
        :set_instance_option('storage-b', 'iproto.ssl',
                             ssl_config_params(cert_dir, 'ca', 'storage-b'))
        :use_replicaset('storage-2')
        :set_replicaset_option('sharding.roles', {'storage'})
        :add_instance('storage-c', {database = {mode = 'rw'}})
        :set_instance_option('storage-c', 'iproto.ssl',
                             ssl_config_params(cert_dir, 'ca', 'storage-c'))
        :use_replicaset('router')
        :set_replicaset_option('sharding.roles', {'router'})
        :add_instance('router', {database = {mode = 'rw'}})
        :set_instance_option('router', 'iproto.ssl',
                             ssl_config_params(cert_dir, 'ca', 'router'))
        :config()
end

local function set_control_uri(g, instance, server_ca, client_cert)
    -- Luatest's control connection also has to present a certificate trusted by
    -- the instance's CA, hence we overload the connection parameters here.
    instance.net_box_uri = {
        uri = ('unix/:%s/%s.iproto'):format(g.cluster._dir,
                                            instance.alias),
        params = ssl_uri_params(g.cert_dir, server_ca, client_cert),
    }
end

g.before_all(function(g)
    t.run_only_if(ok_cbuilder and ok_cluster, 'cbuilder is not available')
    t.run_only_if(vutil.feature.ssl, 'SSL is not available')

    local root = fio.abspath(os.getenv('SOURCEDIR') or '.')
    g.cert_dir = fio.pathjoin(root, 'test/certs/mtls')
    g.cluster = cluster:new(make_config(g.cert_dir), nil,
                            {auto_cleanup = false})
    g.cluster:each(function(instance)
        set_control_uri(g, instance, 'ca', instance.alias)
    end)
    g.cluster:start()
end)

g.after_all(function(g)
    g.cluster:drop()
end)

--
-- Test that when configured via config module, vshard receives correct ssl
-- parameters for each connection and is actually able to connect and serve
-- requests (gh-615).
--
g.test_ssl_config_params_are_used_for_sharding_uris = function(g)
    assert_vshard_uris(g.cluster['storage-a'],
                       ssl_uri_params(g.cert_dir, 'ca', 'storage-a'),
                       false)
    assert_vshard_uris(g.cluster['storage-b'],
                       ssl_uri_params(g.cert_dir, 'ca', 'storage-b'),
                       false)
    assert_vshard_uris(g.cluster['storage-c'],
                       ssl_uri_params(g.cert_dir, 'ca', 'storage-c'),
                       false)
    assert_vshard_uris(g.cluster.router,
                       ssl_uri_params(g.cert_dir, 'ca', 'router'),
                       true)

    for _, storage in pairs({g.cluster['storage-a'],
                             g.cluster['storage-b']}) do
        storage:exec(function(expected)
            for _, uri in pairs(box.cfg.replication) do
                t.assert_equals(uri.params, expected)
            end
        end, {ssl_uri_params(g.cert_dir, 'ca', storage.alias)})
    end

    local storage_a_uuid = g.cluster['storage-a']:exec(function()
        rawset(_G, 'get_uuid', function()
            return box.info.uuid
        end)
        box.schema.func.create('get_uuid')
        box.schema.role.grant('public', 'execute', 'function', 'get_uuid')
        return box.info.uuid
    end)
    local storage_b_uuid = g.cluster['storage-b']:exec(function()
        rawset(_G, 'get_uuid', function()
            return box.info.uuid
        end)
        t.helpers.retrying({timeout = 30}, function()
            t.assert_not_equals(box.func.get_uuid, nil)
        end)
        return box.info.uuid
    end)

    g.cluster.router:exec(function()
        local vshard = require('vshard')
        local ok, err = vshard.router.bootstrap({timeout = 30})
        t.assert_equals(err, nil)
        t.assert_equals(ok, true)
    end)

    local bucket_id = g.cluster['storage-a']:exec(function()
        local bucket_id
        t.helpers.retrying({timeout = 30}, function()
            local vshard = require('vshard')
            local bucket = box.space._bucket.index.status:min(
                {vshard.consts.BUCKET.ACTIVE})
            t.assert_not_equals(bucket, nil)
            bucket_id = bucket.id
        end)
        return bucket_id
    end)

    -- Make sure router.call works as expected.
    g.cluster.router:exec(function(bucket_id, storage_a_uuid, storage_b_uuid)
        local vshard = require('vshard')
        local res, err
        t.helpers.retrying({timeout = 30}, function()
            res, err = vshard.router.callrw(bucket_id, 'get_uuid', {},
                                            {timeout = 30})
            t.assert_equals(err, nil)
            t.assert_equals(res, storage_a_uuid)
        end)

        t.helpers.retrying({timeout = 30}, function()
            res, err = vshard.router.callre(bucket_id, 'get_uuid', {},
                                            {timeout = 30})
            t.assert_equals(err, nil)
            t.assert_equals(res, storage_b_uuid)
        end)
    end, {bucket_id, storage_a_uuid, storage_b_uuid})

    -- Make sure rebalancing works.
    g.cluster['storage-a']:exec(function(bucket_id)
        t.helpers.retrying({timeout = 60}, function()
            local vshard = require('vshard')
            local ok, err = vshard.storage.bucket_send(bucket_id, 'storage-2', {
                timeout = 30,
            })
            t.assert_equals(err, nil)
            t.assert_equals(ok, true)
        end)
    end, {bucket_id})
    g.bucket_moved = true
    g.cluster['storage-c']:exec(function(bucket_id)
        t.helpers.retrying({timeout = 30}, function()
            local bucket = box.space._bucket:get(bucket_id)
            t.assert_not_equals(bucket, nil)
            t.assert_equals(bucket.status,
                            require('vshard.consts').BUCKET.ACTIVE)
        end)
    end, {bucket_id})
end

g.after_test('test_ssl_config_params_are_used_for_sharding_uris', function(g)
    if not g.bucket_moved then
        return
    end
    g.bucket_moved = nil

    -- Let the rebalancer restore the balance and GC collect the old buckets.
    for _, storage in ipairs({g.cluster['storage-a'],
                              g.cluster['storage-c']}) do
        storage:exec(function()
            t.helpers.retrying({timeout = 30}, function()
                local vshard = require('vshard')
                vshard.storage.rebalancer_wakeup()
                local buckets = box.space._bucket
                t.assert_equals(buckets.index.status:count(
                                    {vshard.consts.BUCKET.ACTIVE}), 5)
                t.assert_equals(buckets:count(), 5)
            end)
        end)
    end
end)

local group_config = {
    {
        router_control_ca = 'bad-ca',
        router_cert = 'bad',
        storage_control_ca = 'ca',
        storage_cert = 'storage-a',
        expect_success = false,
    },
    {
        router_control_ca = 'ca',
        router_cert = 'router',
        storage_control_ca = 'bad-ca',
        storage_cert = 'bad',
        expect_success = false,
    },
    {
        router_control_ca = 'ca',
        router_cert = 'router',
        storage_control_ca = 'ca',
        storage_cert = 'storage-a',
        expect_success = true,
    },
}

local g_mtls = t.group('sharding_mtls', group_config)

local function make_mtls_config(cert_dir, router_cert, storage_cert)
    return cbuilder:new()
        :set_global_option('credentials.users.client.password', 'secret')
        :set_global_option('credentials.users.client.roles', {'super'})
        :set_global_option('credentials.users.storage.password', 'storage')
        :set_global_option('credentials.users.storage.roles', {'sharding'})
        :set_global_option('iproto.listen', {{
            uri = 'unix/:./{{ instance_name }}.iproto',
            params = {transport = 'ssl'},
        }})
        :set_global_option('iproto.advertise.sharding', {
            uri = 'unix/:./{{ instance_name }}.iproto',
            login = 'storage',
            params = {transport = 'ssl'},
        })
        :set_global_option('sharding.bucket_count', 1)
        :use_group('group')
        :use_replicaset('storage')
        :set_replicaset_option('sharding.roles', {'storage'})
        :add_instance('storage', {database = {mode = 'rw'}})
        :set_instance_option('storage', 'iproto.ssl',
                             ssl_config_params(cert_dir, 'ca', storage_cert))
        :use_replicaset('router')
        :set_replicaset_option('sharding.roles', {'router'})
        :add_instance('router', {database = {mode = 'rw'}})
        :set_instance_option('router', 'iproto.ssl',
                             ssl_config_params(cert_dir, 'ca', router_cert))
        :config()
end

g_mtls.before_each(function(g)
    t.run_only_if(ok_cbuilder and ok_cluster, 'cbuilder is not available')
    t.run_only_if(vutil.feature.ssl, 'SSL is not available')
    local root = fio.abspath(os.getenv('SOURCEDIR') or '.')
    g.cert_dir = fio.pathjoin(root, 'test/certs/mtls')
end)

g_mtls.after_each(function(g)
    if g.cluster ~= nil then
        g.cluster:drop()
    end
end)

g_mtls.test_certificates = function(g)
    g.cluster = cluster:new(make_mtls_config(g.cert_dir, g.params.router_cert,
                                             g.params.storage_cert), nil,
                            {auto_cleanup = false})
    set_control_uri(g, g.cluster.router, g.params.router_control_ca,
                    'storage-a')
    set_control_uri(g, g.cluster.storage, g.params.storage_control_ca, 'router')
    g.cluster:start()

    if g.params.expect_success then
        g.cluster.router:exec(function()
            t.helpers.retrying({timeout = 30}, function()
                local ok, err =
                    require('vshard').router.bootstrap({timeout = 5})
                t.assert_equals(err, nil)
                t.assert_equals(ok, true)
            end)
        end)
    else
        local ok, err = g.cluster.router:exec(function()
            return require('vshard').router.bootstrap({timeout = 1})
        end)
        t.assert_equals(ok, nil)
        -- The exact connection error is not reported, only the fact that the
        -- master couldn't be found.
        t.assert_equals(err.name, 'MISSING_MASTER')
        t.assert_equals(err.message,
                        'Master is not configured for replicaset storage')
    end
end
