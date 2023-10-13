local t = require('luatest')
local vcfg = require('vshard.cfg')
local vutil = require('vshard.util')
local luuid = require('uuid')

local g = t.group('config')

g.test_replica_uri = function()
    local url = 'storage:storage@127.0.0.1:3301'
    local storage_1_a = {
        uri = url,
        name = 'storage_1_a',
    }
    local config = {
        sharding = {
            storage_1_uuid = {
                replicas = {
                    storage_1_a_uuid = storage_1_a,
                }
            },
        },
    }
    t.assert(vcfg.check(config), 'normal uri')

    if vutil.feature.multilisten then
        storage_1_a.uri = {url}
        t.assert(vcfg.check(config), 'table uri')

        storage_1_a.uri = {url, params = {transport = 'plain'}}
        t.assert(vcfg.check(config), 'uri with options')
    else
        storage_1_a.uri = {url}
        t.assert_error(vcfg.check, config)

        storage_1_a.uri = {url, params = {transport = 'plain'}}
        t.assert_error(vcfg.check, config)
    end
    storage_1_a.uri = 3301
    t.assert(vcfg.check(config), 'number uri')

    storage_1_a.uri = 'bad uri ###'
    t.assert_error(vcfg.check, config)

    storage_1_a.uri = ''
    t.assert_error(vcfg.check, config)

    storage_1_a.uri = {}
    t.assert_error(vcfg.check, config)

    storage_1_a.uri = nil
    t.assert_error(vcfg.check, config)

    storage_1_a.uri = luuid.new()
    t.assert_error(vcfg.check, config)
end

g.test_replica_listen = function()
    t.run_only_if(vutil.feature.multilisten)

    local url1 = 'storage:storage@127.0.0.1:3301'
    local url2 = 'storage:storage@127.0.0.1:3302'
    local storage_1_a = {
        uri = url1,
        listen = url2,
        name = 'storage_1_a',
    }
    local config = {
        sharding = {
            storage_1_uuid = {
                replicas = {
                    storage_1_a_uuid = storage_1_a,
                }
            },
        },
    }

    -- Simple listen with one URI.
    local res = vcfg.check(config)
    t.assert(res, 'listen and uri are both specified')

    local rep_1_a = res.sharding.storage_1_uuid.replicas.storage_1_a_uuid
    t.assert_equals(rep_1_a.uri, url1, 'uri value')
    t.assert_equals(rep_1_a.listen, url2, 'listen value')

    -- Listen can by multiple.
    storage_1_a.listen = {url1, url2}
    res = vcfg.check(config)
    t.assert(res, 'listen is array')

    rep_1_a = res.sharding.storage_1_uuid.replicas.storage_1_a_uuid
    t.assert_equals(rep_1_a.uri, url1, 'uri value')
    t.assert_equals(rep_1_a.listen, {url1, url2}, 'listen value')
end

g.test_extract_vshard = function()
    local config = {
        sharding = 1,
        weights = 2,
        bucket_count = 3,
        listen = 4,
        replication = 5,
        read_only = 6,
        test = 7,
    }
    t.assert_equals(vcfg.extract_vshard(config), {
        sharding = 1,
        weights = 2,
        bucket_count = 3,
    })
    -- Empty instance config.
    t.assert_equals(vcfg.extract_box(config, {}), {
        listen = 4,
        replication = 5,
        read_only = 6,
        -- Unknown options are all considered belonging to box.
        test = 7,
    })
    -- Merge with the instance config.
    t.assert_equals(vcfg.extract_box(config, {
        read_only = 8,
        listen = 9,
        replication_timeout = 10
    }), {
        listen = 9,
        replication = 5,
        read_only = 8,
        test = 7,
        replication_timeout = 10,
    })
end
