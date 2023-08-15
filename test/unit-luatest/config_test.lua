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

g.test_rebalancer_mode = function()
    local config = {
        sharding = {
            storage_1_uuid = {
                replicas = {
                    storage_1_a_uuid = {
                        uri = 'storage:storage@127.0.0.1:3301',
                        name = 'storage_1_a',
                    },
                },
            },
            storage_2_uuid = {
                replicas = {
                    storage_2_a_uuid = {
                        uri = 'storage:storage@127.0.0.1:3302',
                        name = 'storage_2_a',
                    },
                }
            }
        },
        rebalancer_mode = 'off',
    }
    t.assert(vcfg.check(config), 'mode = off')
    local sharding = config.sharding
    local storage_1_a = sharding.storage_1_uuid.replicas.storage_1_a_uuid
    storage_1_a.rebalancer = true
    t.assert(vcfg.check(config), 'mode = off with one marked')
    storage_1_a.rebalancer = nil

    config.rebalancer_mode = 'auto'
    t.assert(vcfg.check(config), 'mode = auto')
    storage_1_a.rebalancer = true
    t.assert(vcfg.check(config), 'mode = auto with one marked')
    storage_1_a.rebalancer = nil

    config.rebalancer_mode = 'manual'
    t.assert(vcfg.check(config), 'mode = manual')
    storage_1_a.rebalancer = true
    t.assert(vcfg.check(config), 'mode = manual with one marked')

    local storage_2_a = sharding.storage_2_uuid.replicas.storage_2_a_uuid
    storage_2_a.rebalancer = true
    t.assert_error_msg_contains('More than one rebalancer is found',
                                vcfg.check, config)
end
