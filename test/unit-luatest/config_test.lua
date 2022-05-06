local t = require('luatest')
local vcfg = require('vshard.cfg')
local vutil = require('vshard.util')
local luuid = require('uuid')

local g = t.group('config')

g.test_basic_uri = function()
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
