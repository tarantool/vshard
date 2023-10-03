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

g.test_rebalancer_flag = function()
    local storage_1_a = {
        uri = 'storage:storage@127.0.0.1:3301',
        name = 'storage_1_a',
    }
    local replicaset_1 = {
        replicas = {
            storage_1_a_uuid = storage_1_a,
        },
    }
    local storage_2_a = {
        uri = 'storage:storage@127.0.0.1:3302',
        name = 'storage_2_a',
    }
    local replicaset_2 = {
        replicas = {
            storage_2_a_uuid = storage_2_a,
        },
    }
    local config = {
        sharding = {
            storage_1_uuid = replicaset_1,
            storage_2_uuid = replicaset_2,
        },
    }
    t.assert(vcfg.check(config))
    --
    -- Bad replica-rebalancer flag.
    --
    storage_1_a.rebalancer = 'test'
    t.assert_error_msg_content_equals(
        'Rebalancer flag must be boolean', vcfg.check, config)
    storage_1_a.rebalancer = nil
    --
    -- Bad replicaset-rebalancer flag.
    --
    replicaset_1.rebalancer = 'test'
    t.assert_error_msg_content_equals(
        'Rebalancer flag must be boolean', vcfg.check, config)
    replicaset_1.rebalancer = nil
    --
    -- Rebalancer flag for a replicaset and an instance.
    --
    storage_1_a.rebalancer = true
    replicaset_1.rebalancer = true
    t.assert_error_msg_content_equals(
        'Found 2 rebalancer flags at storage_1_uuid and storage_1_a_uuid',
        vcfg.check, config)
    storage_1_a.rebalancer = nil
    replicaset_1.rebalancer = nil
    --
    -- Rebalancer flag for 2 replicasets.
    --
    replicaset_1.rebalancer = true
    replicaset_2.rebalancer = true
    t.assert_error_msg_content_equals(
        'Found 2 rebalancer flags at storage_1_uuid and storage_2_uuid',
        vcfg.check, config)
    replicaset_1.rebalancer = nil
    replicaset_2.rebalancer = nil
    --
    -- Rebalancer flag for 2 instances.
    --
    storage_1_a.rebalancer = true
    storage_2_a.rebalancer = true
    t.assert_error_msg_content_equals(
        'Found 2 rebalancer flags at storage_1_a_uuid and storage_2_a_uuid',
        vcfg.check, config)
    storage_1_a.rebalancer = nil
    storage_2_a.rebalancer = nil
    --
    -- Conflicting rebalancer flag in one replicaset.
    --
    replicaset_1.rebalancer = false
    storage_1_a.rebalancer = true
    t.assert_error_msg_content_equals(
        'Replicaset storage_1_uuid can\'t run the rebalancer, and yet it was '..
        'explicitly assigned to its instance storage_1_a_uuid',
        vcfg.check, config)
    replicaset_1.rebalancer = nil
    storage_1_a.rebalancer = nil
end

g.test_rebalancer_mode = function()
    local storage_1_a = {
        uri = 'storage:storage@127.0.0.1:3301',
        name = 'storage_1_a',
    }
    local replicaset_1 = {
        replicas = {
            storage_1_a_uuid = storage_1_a,
        },
    }
    local config = {
        sharding = {
            storage_1_uuid = replicaset_1,
        },
    }
    t.assert(vcfg.check(config))

    local function check_all_flag_combinations()
        t.assert(vcfg.check(config))
        storage_1_a.rebalancer = true
        t.assert(vcfg.check(config))
        storage_1_a.rebalancer = nil
        replicaset_1.rebalancer = true
        t.assert(vcfg.check(config))
        replicaset_1.rebalancer = false
        t.assert(vcfg.check(config))
        replicaset_1.rebalancer = nil
        storage_1_a.rebalancer = false
        t.assert(vcfg.check(config))
        storage_1_a.rebalancer = nil
    end
    config.rebalancer_mode = 'auto'
    check_all_flag_combinations()
    config.rebalancer_mode = 'manual'
    check_all_flag_combinations()
    config.rebalancer_mode = 'off'
    check_all_flag_combinations()
end
