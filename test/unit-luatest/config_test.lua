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

g.test_enum = function()
    local config = {
        sharding = {
            storage_1_uuid = {
                replicas = {}
            },
        },
    }
    t.assert(vcfg.check(config), 'normal config')

    config.sharding.storage_1_uuid.master = 'auto'
    t.assert(vcfg.check(config))

    config.sharding.storage_1_uuid.master = 'bad'
    t.assert_error_msg_content_equals(
        "Master search mode must be enum {'auto', nil}",
        vcfg.check, config)

    config.sharding.storage_1_uuid.master = nil
    for _, v in pairs({'on', 'off', 'once'}) do
        config.discovery_mode = v
        t.assert(vcfg.check(config))
    end
    config.discovery_mode = 'bad'
    t.assert_error_msg_content_equals(
        "Discovery mode must be enum {'on', 'off', 'once', nil}",
        vcfg.check, config)
    config.discovery_mode = nil

    for _, v in pairs({'auto', 'manual', 'off'}) do
        config.rebalancer_mode = v
        t.assert(vcfg.check(config))
    end
    config.rebalancer_mode = 'bad'
    t.assert_error_msg_content_equals(
        "Rebalancer mode must be enum {'auto', 'manual', 'off', nil}",
        vcfg.check, config)
    config.rebalancer_mode = nil

    for _, v in pairs({'auto', 'manual'}) do
        config.box_cfg_mode = v
        t.assert(vcfg.check(config))
    end
    config.box_cfg_mode = 'bad'
    t.assert_error_msg_content_equals(
        "Box.cfg mode must be enum {'auto', 'manual', nil}",
        vcfg.check, config)
    config.box_cfg_mode = nil

    for _, v in pairs({'auto', 'manual_access'}) do
        config.schema_management_mode = v
        t.assert(vcfg.check(config))
    end
    config.schema_management_mode = 'bad'
    t.assert_error_msg_content_equals(
        "Schema management mode must be enum {'auto', 'manual_access', nil}",
        vcfg.check, config)
    config.schema_management_mode = nil
end

g.test_enum_identification_mode = function()
    t.run_only_if(vutil.feature.persistent_names)
    local config = {
        sharding = {
            storage_1_uuid = {
                replicas = {}
            },
        },
    }
    -- Test enum identification_mode.
    for _, v in pairs({'uuid_as_key', 'name_as_key'}) do
        config.identification_mode = v
        t.assert(vcfg.check(config))
    end
    config.identification_mode = 'bad'
    t.assert_error_msg_content_equals(
        "Config identification mode must be enum " ..
        "{'uuid_as_key', 'name_as_key', nil}",
        vcfg.check, config)
    config.identification_mode = nil
end

g.test_identification_mode_name_as_key = function()
    t.run_only_if(vutil.feature.persistent_names)
    local storage_1_a = {
        uuid = 'storage_1_a_uuid',
        uri = 'storage:storage@127.0.0.1:3301',
    }
    local replicaset_1 = {
        uuid = 'replicaset_1_uuid',
        replicas = {
            storage_1_a = storage_1_a,
        },
    }
    local config = {
        identification_mode = 'name_as_key',
        sharding = {
            replicaset_1 = replicaset_1,
        },
    }

    -- UUID is optional.
    storage_1_a.uuid = nil
    replicaset_1.uuid = nil
    t.assert(vcfg.check(config))

    -- replica.name is forbidden.
    storage_1_a.name = 'name'
    t.assert_error_msg_content_equals(
        'replica.name can be specified only when ' ..
        'identification_mode = "uuid_as_key"', vcfg.check, config)
end

g.test_identification_mode_uuid_as_key = function()
    local storage_1_a = {
        name = 'storage_1_a',
        uri = 'storage:storage@127.0.0.1:3301',
    }
    local replicaset_1 = {
        replicas = {
            storage_1_a_uuid = storage_1_a,
        },
    }
    local config = {
        identification_mode = 'uuid_as_key',
        sharding = {
            replicaset_1_uuid = replicaset_1,
        },
    }
    t.assert(vcfg.check(config))

    -- replica.name is optional.
    storage_1_a.name = nil
    t.assert(vcfg.check(config))

    -- replicaset/replica.uuid is forbidden.
    storage_1_a.uuid = 'uuid'
    t.assert_error_msg_content_equals(
        'uuid option can be specified only when ' ..
        'identification_mode = "name_as_key"', vcfg.check, config)
end
