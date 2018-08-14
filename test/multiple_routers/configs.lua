util = require('util')

rs_1_1 = util.replicasets[1]
rs_1_2 = util.replicasets[2]
rs_2_1 = util.replicasets[3]
rs_2_2 = util.replicasets[4]

local cfg_1 = {}
cfg_1.sharding = {
    [rs_1_1] = {
        replicas = {
            [util.name_to_uuid.storage_1_1_a] = {
                uri = 'storage:storage@127.0.0.1:3301',
                name = 'storage_1_1_a',
                master = true,
            },
            [util.name_to_uuid.storage_1_1_b] = {
                uri = 'storage:storage@127.0.0.1:3302',
                name = 'storage_1_1_b',
            },
        }
    },
    [rs_1_2] = {
        replicas = {
            [util.name_to_uuid.storage_1_2_a] = {
                uri = 'storage:storage@127.0.0.1:3303',
                name = 'storage_1_2_a',
                master = true,
            },
            [util.name_to_uuid.storage_1_2_b] = {
                uri = 'storage:storage@127.0.0.1:3304',
                name = 'storage_1_2_b',
            },
        }
    },
}


local cfg_2 = {}
cfg_2.sharding = {
    [rs_2_1] = {
        replicas = {
            [util.name_to_uuid.storage_2_1_a] = {
                uri = 'storage:storage@127.0.0.1:3305',
                name = 'storage_2_1_a',
                master = true,
            },
            [util.name_to_uuid.storage_2_1_b] = {
                uri = 'storage:storage@127.0.0.1:3306',
                name = 'storage_2_1_b',
            },
        }
    },
    [rs_2_2] = {
        replicas = {
            [util.name_to_uuid.storage_2_2_a] = {
                uri = 'storage:storage@127.0.0.1:3307',
                name = 'storage_2_2_a',
                master = true,
            },
            [util.name_to_uuid.storage_2_2_b] = {
                uri = 'storage:storage@127.0.0.1:3308',
                name = 'storage_2_2_b',
            },
        }
    },
}

return {
    cfg_1 = cfg_1,
    cfg_2 = cfg_2,
}
