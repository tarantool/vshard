names = require('names')
replica = names.replica_uuid
rs = names.rs_uuid

sharding = {
    [rs[1]] = {
        replicas = {
            [replica.box_1_a] = {
                uri = 'storage:storage@127.0.0.1:3301',
                name = 'box_1_a',
                master = true,
            },
            [replica.box_1_b] = {
                uri = 'storage:storage@127.0.0.1:3302',
                name = 'box_1_b',
            }
        }
    },
    [rs[2]] = {
        replicas = {
            [replica.box_2_a] = {
                uri = 'storage:storage@127.0.0.1:3303',
                name = 'box_2_a',
                master = true,
            },
            [replica.box_2_b] = {
                uri = 'storage:storage@127.0.0.1:3304',
                name = 'box_2_b',
            }
        }
    }
}

return {
    sharding = sharding
}
