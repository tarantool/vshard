names = require('names')
replica = names.replica_uuid
rs = names.rs_uuid
weights = {
    [1] = {
        [2] = 1,
        [3] = 2,
        [4] = 3,
    },
    [2] = {
        [1] = 10,
        [2] = 0,
        [3] = 10,
        [4] = 20,
    },
    [3] = {
        [1] = 100,
        [2] = 200,
        [4] = 1000,
    }
}

return {
    weights = weights,
    sharding = {
        [rs[1]] = {
            replicas = {
                [replica.box_1_a] = {
                    uri = 'storage:storage@127.0.0.1:3301',
                    name = 'box_1_a',
                    master = true,
                    zone = 4
                },
                [replica.box_1_b] = {
                    uri = 'storage:storage@127.0.0.1:3302',
                    name = 'box_1_b',
                    zone = 2
                },
                [replica.box_1_c] = {
                    uri = 'storage:storage@127.0.0.1:3303',
                    name = 'box_1_c',
                    zone = 3
                },
                [replica.box_1_d] = {
                    uri = 'storage:storage@127.0.0.1:3304',
                    name = 'box_1_d',
                    zone = 1
                }
            }
        },
        [rs[2]] = {
            replicas = {
                [replica.box_2_a] = {
                    uri = 'storage:storage@127.0.0.1:3305',
                    name = 'box_2_a',
                    master = true,
                    zone = 1
                },
                [replica.box_2_b] = {
                    uri = 'storage:storage@127.0.0.1:3306',
                    name = 'box_2_b',
                    zone = 3
                },
                [replica.box_2_c] = {
                    uri = 'storage:storage@127.0.0.1:3307',
                    name = 'box_2_c',
                    zone = 2
                }
            }
        },
        [rs[3]] = {
            replicas = {
                [replica.box_3_a] = {
                    uri = 'storage:storage@127.0.0.1:3308',
                    name = 'box_3_a',
                    zone = 1
                },
                [replica.box_3_b] = {
                    uri = 'storage:storage@127.0.0.1:3309',
                    name = 'box_3_b',
                    master = true
                }
            }
        }
    }
}

