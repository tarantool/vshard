local util = require('util')

return {
    sharding = {
        [util.replicasets[1]] = {
            replicas = {
                [util.name_to_uuid.box_1_a] = {
                    uri = 'storage:storage@127.0.0.1:3301',
                    name = 'box_1_a',
                    master = true,
                },
                [util.name_to_uuid.box_1_b] = {
                    uri = 'storage:storage@127.0.0.1:3302',
                    name = 'box_1_b',
                },
                [util.name_to_uuid.box_1_c] = {
                    uri = 'storage:storage@127.0.0.1:3303',
                    name = 'box_1_c',
                },
            }
        }
    }
}
