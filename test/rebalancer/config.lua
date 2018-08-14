local util = require('util')

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
            }
        }
    },
    [util.replicasets[2]] = {
        replicas = {
            [util.name_to_uuid.box_2_a] = {
                uri = 'storage:storage@127.0.0.1:3303',
                name = 'box_2_a',
                master = true,
            },
            [util.name_to_uuid.box_2_b] = {
                uri = 'storage:storage@127.0.0.1:3304',
                name = 'box_2_b',
            }
        }
    }
}

function add_replicaset()
    sharding[util.replicasets[3]] = {
        replicas = {
            [util.name_to_uuid.box_3_a] = {
                uri = 'storage:storage@127.0.0.1:3305',
                name = 'box_3_a',
                master = true
            },
            [util.name_to_uuid.box_3_b] = {
                uri = 'storage:storage@127.0.0.1:3306',
                name = 'box_3_b',
            }
        }
    }
end

function add_second_replicaset()
    sharding[util.replicasets[4]] = {
        replicas = {
            [util.name_to_uuid.box_4_a] = {
                uri = 'storage:storage@127.0.0.1:3307',
                name = 'box_4_a',
                master = true
            },
            [util.name_to_uuid.box_4_b] = {
                uri = 'storage:storage@127.0.0.1:3308',
                name = 'box_4_b',
            }
        }
    }
end

function remove_replicaset_first_stage()
    sharding[util.replicasets[3]].weight = 0
end

function remove_replicaset_second_stage()
    sharding[util.replicasets[3]] = nil
end

function remove_second_replicaset_first_stage()
    sharding[util.replicasets[4]].weight = 0
end

return {
    -- Use small number of buckets to speedup tests.
    bucket_count = 200,
    sharding = sharding,
    rebalancer_disbalance_threshold = 0.01,
    shard_index = 'vbucket',
}
