names = dofile('names.lua')
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

-- Reverse weights in config.
function reverse_weights()
    weights[1] = {
        [2] = 3,
        [3] = 2,
        [4] = 1
    }
    weights[2] = {
        [1] = 10,
        [2] = 0,
        [3] = 10,
        [4] = 10
    }
    weights[3] = {
        [1] = 1000,
        [2] = 200,
        [4] = 100
    }
end

local saved_replicas = {}
function remove_some_replicas()
    saved_replicas.box_1_a = sharding[rs[1]].replicas[replica.box_1_a]
    saved_replicas.box_1_c = sharding[rs[1]].replicas[replica.box_1_c]
    saved_replicas.box_2_a = sharding[rs[2]].replicas[replica.box_2_a]
    saved_replicas.box_2_c = sharding[rs[2]].replicas[replica.box_2_c]
    saved_replicas.box_3_a = sharding[rs[3]].replicas[replica.box_3_a]
    saved_replicas.box_3_b = sharding[rs[3]].replicas[replica.box_3_b]
    sharding[rs[1]].replicas[replica.box_1_a] = nil
    sharding[rs[1]].replicas[replica.box_1_c] = nil
    sharding[rs[2]].replicas[replica.box_2_a] = nil
    sharding[rs[2]].replicas[replica.box_2_c] = nil
    sharding[rs[3]].replicas[replica.box_3_a] = nil
    sharding[rs[3]].replicas[replica.box_3_b] = nil
end

function add_some_replicas()
    sharding[rs[1]].replicas[replica.box_1_a] = saved_replicas.box_1_a
    sharding[rs[1]].replicas[replica.box_1_c] = saved_replicas.box_1_c
    sharding[rs[2]].replicas[replica.box_2_a] = saved_replicas.box_2_a
    sharding[rs[2]].replicas[replica.box_2_c] = saved_replicas.box_2_c
    sharding[rs[3]].replicas[replica.box_3_a] = saved_replicas.box_3_a
    sharding[rs[3]].replicas[replica.box_3_b] = saved_replicas.box_3_b
end

return {
    weights = weights,
    sharding = sharding,
    replication_connect_quorum = 0,
}

