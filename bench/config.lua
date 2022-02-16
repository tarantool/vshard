return {
    sharding = {
        ['00000000-0000-0000-0000-000000000001'] = {
            replicas = {
                ['00000000-0000-0000-0000-000000000002'] = {
                    uri = 'storage:storage@127.0.0.1:3301',
                    name = 'node1',
                    master = true,
                },
                ['00000000-0000-0000-0000-000000000003'] = {
                    uri = 'storage:storage@127.0.0.1:3302',
                    name = 'node2',
                },
                ['00000000-0000-0000-0000-000000000004'] = {
                    uri = 'storage:storage@127.0.0.1:3303',
                    name = 'node3',
                },
            },
        },
    },
    bucket_count = 100,
    net_msg_max = 10000,
}
