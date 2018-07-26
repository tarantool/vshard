names = {
    storage_1_1_a = '32a2d4b8-f146-44ed-9d51-2436507efdf8',
    storage_1_1_b = 'c1c849b1-641d-40b8-9283-bcfe73d46270',
    storage_1_2_a = '04e677ed-c7ba-47e0-a67f-b5100cfa86af',
    storage_1_2_b = 'c7a979ee-9263-4a38-84a5-2fb6a0a32684',
    storage_2_1_a = '88dc03f0-23fb-4f05-b462-e29186542864',
    storage_2_1_b = '4230b711-f5c4-4131-bf98-88cd43a16901',
    storage_2_2_a = '6b1eefbc-1e2e-410e-84ff-44c572ea9916',
    storage_2_2_b = 'be74419a-1e56-4ba4-97e9-6b18710f63c5',
}

rs_1_1 = 'dd208fb8-8b90-49bc-8393-6b3a99da7c52'
rs_1_2 = 'af9cfe88-2091-4613-a877-a623776c5c0e'
rs_2_1 = '9ca8ee15-ae18-4f31-9385-4859f89ce73f'
rs_2_2 = '007f5f58-b654-4125-8441-a71866fb62b5'

local cfg_1 = {}
cfg_1.sharding = {
    [rs_1_1] = {
        replicas = {
            [names.storage_1_1_a] = {
                uri = 'storage:storage@127.0.0.1:3301',
                name = 'storage_1_1_a',
                master = true,
            },
            [names.storage_1_1_b] = {
                uri = 'storage:storage@127.0.0.1:3302',
                name = 'storage_1_1_b',
            },
        }
    },
    [rs_1_2] = {
        replicas = {
            [names.storage_1_2_a] = {
                uri = 'storage:storage@127.0.0.1:3303',
                name = 'storage_1_2_a',
                master = true,
            },
            [names.storage_1_2_b] = {
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
            [names.storage_2_1_a] = {
                uri = 'storage:storage@127.0.0.1:3305',
                name = 'storage_2_1_a',
                master = true,
            },
            [names.storage_2_1_b] = {
                uri = 'storage:storage@127.0.0.1:3306',
                name = 'storage_2_1_b',
            },
        }
    },
    [rs_2_2] = {
        replicas = {
            [names.storage_2_2_a] = {
                uri = 'storage:storage@127.0.0.1:3307',
                name = 'storage_2_2_a',
                master = true,
            },
            [names.storage_2_2_b] = {
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
