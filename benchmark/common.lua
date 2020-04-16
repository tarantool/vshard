local clock = require('clock')
local log = require('log')
require('strict').on()

local instance_uuid = {
    storage_1_1 = '8a274925-a26d-47fc-9e1b-af88ce939412',
    storage_2_1 = '3de2e3e1-9ebe-4d0d-abb1-26d301b84633',
    storage_3_1 = '0be2896c-2a82-4f5f-ba93-56306790c488',
    storage_4_1 = 'fd581199-c4e5-452c-9004-69af179ef113',
}

local replicaset_uuid = {
    storage_1 = 'cbf06940-0790-498b-948d-042b62cf3d29',
    storage_2 = 'ac522f65-aa94-4134-9f64-51ee384f1a54',
    storage_3 = '2266e99f-903b-43ad-8387-ee79e21835cc',
    storage_4 = '3683f897-15cd-40d5-a6d8-9d629697c507',
}

local router_listen = {
    router_01 = '127.0.0.1:3401',
    router_02 = '127.0.0.1:3402',
    router_03 = '127.0.0.1:3403',
    router_04 = '127.0.0.1:3404',
    router_05 = '127.0.0.1:3405',
    router_06 = '127.0.0.1:3406',
    router_07 = '127.0.0.1:3407',
    router_08 = '127.0.0.1:3408',
    router_09 = '127.0.0.1:3409',
    router_10 = '127.0.0.1:3410',
    router_11 = '127.0.0.1:3411',
    router_12 = '127.0.0.1:3412',
    router_13 = '127.0.0.1:3413',
    router_14 = '127.0.0.1:3414',
    router_15 = '127.0.0.1:3415',
    router_16 = '127.0.0.1:3416',
}

-- Change these values to try different benchmarks.
local client_params = {
    -- How many fibers a client should start. Each fiber
    -- synchronously makes requests, one by one.
    worker_count = 5000,
    -- Load type. These are 'yield', 'echo', 'select', 'random'.
    -- See corresponding functions in storage.lua to check what
    -- each of them does.
    load = 'echo',
    -- Size of each argument passed to 'echo' on storage.
    arg_size = 1,
    -- Number of arguments to pass to 'echo' on storage.
    arg_count = 20,
    -- Direct means the client is router. Not direct is when the
    -- client accesses storage through routers.
    is_direct = false,
    -- How many tuples to select when load type is 'select'.
    select_limit = 10,
    -- Number of connections to establish per router. Connections
    -- are used in round-robin manner.
    connect_count_per_router = 10,
    -- Timeout of each request.
    timeout = 30,
}

local rate_meter_mt = {
    __index = {
        feed = function(self, count)
            local ts = clock.monotonic64()
            if self.last_ts == 0 then
                self.last_ts = ts
                return
            end
            self.last_sample = self.last_sample + count
            local passed_ms = tonumber(ts - self.last_ts) / 1000000
            if passed_ms < 50 then
                return
            end
            local rate_100ms = self.last_sample * 100.0 / passed_ms
            local curr = self.current_sample
            if curr == 10 then
                self.current_sample = 1
            else
                self.current_sample = curr + 1
            end
            self.rate = self.rate - self.samples[curr] + rate_100ms
            self.samples[curr] = rate_100ms
            self.last_ts = ts
            self.last_sample = 0
        end
    },
    __serialize = function(self)
        return self.rate
    end
}

local function create_rate_meter()
    return setmetatable({
        samples = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        current_sample = 1,
        rate = 0,
        last_ts = 0,
        last_sample = 0,
    }, rate_meter_mt)
end

return {
    config =  {
        sharding = {
            [replicaset_uuid.storage_1] = {
                replicas = {
                    [instance_uuid.storage_1_1] = {
                        uri = 'guest@127.0.0.1:3301',
                        name = 'storage_1_a',
                        master = true
                    },
                },
            },
            [replicaset_uuid.storage_2] = {
                replicas = {
                    [instance_uuid.storage_2_1] = {
                        uri = 'guest@127.0.0.1:3302',
                        name = 'storage_2_a',
                        master = true
                    },
                },
            },
            [replicaset_uuid.storage_3] = {
                replicas = {
                    [instance_uuid.storage_3_1] = {
                        uri = 'guest@127.0.0.1:3303',
                        name = 'storage_3_a',
                        master = true
                    },
                },
            },
            [replicaset_uuid.storage_4] = {
                replicas = {
                    [instance_uuid.storage_4_1] = {
                        uri = 'guest@127.0.0.1:3304',
                        name = 'storage_4_a',
                        master = true
                    },
                },
            },
        },
        bucket_count = 4096,
        replication_connect_quorum = 0,
        net_msg_max = client_params.worker_count,
        readahead = 1024 * 1024,
    },
    instance_uuid = instance_uuid,
    router_listen = router_listen,
    client_params = client_params,
    create_rate_meter = create_rate_meter,
}
