local replicaset_uuid = {'cbf06940-0790-498b-948d-042b62cf3d29',
			 'ac522f65-aa94-4134-9f64-51ee384f1a54'}

local name_to_uuid = {
	bad_uuid_1_a = '8a274925-a26d-47fc-9e1b-af88ce939412',
	bad_uuid_1_b = '3de2e3e1-9ebe-4d0d-abb1-26d301b84633',
	bad_uuid_2_a = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
	bad_uuid_2_b = '001688c3-66f8-4a31-8e19-036c17d489c2',
}

local shard_cfg = {
	sharding = {
		[replicaset_uuid[1]] = {
			replicas = {
				[name_to_uuid.bad_uuid_1_a] = {
					uri = 'storage:storage@127.0.0.1:3301',
					name = 'bad_uuid_1_a',
					master = true
				},
				[name_to_uuid.bad_uuid_1_b] = {
					uri = 'storage:storage@127.0.0.1:3302',
					name = 'bad_uuid_1_b'
				}
			}
		},
		[replicaset_uuid[2]] = {
			replicas = {
				[name_to_uuid.bad_uuid_2_a] = {
					uri = 'storage:storage@127.0.0.1:3303',
					name = 'bad_uuid_2_a',
					master = true
				},
				[name_to_uuid.bad_uuid_2_b] = {
					uri = 'storage:storage@127.0.0.1:3304',
					name = 'bad_uuid_2_b'
				}
			}
		},
	}
}

return {cfg = shard_cfg, replicaset_uuid = replicaset_uuid,
	name_to_uuid = name_to_uuid, replication_connect_quorum = 0}
