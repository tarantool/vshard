#!/usr/bin/env tarantool

-- Get instance name
local name = require('fio').basename(arg[0], '.lua')
-- Check if we are running under test-run
if os.getenv('ADMIN') then
    test_run = require('test_run').new()
    require('console').listen(os.getenv('ADMIN'))
end
util = require('util')
instance_uuid = nil
replicaset_uuid = nil
if name == 'storage_1_1' then
	box.cfg{}
	instance_uuid = box.info.uuid
	replicaset_uuid = box.info.cluster.uuid
elseif name == 'storage_1_2' then
	box.cfg{instance_uuid = '8a274925-a26d-47fc-9e1b-af88ce939412'}
	instance_uuid = '8a274925-a26d-47fc-9e1b-af88ce000000'
	replicaset_uuid = box.info.cluster.uuid
elseif name == 'storage_1_3' then
	box.cfg{replicaset_uuid = '8a274925-a26d-47fc-9e1b-af88ce939412'}
	instance_uuid = box.info.uuid
	replicaset_uuid = '8a274925-a26d-47fc-9e1b-af88ce000000'
else
	assert(false)
end

cfg = {
    sharding = {
        [replicaset_uuid] = {
            replicas = {
                [instance_uuid] = {
                    uri = 'storage:storage@127.0.0.1:3301',
                    name = 'storage_1_1',
                    master = true
                }
            }
        }
    },
    replication_connect_timeout = 0.01,
    replication_connect_quorum = 0,
}

vshard = require('vshard')
