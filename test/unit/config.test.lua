test_run = require('test_run').new()
vshard = require('vshard')
util = require('vshard.util')
require('util')

--
-- Check sharding config sanity.
--
check_config = util.sanity_check_config

-- Not table.
check_error(check_config, 100)

-- Replicaset is not table.
check_error(check_config, {100})

server = {}
replicaset = {servers = {['replica_uuid'] = server}}
cfg = {['replicaset_uuid'] = replicaset}

-- URI is not string.
check_error(check_config, cfg)
server.uri = 100
check_error(check_config, cfg)
server.uri = 'uri:uri@uri'

-- Name is not string.
check_error(check_config, cfg)
server.name = 100
check_error(check_config, cfg)
server.name = 'storage'

-- Master is not boolean.
server.master = 100
check_error(check_config, cfg)
server.master = true

-- Multiple masters.
server2 = {uri = 'uri:uri@uri2', name = 'storage2', master = true}
replicaset.servers['id2'] = server2
check_error(check_config, cfg)
replicaset.servers['id2'] = nil

-- URI duplicate in one replicaset.
server2 = {uri = 'uri:uri@uri', name = 'storage2'}
replicaset.servers['id2'] = server2
check_error(check_config, cfg)
replicaset.servers['id2'] = nil

-- URI duplicate in different replicasets.
replicaset2 = {servers = {['id2'] = {uri = 'uri:uri@uri', name = 'storage2', master = true}}}
cfg['rsid2'] = replicaset2
check_error(check_config, cfg)
cfg['rsid2'] = nil

-- UUID duplicate in different replicasets.
replicaset2 = {servers = {['id3'] = {uri = 'uri:uri@uri2', name = 'storage', master = true}}}
cfg['rsid2'] = replicaset2
replicaset3 = {servers = {['id3'] = {uri = 'uri:uri@uri3', name = 'storage', master = true}}}
cfg['rsid3'] = replicaset3
check_error(check_config, cfg)
cfg['rsid2'] = nil
cfg['rsid3'] = nil
