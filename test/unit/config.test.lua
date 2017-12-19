test_run = require('test_run').new()
vshard = require('vshard')
vutil = require('vshard.util')
util = require('util')

--
-- Check sharding config sanity.
--

function check(cfg) return util.check_error(vutil.sanity_check_config, cfg) end

-- Not table.
check(100)

-- Replicaset is not table.
check({100})

replica = {}
replicaset = {replicas = {['replica_uuid'] = replica}}
cfg = {['replicaset_uuid'] = replicaset}

-- URI is not string.
check(cfg)
replica.uri = 100
check(cfg)
replica.uri = 'uri:uri@uri'

-- Name is not string.
check(cfg)
replica.name = 100
check(cfg)
replica.name = 'storage'

-- Master is not boolean.
replica.master = 100
check(cfg)
replica.master = true

-- Multiple masters.
replica2 = {uri = 'uri:uri@uri2', name = 'storage2', master = true}
replicaset.replicas['id2'] = replica2
check(cfg)
replicaset.replicas['id2'] = nil

-- URI duplicate in one replicaset.
replica2 = {uri = 'uri:uri@uri', name = 'storage2'}
replicaset.replicas['id2'] = replica2
check(cfg)
replicaset.replicas['id2'] = nil

-- URI duplicate in different replicasets.
replicaset2 = {replicas = {['id2'] = {uri = 'uri:uri@uri', name = 'storage2', master = true}}}
cfg['rsid2'] = replicaset2
check(cfg)
cfg['rsid2'] = nil

-- UUID duplicate in different replicasets.
replicaset2 = {replicas = {['id3'] = {uri = 'uri:uri@uri2', name = 'storage', master = true}}}
cfg['rsid2'] = replicaset2
replicaset3 = {replicas = {['id3'] = {uri = 'uri:uri@uri3', name = 'storage', master = true}}}
cfg['rsid3'] = replicaset3
check(cfg)
cfg['rsid2'] = nil
cfg['rsid3'] = nil
