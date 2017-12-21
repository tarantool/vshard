test_run = require('test_run').new()
vshard = require('vshard')
lcfg = require('vshard.cfg')
util = require('util')

--
-- Check sharding config sanity.
--

function check(cfg) return util.check_error(lcfg.check, cfg) end

-- Not table.
check(100)

-- Sharding is not a table.
check({sharding = 100})

-- Replicaset is not table.
check({sharding = {100}})

replica = {}
replicaset = {replicas = {['replica_uuid'] = replica}}
cfg = {sharding = {['replicaset_uuid'] = replicaset}}

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
cfg.sharding['rsid2'] = replicaset2
check(cfg)
cfg.sharding['rsid2'] = nil

-- UUID duplicate in different replicasets.
replicaset2 = {replicas = {['id3'] = {uri = 'uri:uri@uri2', name = 'storage', master = true}}}
cfg.sharding['rsid2'] = replicaset2
replicaset3 = {replicas = {['id3'] = {uri = 'uri:uri@uri3', name = 'storage', master = true}}}
cfg.sharding['rsid3'] = replicaset3
check(cfg)
cfg.sharding['rsid2'] = nil
cfg.sharding['rsid3'] = nil

--
-- gh-40: replicaset weight. Weight is used by a rebalancer to
-- correctly spead buckets on a cluster.
--
replicaset.weight = '100'
check(cfg)
replicaset.weight = -100
check(cfg)
replicaset.weight = 0
lcfg.check(cfg)
replicaset.weight = 0.123
lcfg.check(cfg)
replicaset.weight = 100000
lcfg.check(cfg)

--
-- gh-12: zones, zone weight and failover by weight.
--
cfg.weights = 100
check(cfg)
cfg.weights = {[{1}] = 200}
check(cfg)
weights = {zone1 = 100}
cfg.weights = weights
check(cfg)
weights.zone1 = {[{1}] = 100}
check(cfg)
weights.zone1 = {zone2 = '100'}
check(cfg)
weights.zone1 = {zone1 = 100}
check(cfg)
weights[2] = {zone1 = 100}
weights.zone1 = {[2] = 100}
lcfg.check(cfg)
