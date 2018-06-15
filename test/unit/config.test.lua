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
replicaset2 = {replicas = {['id3'] = {uri = 'uri:uri@uri2', name = 'storage2', master = true}}}
cfg.sharding['rsid2'] = replicaset2
replicaset3 = {replicas = {['id3'] = {uri = 'uri:uri@uri3', name = 'storage3', master = false}}}
cfg.sharding['rsid3'] = replicaset3
check(cfg)
cfg.sharding['rsid3'] = nil
cfg.sharding['rsid2'] = nil

--
-- gh-101: Log warning in case replica.name duplicate found.
--

-- name duplicate in one replicaset.
replica2_1 = {uri = 'uri:uri@uri2_1', name = 'dup_name1', master = true}
replica2_2 = {uri = 'uri:uri@uri2_2', name = 'dup_name1', master = false}
replicaset2 = {replicas = {['id2'] = replica2_1, ['id3'] = replica2_2}}
cfg.sharding['rsid2'] = replicaset2
_ = check(cfg)
test_run:grep_log('default', 'Duplicate replica.name is found: dup_name1')
cfg.sharding['rsid2'] = nil

-- name duplicate in different replicasets.
replica2 = {uri = 'uri:uri@uri2', name = 'dup_name2', master = true}
replica3 = {uri = 'uri:uri@uri3', name = 'dup_name2', master = true}
replicaset2 = {replicas = {['id2'] = replica2}}
replicaset3 = {replicas = {['id3'] = replica3}}
cfg.sharding['rsid2'] = replicaset2
cfg.sharding['rsid3'] = replicaset3
_ = check(cfg)
test_run:grep_log('default', 'Duplicate replica.name is found: dup_name2')
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
_ = lcfg.check(cfg)
replicaset.weight = 0.123
_ = lcfg.check(cfg)
replicaset.weight = 100000
_ = lcfg.check(cfg)

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
_ = lcfg.check(cfg)

--
-- gh-62: allow to specify bucket_count, rebalancer settings.
--
cfg.bucket_count = -100
check(cfg)
cfg.bucket_count = '0'
check(cfg)
cfg.bucket_count = 100.5
check(cfg)
cfg.bucket_count = 0
check(cfg)
cfg.bucket_count = 100
_ = lcfg.check(cfg)

cfg.rebalancer_disbalance_threshold = -100
check(cfg)
cfg.rebalancer_disbalance_threshold = '100'
check(cfg)
cfg.rebalancer_disbalance_threshold = 0.5
_ = lcfg.check(cfg)

cfg.rebalancer_max_receiving = -100
check(cfg)
cfg.rebalancer_max_receiving = '100'
check(cfg)
cfg.rebalancer_max_receiving = 0.5
check(cfg)
cfg.rebalancer_max_receiving = 0
check(cfg)
cfg.rebalancer_max_receiving = 10
_ = lcfg.check(cfg)

--
-- gh-74: allow to specify name or id of an index on bucket
-- identifiers.
--
cfg.shard_index = -100
check(cfg)
cfg.shard_index = 0.1
check(cfg)
cfg.shard_index = 0
_ = lcfg.check(cfg)
cfg.shard_index = ''
check(cfg)
cfg.shard_index = 'vbucket'
_ = lcfg.check(cfg)

--
-- gh-77: garbage collection options.
--
cfg.collect_bucket_garbage_interval = 'str'
check(cfg)
cfg.collect_bucket_garbage_interval = 0
check(cfg)
cfg.collect_bucket_garbage_interval = -1
check(cfg)
cfg.collect_bucket_garbage_interval = 100.5
_ = lcfg.check(cfg)

cfg.collect_lua_garbage = 100
check(cfg)
cfg.collect_lua_garbage = true
_ = lcfg.check(cfg)
cfg.collect_lua_garbage = false
_ = lcfg.check(cfg)

--
-- gh-84: sync before master demotion, and allow to configure
-- sync timeout.
--
cfg.sync_timeout = -100
check(cfg)
cfg.sync_timeout = 0
_ = lcfg.check(cfg)
cfg.sync_timeout = 10.5
_ = lcfg.check(cfg)

-- gh-91: Name is optional.
replica.name = nil
_ = lcfg.check(cfg)
replica.name = 'storage'

-- gh-47: Check uri
old_uri = replica.url
replica.uri = 'invalid uri'
util.check_error(lcfg.check, cfg)

replica.uri = '127.0.0.1'
lcfg.check(cfg)['sharding']
replica.uri = 'user:password@localhost'
lcfg.check(cfg)['sharding']
replica.url = old_uri
