test_run = require('test_run').new()
vshard = require('vshard')
fiber = require('fiber')
calc_metrics = vshard.storage.internal.rebalancer_calculate_metrics
build_routes = vshard.storage.internal.rebalancer_build_routes
consts = vshard.consts

--
-- Test adding two new replicasets.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 100, weight = 50},
	uuid2 = {bucket_count = 0, weight = 20},
	uuid3 = {bucket_count = 0, weight = 30},
};
test_run:cmd("setopt delimiter ''");
calc_metrics(replicasets, 100)
replicasets
build_routes(replicasets)

--
-- Test removing replicasets.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 2, weight = 1},
	uuid2 = {bucket_count = 2, weight = 1},
	uuid3 = {bucket_count = 3, weight = 0},
};
test_run:cmd("setopt delimiter ''");
calc_metrics(replicasets, 7)
replicasets
build_routes(replicasets)

--
-- Test big weights.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 100, weight = 1000},
	uuid2 = {bucket_count = 100, weight = 2000},
	uuid3 = {bucket_count = 100, weight = 500},
};
test_run:cmd("setopt delimiter ''");
calc_metrics(replicasets, 300)
replicasets
build_routes(replicasets)

--
-- Test no changes on already balanced cluster.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 100, weight = 1},
	uuid2 = {bucket_count = 100, weight = 1},
	uuid3 = {bucket_count = 100, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_metrics(replicasets, 300)
replicasets
build_routes(replicasets)

--
-- gh-4: limit number of buckets receiving at once by node. In the
-- test below a new replicaset is introduced and it needed 1000
-- buckets. But at once it can receive only
-- consts.REBALANCER_MAX_RECEIVING.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 1500, weight = 1},
	uuid2 = {bucket_count = 1500, weight = 1},
	uuid3 = {bucket_count = 0, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_metrics(replicasets, 3000)
replicasets
build_routes(replicasets)

--
-- Test rebalancer local state.
--
get_state = vshard.storage.rebalancer_request_state
_bucket = box.schema.create_space('_bucket')
pk = _bucket:create_index('pk')
status = _bucket:create_index('status', {parts = {{2, 'string'}}, unique = false})
_bucket:replace{1, consts.BUCKET.ACTIVE}
_bucket:replace{2, consts.BUCKET.ACTIVE}
_bucket:replace{3, consts.BUCKET.SENT}
get_state()

_bucket:replace{1, consts.BUCKET.RECEIVING}
get_state()

_bucket:drop()
