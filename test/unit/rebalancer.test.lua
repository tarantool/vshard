test_run = require('test_run').new()
vshard = require('vshard')
fiber = require('fiber')
calc_metrics = vshard.storage.internal.rebalancer_calculate_metrics
build_routes = vshard.storage.internal.rebalancer_build_routes
calc_etalon = require('vshard.replicaset').calculate_etalon_balance
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
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
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
calc_etalon(replicasets, 7)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
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
calc_etalon(replicasets, 300)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
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
calc_etalon(replicasets, 300)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets
build_routes(replicasets)

--
-- gh-4: limit number of buckets receiving at once by node. In the
-- test below a new replicaset is introduced and it needed 1000
-- buckets. But at once it can receive only specified in config
-- ones.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 1500, weight = 1},
	uuid2 = {bucket_count = 1500, weight = 1},
	uuid3 = {bucket_count = 0, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 3000)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
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

--
-- Other tests.
--
consts.BUCKET_COUNT = 100
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 50, weight = 1},
	uuid2 = {bucket_count = 50, weight = 1},
	uuid3 = {bucket_count = 0, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets

consts.BUCKET_COUNT = 100
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 34, weight = 1},
	uuid2 = {bucket_count = 34, weight = 1},
	uuid3 = {bucket_count = 32, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets

consts.BUCKET_COUNT = 100
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 13, weight = 1},
	uuid2 = {bucket_count = 13, weight = 1},
	uuid3 = {bucket_count = 13, weight = 1},
	uuid4 = {bucket_count = 13, weight = 1},
	uuid5 = {bucket_count = 13, weight = 1},
	uuid6 = {bucket_count = 12, weight = 1},
	uuid7 = {bucket_count = 12, weight = 1},
	uuid8 = {bucket_count = 11, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets

test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 25, weight = 1},
	uuid2 = {bucket_count = 25, weight = 1},
	uuid3 = {bucket_count = 25, weight = 1},
	uuid4 = {bucket_count = 25, weight = 0},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets

--
-- gh-71: allow to pin buckets. A pinned bucket can not be sent
-- out of its replicaset even to satisfy perfect balance.
--
-- For this case the rebalancer does best effort balance. The
-- perfect balance here is unrechable, since on each replicaset
-- too many buckets are pinned.
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 33, pinned_count = 26, weight = 1},
	uuid2 = {bucket_count = 33, pinned_count = 24, weight = 1},
	uuid3 = {bucket_count = 34, pinned_count = 30, weight = 1},
	uuid4 = {bucket_count = 0, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets
--
-- Here the disbalance is ok for the replicaset with uuid1 only -
-- other replicasets have pinned buckets too, but not enough to
-- break the balance: buckets are moved ok to uuid4.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 33, pinned_count = 30, weight = 1},
	uuid2 = {bucket_count = 33, pinned_count = 10, weight = 1},
	uuid3 = {bucket_count = 34, pinned_count = 15, weight = 1},
	uuid4 = {bucket_count = 0, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets
--
-- Non-locked replicaset with any pinned bucket count can receive
-- more buckets, if the rebalancer decides it is the best balance.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 30, pinned_count = 25, weight = 0},
	uuid2 = {bucket_count = 25, pinned_count = 25, weight = 1},
	uuid3 = {bucket_count = 25, pinned_count = 25, weight = 1},
	uuid4 = {bucket_count = 20, weight = 0},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets
--
-- Check that the rebalancer can calculate a complex case, when a
-- perfect balance is learned in several steps of the algorithm.
-- Here on the first step it is calculated, that each replicaset
-- must contain 25 buckets. But UUID1 can not satisfy it, so it
-- is ignored. On the next step there are 100 - 30 pinned buckets
-- from UUID1 = 70 buckets, and 3 replicasets. A new perfect
-- balance is 23-23-24. But it can not be satisfied too - UUID2
-- has 25 pinned buckets, so it is ignored. On the third step
-- there are 70 - 25 = 45 buckets and 2 replicasets. A new perfect
-- balance is 22-23. But it is unreachable too, because UUID3 has
-- 24 pinned buckets. So only UUID4 is not ignored, and it
-- receives all non-pinned buckets: 45 - 24 = 21.
--
test_run:cmd("setopt delimiter ';'")
replicasets = {
	uuid1 = {bucket_count = 33, pinned_count = 30, weight = 1},
	uuid2 = {bucket_count = 33, pinned_count = 25, weight = 1},
	uuid3 = {bucket_count = 34, pinned_count = 24, weight = 1},
	uuid4 = {bucket_count = 0, weight = 1},
};
test_run:cmd("setopt delimiter ''");
calc_etalon(replicasets, 100)
replicasets
calc_metrics(replicasets, consts.DEFAULT_REBALANCER_MAX_RECEIVING)
replicasets

_bucket:drop()
