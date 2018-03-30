# Replicaset lock and bucket pin

* **Status**: Complete
* **Start date**: 19-03-2018
* **Authors**: Vladislav Shpilevoy @Gerold103 <v.shpilevoy@tarantool.org>
* **Issues**: [#71](https://github.com/tarantool/vshard/issues/71)

## Summary

Replicaset lock makes it invisible for the rebalancer - a locked replicaset can neither receive new buckets nor send its own.
Bucket pin blocks this concrete bucket sending - it will stay on a replicaset to which pinned, until it is unpinned.
Pinning all replicaset buckets is not the same as replicaset locking - even if you pin all buckets, the **non-locked replicaset still can receive new buckets**.

## Background and motivation

Replicaset lock allows, for example, to separate a replicaset for testsing from production replicasets. Or to preserve some application metadata, that must not be sharded for a while. Bucket pin allows the same, but in the smaller scope.

Difference between replicaset lock and all buckets pinning is motivated by ability to button-up an entire replicaset.

Mostly locked and pinned buckets affect the rebalancing algorithm, which must ignore locked replicasets, and take pinned buckets into account, attempting to reach the best possible balance. It is not a trivial task, because a user can pin to a replicaset so many buckets, that a perfect balance is unreachable. For example, see the cluster (assume all weights are equal to 1):
```
rs - replicaset

rs1: bucket_count = 150
rs2: bucket_count = 150, pinned_count 120

Add a replicaset:

rs1: bucket_count = 150
rs2: bucket_count = 150, pinned_count 120
rs3: bucket_count = 0
```
Here the perfect balance is `100 - 100 - 100` that is impossible, since the replicaset rs2 have 120 pinned buckets. So the best reachable balance here is the following:
```
rs1: bucket_count = 90
rs2: bucket_count = 120, pinned_count 120
rs3: bucket_count = 90
```

Here the rebalancer moved from rs2 as many buckets as could to decrease disbalance. At the same time it respected equal weights of rs1 and rs3.

## Detailed design

The algorithms of respecting locks and pins are completely different despite of the similar functionality, and are considered separately.

### Replicaset lock and rebalancing

When a replicaset is locked, it simply does not participate in rebalancing. It means, that even if its perfect bucket count is not equal to an actual one, this disbalance can not be fixed due to lock. When the rebalancer detects that one of replicasets appears to be locked, it recalculates perfect bucket count of non-locked replicasets as if the locked replicaset and its buckets does not exist.

### Bucket pin and rebalancing

It is the much more complex case, that splits the rebalancing algorithm in the several steps:

1. The rebalancer calculates perfect bucket count as if all buckets are not pinned. Then it looks at each replicaset and compares its new perfect bucket count against pinned bucket count. If the pinned one is less - it is ok. A non-locked replicaset (on this step all locked replicasets already are filtered out) with pinned buckets can receive new ones.

2. If perfect bucket count is less, than pinned one, this disbalance can not be fixed - the rebalancer can not move pinned buckets out of this replicaset. In such a case perfect bucket count of this replicasets are set to the exactly pinned one. These replicasets are not considered by the rebalancer then, and their pinned count is subtracted from a total bucket count. Here the rebalancer tries to move out of such replicasets as many buckets as possible.

3. The described procecure is restarted from the step 1 with new total bucket count and with replicasets, those perfect bucket count >= pinned one, until it appears, that on all replicasets perfect bucket count >= pinned one.

Pseudocode:
```
function cluster_calculate_perfect_balance(replicasets, bucket_count)
	-- spread buckets over still considered replicasets using weights --
end;

cluster = <all of non-locked replicasets>;
bucket_count = <total bucket count in the cluster>;
can_reach_balance = false
while not can_reach_balance do
	can_reach_balance = true
	cluster_calculate_perfect_balance(cluster, bucket_count);
	foreach replicaset in cluster do
		if replicaset.perfect_bucket_count <
		   replicaset.pinned_bucket_count then
			can_reach_balance = false
			bucket_count -= replicaset.pinned_bucket_count;
			replicaset.perfect_bucket_count =
				replicaset.pinned_bucket_count;
		end;
	end;
end;
cluster_calculate_perfect_balance(cluster, bucket_count);
```
Complexity of the algorithm is `O(N^2)`, where `N` is replicaset count. On each step it either finishes the calculation, or ignores at least one new replicaset, which is overpopulated due to pinned buckets, and updates perfect bucket count of others.
