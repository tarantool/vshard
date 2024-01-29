package vshard_router

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

// --------------------------------------------------------------------------------
// -- Discovery
// --------------------------------------------------------------------------------

// BucketDiscovery search bucket in whole cluster
func (r *Router) BucketDiscovery(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	r.searchLock.mu.Lock()             // локаем чтобы понять можно ли начать ли поиск и не пытается ли узнать другой бакет что искать и записать свой лок канал
	<-r.searchLock.perBucket[bucketID] // проверяем что этот бакет ранее не вошел в поиск

	rs := r.routeMap[bucketID]
	if rs != nil {
		r.searchLock.mu.Unlock()

		return rs, nil
	}

	lockCh := make(chan struct{})
	r.searchLock.perBucket[bucketID] = lockCh
	r.searchLock.mu.Unlock()

	defer close(lockCh)

	r.cfg.Logger.Info(ctx, fmt.Sprintf("Discovering bucket %d", bucketID))

	wg := sync.WaitGroup{}
	wg.Add(len(r.idToReplicaset))

	var err error
	var resultRs *Replicaset

	for rsID, rs := range r.idToReplicaset {
		go func(_rs *Replicaset) {
			defer wg.Done()
			_, errStat := _rs.bucketStat(ctx, bucketID)
			if errStat == nil {
				resultRs, err = r.BucketSet(bucketID, rsID)
			}
		}(rs)
	}

	wg.Wait()
	if err != nil || resultRs == nil {
		return nil, Errors[9] // NO_ROUTE_TO_BUCKET
	}
	/*
	   -- All replicasets were scanned, but a bucket was not
	   -- found anywhere, so most likely it does not exist. It
	   -- can be wrong, if rebalancing is in progress, and a
	   -- bucket was found to be RECEIVING on one replicaset, and
	   -- was not found on other replicasets (it was sent during
	   -- discovery).
	*/

	return resultRs, nil
}

// BucketResolve resolve bucket id to replicaset
func (r *Router) BucketResolve(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	rs := r.routeMap[bucketID]
	if rs != nil {
		return rs, nil
	}

	// Replicaset removed from cluster, perform discovery
	rs, err := r.BucketDiscovery(ctx, bucketID)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// DiscoveryHandleBuckets arrange downloaded buckets to the route map so as they reference a given replicaset.
func (r *Router) DiscoveryHandleBuckets(ctx context.Context, rs *Replicaset, buckets []uint64) {
	count := rs.bucketCount.Load()
	affected := make(map[*Replicaset]int)

	for _, bucketID := range buckets {
		oldRs := r.routeMap[bucketID]

		if oldRs != rs {
			count++

			if oldRs != nil {
				bc := oldRs.bucketCount

				if _, exists := affected[oldRs]; !exists {
					affected[oldRs] = int(bc.Load())
				}

				oldRs.bucketCount.Store(bc.Load() - 1)
			} else {
				//                 router.known_bucket_count = router.known_bucket_count + 1
				r.knownBucketCount.Add(1)
			}
			r.routeMap[bucketID] = rs
		}
	}

	if count != rs.bucketCount.Load() {
		r.cfg.Logger.Info(ctx, fmt.Sprintf("Updated %s buckets: was %d, became %d", rs.info.Name, rs.bucketCount, count))
	}

	rs.bucketCount.Store(count)

	for rs, oldBucketCount := range affected {
		r.log().Info(ctx, fmt.Sprintf("Affected buckets of %s: was %d, became %d", rs.info.Name, oldBucketCount, rs.bucketCount))
	}
}

func (r *Router) DiscoveryAllBuckets(ctx context.Context) error {
	type BucketsDiscoveryPaginationRequest struct {
		From uint64 `msgpack:"from"`
	}

	t := time.Now()
	r.log().Info(ctx, "start discovery all buckets")

	knownBucket := atomic.Int32{}

	errGr, ctx := errgroup.WithContext(ctx)

	for _, rs := range r.idToReplicaset {
		rs := rs

		errGr.Go(func() error {
			rawReq := BucketsDiscoveryPaginationRequest{From: 0}

			for {
				bucketsInRS := make([]uint64, 0) // cause lua starts from 1
				nextFrom := new(uint64)
				req := tarantool.NewCallRequest("vshard.storage.buckets_discovery").
					Context(ctx).
					Args([]interface{}{&rawReq})

				future := rs.conn.Do(req, pool.PreferRO)

				err := future.GetTyped(&[]interface{}{&struct {
					Buckets  *[]uint64 `msgpack:"buckets"`
					NextFrom *uint64   `msgpack:"next_from"`
				}{
					Buckets:  &bucketsInRS,
					NextFrom: nextFrom,
				}})
				if err != nil {
					return err
				}

				if len(bucketsInRS) == 0 {
					return nil
				}

				for _, bucket := range bucketsInRS {
					if bucket == 0 {
						break
					}

					r.routeMap[bucket] = rs
					knownBucket.Add(1)
				}

				rawReq.From = *nextFrom
			}
		})

	}

	err := errGr.Wait()
	if err != nil {
		return nil
	}
	r.log().Info(ctx, fmt.Sprintf("discovery done since: %s", time.Since(t)))

	r.knownBucketCount.Store(knownBucket.Load())

	return nil
}

// startCronDiscovery is discovery_service_f analog with goroutines instead fibers
func (r *Router) startCronDiscovery(ctx context.Context) error {
	select {
	case <-ctx.Done():
		r.metrics().CronDiscoveryEvent(false, 0, "ctx-cancel")

		return ctx.Err()
	case <-time.After(r.cfg.DiscoveryTimeout):
		tStartDiscovery := time.Now()

		err := r.DiscoveryAllBuckets(ctx)
		if err != nil {
			r.metrics().CronDiscoveryEvent(false, time.Since(tStartDiscovery), "discovery-error")

			r.log().Error(ctx, fmt.Sprintf("cant do cron discovery with error: %s", err))
		}

		r.metrics().CronDiscoveryEvent(true, time.Since(tStartDiscovery), "ok")
	}

	return nil
}
