package vshard_router

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

// --------------------------------------------------------------------------------
// -- API
// --------------------------------------------------------------------------------

type VshardMode string

const (
	ReadMode  VshardMode = "read"
	WriteMode VshardMode = "write"
)

func (c VshardMode) String() string {
	return string(c)
}

type StorageCallAssertError struct {
	Code     int         `msgpack:"code"`
	BaseType string      `msgpack:"base_type"`
	Type     string      `msgpack:"type"`
	Message  string      `msgpack:"message"`
	Trace    interface{} `msgpack:"trace"`
}

func (s StorageCallAssertError) Error() string {
	return fmt.Sprintf("vshard.storage.call assert error code: %d, type:%s, message: %s", s.Code, s.Type, s.Message)
}

type StorageCallVShardError struct {
	BucketID       uint64  `msgpack:"bucket_id" mapstructure:"bucket_id"`
	Reason         string  `msgpack:"reason"`
	Code           int     `msgpack:"code"`
	Type           string  `msgpack:"type"`
	Message        string  `msgpack:"message"`
	Name           string  `msgpack:"name"`
	MasterUUID     *string `msgpack:"master_uuid" mapstructure:"master_uuid"`         // mapstructure cant decode to source uuid type
	ReplicasetUUID *string `msgpack:"replicaset_uuid" mapstructure:"replicaset_uuid"` // mapstructure cant decode to source uuid type
}

func (s StorageCallVShardError) Error() string {
	return fmt.Sprintf("vshard.storage.call bucket error bucket_id: %d, reason: %s, name: %s", s.BucketID, s.Reason, s.Name)
}

type StorageResultTypedFunc = func(result interface{}) error

type CallOpts struct {
	VshardMode VshardMode // vshard mode in call
	PoolMode   pool.Mode
	Timeout    time.Duration
}

const CallTimeoutMin = time.Second / 2

// RouterCallImpl Perform shard operation function will restart operation
// after wrong bucket response until timeout is reached
func (r *Router) RouterCallImpl(ctx context.Context,
	bucketID uint64,
	opts CallOpts,
	fnc string,
	args interface{}) (interface{}, StorageResultTypedFunc, error) {
	if bucketID > r.cfg.TotalBucketCount {
		return nil, nil, fmt.Errorf("bucket is unreachable: bucket id is out of range")
	}

	if opts.Timeout == 0 {
		opts.Timeout = CallTimeoutMin
	}

	timeout := opts.Timeout
	timeStart := time.Now()

	req := tarantool.NewCallRequest("vshard.storage.call")
	req = req.Context(ctx)
	req = req.Args([]interface{}{
		bucketID,
		opts.VshardMode.String(),
		fnc,
		args,
	})

	var err error

	for {

		if since := time.Since(timeStart); since > timeout {
			r.metrics().RequestDuration(since, false)

			r.log().Debug(ctx, fmt.Sprintf("return result on timeout; since %s of timeout %s", since, timeout))
			if err == nil {
				err = fmt.Errorf("cant get call cause call impl timeout")
			}

			return nil, nil, err
		}

		var rs *Replicaset

		rs, err = r.BucketResolve(ctx, bucketID)
		if err != nil {
			r.log().Debug(ctx, fmt.Sprintf("cant resolve bucket %d", bucketID))

			r.metrics().RetryOnCall("bucket_resolve_error")
			continue
		}

		future := rs.conn.Do(req, opts.PoolMode)

		var resp *tarantool.Response

		resp, err = future.Get()
		if err != nil {
			r.metrics().RetryOnCall("future_get_error")

			continue
		}

		if len(resp.Data) != 2 {
			r.log().Error(ctx, fmt.Sprintf("invalid response data lenght; current lenght %d", len(resp.Data)))

			r.metrics().RetryOnCall("resp_data_error")

			err = fmt.Errorf("invalid length of response data: must be = 2, current: %d", len(resp.Data))
			continue
		}

		if resp.Data[0] == nil {
			vshardErr := &StorageCallVShardError{}

			err = mapstructure.Decode(resp.Data[1], vshardErr)
			if err != nil {
				r.metrics().RetryOnCall("internal_error")

				r.log().Error(ctx, fmt.Sprintf("cant decode vhsard err by trarantool with err: %s; continue try", err))
				continue
			}

			err = vshardErr

			r.log().Error(ctx, fmt.Sprintf("got vshard storage call error: %s", err))

			if vshardErr.Name == "WRONG_BUCKET" ||
				vshardErr.Name == "BUCKET_IS_LOCKED" ||
				vshardErr.Name == "TRANSFER_IS_IN_PROGRESS" {
				r.BucketReset(bucketID)
				r.metrics().RetryOnCall("bucket_migrate")

				continue
			}

			continue
		}

		isVShardRespOk := false
		err = future.GetTyped(&[]interface{}{&isVShardRespOk})
		if err != nil {
			r.log().Debug(ctx, fmt.Sprintf("cant get typed with err: %s", err))

			continue
		}

		if !isVShardRespOk { // error
			errorResp := &StorageCallAssertError{}

			err = future.GetTyped(&[]interface{}{&isVShardRespOk, errorResp})
			if err != nil {
				err = fmt.Errorf("cant get typed vshard err with err: %s", err)
			}

			err = errorResp
		}

		r.metrics().RequestDuration(time.Since(timeStart), true)

		r.log().Debug(ctx, fmt.Sprintf("got call result response data %s", resp.Data))

		return resp.Data[1], func(result interface{}) error {
			var stub interface{}

			return future.GetTyped(&[]interface{}{&stub, result})
		}, nil
	}
}

// todo: router_map_callrw

// RouterRoute get replicaset object by bucket identifier.
// alias to BucketResolve
func (r *Router) RouterRoute(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	return r.BucketResolve(ctx, bucketID)
}

// RouterRouteAll return map of all replicasets.
func (r *Router) RouterRouteAll() map[uuid.UUID]*Replicaset {
	return r.idToReplicaset
}
