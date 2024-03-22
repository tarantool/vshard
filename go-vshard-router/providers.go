package vshard_router

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

var (
	_ MetricsProvider = (*EmptyMetrics)(nil)
	_ LogProvider     = (*EmptyLogger)(nil)
	_ LogProvider     = (*StdoutLogger)(nil)
)

type LogProvider interface {
	Info(context.Context, string)
	Debug(context.Context, string)
	Error(context.Context, string)
	Warn(context.Context, string)
}

type EmptyLogger struct{}

func (e *EmptyLogger) Info(ctx context.Context, msg string)  {}
func (e *EmptyLogger) Debug(ctx context.Context, msg string) {}
func (e *EmptyLogger) Error(ctx context.Context, msg string) {}
func (e *EmptyLogger) Warn(ctx context.Context, msg string)  {}

type StdoutLogger struct{}

func (e *StdoutLogger) Info(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Debug(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Error(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Warn(ctx context.Context, msg string) {
	log.Println(msg)
}

// Metrics

type MetricsProvider interface {
	CronDiscoveryEvent(ok bool, duration time.Duration, reason string)
	RetryOnCall(reason string)
	RequestDuration(duration time.Duration, ok bool)
}

// EmptyMetrics is default empty metrics provider
// you can embed this type and realize just some metrics
type EmptyMetrics struct{}

func (e *EmptyMetrics) CronDiscoveryEvent(ok bool, duration time.Duration, reason string) {}
func (e *EmptyMetrics) RetryOnCall(reason string)                                         {}
func (e *EmptyMetrics) RequestDuration(duration time.Duration, ok bool)                   {}

type TopologyProvider struct {
	r *Router
}

func (r *Router) Topology() *TopologyProvider {
	return &TopologyProvider{r: r}
}

func (t *TopologyProvider) AddInstance(ctx context.Context, rsID uuid.UUID, info InstanceInfo) error {
	return t.r.idToReplicaset[rsID].conn.Add(ctx, info.UUID.String(), tarantool.NetDialer{
		Address:  info.Addr,
		User:     t.r.cfg.User,
		Password: t.r.cfg.Password,
	})
}

func (t *TopologyProvider) RemoveInstance(ctx context.Context, rsID, instanceID uuid.UUID) error {
	return t.r.idToReplicaset[rsID].conn.Remove(instanceID.String())
}

func (t *TopologyProvider) AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error {
	router := t.r
	cfg := router.cfg

	replicaset := &Replicaset{
		info: ReplicasetInfo{
			Name: rsInfo.Name,
			UUID: rsInfo.UUID,
		},
		bucketCount: atomic.Int32{},
	}

	replicaset.bucketCount.Store(0)

	rsDialers := make(map[string]tarantool.Dialer, len(instances))

	for _, instance := range instances {
		dialer := tarantool.NetDialer{
			Address:  instance.Addr,
			User:     cfg.User,
			Password: cfg.Password,
		}

		rsDialers[instance.UUID.String()] = dialer
	}

	conn, err := pool.Connect(ctx, rsDialers, router.cfg.PoolOpts)
	if err != nil {
		return err
	}

	replicaset.conn = conn
	router.idToReplicaset[rsInfo.UUID] = replicaset // add when conn is ready

	return nil
}

func (t *TopologyProvider) AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error {
	for rsInfo, rsInstances := range replicasets {
		err := t.AddReplicaset(ctx, rsInfo, rsInstances)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TopologyProvider) RemoveReplicaset(ctx context.Context, rsID uuid.UUID) []error {
	r := t.r

	errors := r.idToReplicaset[rsID].conn.CloseGraceful()
	delete(r.idToReplicaset, rsID)

	return errors
}
