package vshard_router

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewRouter_EmptyReplicasets(t *testing.T) {
	ctx := context.TODO()

	router, err := NewRouter(ctx, Config{})
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InvalidReplicasetUUID(t *testing.T) {
	ctx := context.TODO()

	router, err := NewRouter(ctx, Config{
		Replicasets: map[ReplicasetInfo][]InstanceInfo{
			ReplicasetInfo{
				Name: "123",
			}: []InstanceInfo{
				{Addr: "first.internal:1212"},
			},
		},
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InstanceAddr(t *testing.T) {
	ctx := context.TODO()

	router, err := NewRouter(ctx, Config{
		Replicasets: map[ReplicasetInfo][]InstanceInfo{
			ReplicasetInfo{
				Name: "123",
				UUID: uuid.New(),
			}: []InstanceInfo{
				{Addr: "first.internal:1212"},
			},
		},
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestRouterBucketIDStrCRC32(t *testing.T) {
	// required values from tarantool example
	require.Equal(t, uint64(103202), BucketIDStrCRC32("2707623829", uint64(256000)))
	require.Equal(t, uint64(35415), BucketIDStrCRC32("2706201716", uint64(256000)))
}
