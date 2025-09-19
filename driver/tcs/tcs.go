// Package tcs provides a Tarantool Cartridge storage driver implementation.
// It enables using Tarantool as a distributed key-value storage backend.
package tcs

import (
	"context"
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"

	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

// DriverTCS is a Tarantool implementation of the storage driver interface.
// It uses TCS as the underlying key-value storage backend.
type DriverTCS struct {
	conn *pool.ConnectionPool // Tarantool connection pool.
}

// NewDriverTarantool creates a new Tarantool driver instance.
// It establishes connections to Tarantool instances using the provided addresses.
func NewDriverTarantool(ctx context.Context, addrs []string) (*DriverTCS, error) {
	instances := make([]pool.Instance, 0, len(addrs))
	for i, addr := range addrs {
		instances = append(instances, pool.Instance{
			Name: fmt.Sprintf("instance-%d", i),
			Dialer: &tarantool.NetDialer{
				Address:  addr,
				User:     "user",
				Password: "password",
				RequiredProtocolInfo: tarantool.ProtocolInfo{
					Auth:     tarantool.AutoAuth,
					Version:  tarantool.ProtocolVersion(0),
					Features: nil,
				},
			},
			Opts: tarantool.Opts{
				Timeout:       0,
				Reconnect:     0,
				MaxReconnects: 0,
				RateLimit:     0,
				RLimitAction:  tarantool.RLimitAction(0),
				Concurrency:   0,
				SkipSchema:    false,
				Notify:        nil,
				Handle:        nil,
				Logger:        nil,
			},
		})
	}

	conn, err := pool.Connect(ctx, instances)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to tarantool pool: %w", err)
	}

	return &DriverTCS{conn: conn}, nil
}

// ExecuteTX executes a transactional operation with conditional logic.
// It processes predicates to determine whether to execute thenOps or elseOps.
func (d DriverTCS) ExecuteTX(
	_ context.Context,
	_ []predicate.Predicate,
	_ []operation.Operation,
	_ []operation.Operation,
) (tx.TxnResponse, error) {
	panic("implement me")
}

// Watch monitors changes to a specific key and returns a stream of events.
// It supports optional watch configuration through the opts parameter.
func (d DriverTCS) Watch(_ context.Context, _ []byte, _ ...watch.Option) <-chan watch.Event {
	panic("implement me")
}
