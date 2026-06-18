// Package driver defines the interface for storage driver implementations.
// It provides a common interface for different storage backends like etcd and TCS.
package driver

import (
	"context"

	"github.com/tarantool/go-storage/v2/locker"
	"github.com/tarantool/go-storage/v2/operation"
	"github.com/tarantool/go-storage/v2/predicate"
	"github.com/tarantool/go-storage/v2/tx"
	"github.com/tarantool/go-storage/v2/watch"
)

// Driver is the interface that storage drivers must implement.
// It provides low-level operations for transaction execution and watch functionality.
type Driver interface {
	// Execute executes a transactional operation with conditional logic.
	// The transaction will execute thenOps if all predicates evaluate to true,
	// otherwise it will execute elseOps.
	Execute(
		ctx context.Context,
		predicates []predicate.Predicate,
		thenOps []operation.Operation,
		elseOps []operation.Operation,
	) (tx.Response, error)

	// Watch establishes a watch stream for changes to a specific key or prefix.
	// The returned channel will receive events as changes occur.
	// The returned cleanup function should be called to stop the watch and release resources.
	// An error is returned if the watch could not be established.
	Watch(ctx context.Context, key []byte) (<-chan watch.Event, func(), error)

	// NewLocker creates a Locker for name. ctx is the locker-lifetime context:
	// cancelling it stops any keepalive goroutine and aborts a blocking Lock.
	NewLocker(ctx context.Context, name string, opts ...locker.Option) (locker.Locker, error)
}
