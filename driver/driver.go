// Package driver defines the interface for storage driver implementations.
// It provides a common interface for different storage backends like etcd and TCS.
package driver

import (
	"context"

	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
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
	Watch(ctx context.Context, key []byte, opts ...watch.Option) <-chan watch.Event
}
