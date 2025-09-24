package storage

import (
	"context"
	"fmt"

	"github.com/tarantool/go-option"

	"github.com/tarantool/go-storage/driver"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	txPkg "github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

// rangeOptions contains configuration options for range operations.
type rangeOptions struct {
	Prefix string // Prefix filter for range queries.
	Limit  int    // Maximum number of results to return.
}

// RangeOption is a function that configures range operation options.
type RangeOption func(*rangeOptions)

// WithPrefix configures a range operation to filter keys by the specified prefix.
func WithPrefix(prefix string) RangeOption {
	return func(opts *rangeOptions) {
		opts.Prefix = prefix
	}
}

// WithLimit configures a range operation to limit the number of results returned.
func WithLimit(limit int) RangeOption {
	return func(opts *rangeOptions) {
		opts.Limit = limit
	}
}

// Storage is the main interface for key-value storage operations.
// It provides methods for watching changes, transaction management, and range queries.
type Storage interface {
	// Watch streams changes for a specific key or prefix.
	// Options:
	//   - WithPrefix: watch for changes on keys with the specified prefix
	Watch(ctx context.Context, key []byte, opts ...watch.Option) <-chan watch.Event

	// Tx creates a new transaction.
	// The context manages timeouts and cancellation for the transaction.
	Tx(ctx context.Context) txPkg.Tx

	// Range queries a range of keys with optional filtering.
	// Options:
	//   - WithPrefix: filter keys by prefix
	//   - WithLimit: limit the number of results returned
	Range(ctx context.Context, opts ...RangeOption) ([]kv.KeyValue, error)
}

// storageOptions contains configuration options for storage instances.
type storageOptions struct{}

// Option is a function that configures storage options.
type Option func(*storageOptions)

// WithTimeout configures a default timeout for storage operations.
// This is a dummy option for demonstration purposes.
func WithTimeout() Option {
	return func(_ *storageOptions) {
		// Dummy implementation.
	}
}

// WithRetry configures retry behavior for failed operations.
// This is a dummy option for demonstration purposes.
func WithRetry() Option {
	return func(_ *storageOptions) {
		// Dummy implementation.
	}
}

// storage is the concrete implementation of the Storage interface.
type storage struct {
	driver driver.Driver // Underlying storage driver.
}

// Watch implements the Storage interface for watching key changes.
func (s storage) Watch(_ context.Context, _ []byte, _ ...watch.Option) <-chan watch.Event {
	panic("implement me")
}

// Tx implements the Storage interface for transaction creation.
func (s storage) Tx(ctx context.Context) txPkg.Tx {
	return newTx(ctx, s.driver)
}

// Range implements the Storage interface for range queries.
func (s storage) Range(_ context.Context, _ ...RangeOption) ([]kv.KeyValue, error) {
	panic("implement me")
}

// NewStorage creates a new Storage instance with the specified driver.
// Optional StorageOption parameters can be provided to configure the storage.
func NewStorage(driver driver.Driver, _ ...Option) Storage {
	return &storage{
		driver: driver,
	}
}

// tx is the internal implementation of the Tx interface.
type tx struct {
	driver driver.Driver
	ctx    context.Context //nolint:containedctx // Context is stored for transaction execution

	predicates option.Generic[[]predicate.Predicate]
	thenOps    option.Generic[[]operation.Operation]
	elseOps    option.Generic[[]operation.Operation]
}

// newTx creates a new transaction builder with the given driver and context.
func newTx(ctx context.Context, driver driver.Driver) txPkg.Tx {
	return &tx{
		driver:     driver,
		ctx:        ctx,
		predicates: option.None[[]predicate.Predicate](),
		thenOps:    option.None[[]operation.Operation](),
		elseOps:    option.None[[]operation.Operation](),
	}
}

// If adds predicates to the transaction condition.
// Empty predicate list means always true (unconditional execution).
// If should be called before Then/Else.
func (tb *tx) If(predicates ...predicate.Predicate) txPkg.Tx {
	if tb.predicates.IsSome() {
		panic("predicates are already set")
	} else if tb.thenOps.IsSome() || tb.elseOps.IsSome() {
		panic("If can only be called before Then/Else")
	}

	tb.predicates = option.Some(predicates)

	return tb
}

// Then adds operations to execute if predicates evaluate to true.
// At least one Then call is required.
// Then can only be called before Else.
func (tb *tx) Then(operations ...operation.Operation) txPkg.Tx {
	if tb.thenOps.IsSome() {
		panic("then operations are already set")
	} else if tb.elseOps.IsSome() {
		panic("Then can only be called before Else")
	}

	tb.thenOps = option.Some(operations)

	return tb
}

// Else adds operations to execute if predicates evaluate to false.
// This is optional.
// Else can only be called before Commit.
func (tb *tx) Else(operations ...operation.Operation) txPkg.Tx {
	if tb.elseOps.IsSome() {
		panic("else operations are already set")
	}

	tb.elseOps = option.Some(operations)

	return tb
}

// Commit atomically executes the transaction by delegating to the driver.
func (tb *tx) Commit() (txPkg.Response, error) {
	resp, err := tb.driver.Execute(
		tb.ctx,
		tb.predicates.UnwrapOr(nil),
		tb.thenOps.UnwrapOr(nil),
		tb.elseOps.UnwrapOr(nil),
	)
	if err != nil {
		return txPkg.Response{}, fmt.Errorf("tx execute failed: %w", err)
	}

	return resp, nil
}
