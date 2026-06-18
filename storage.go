package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/tarantool/go-option"

	"github.com/tarantool/go-storage/v2/driver"
	"github.com/tarantool/go-storage/v2/kv"
	"github.com/tarantool/go-storage/v2/locker"
	"github.com/tarantool/go-storage/v2/operation"
	"github.com/tarantool/go-storage/v2/predicate"
	txPkg "github.com/tarantool/go-storage/v2/tx"
	"github.com/tarantool/go-storage/v2/watch"
)

// rangeOptions contains configuration options for range operations.
type rangeOptions struct {
	Prefix string // Prefix filter for range queries.
}

// RangeOption is a function that configures range operation options.
type RangeOption func(*rangeOptions)

// WithPrefix configures a range operation to filter keys by the specified prefix.
func WithPrefix(prefix string) RangeOption {
	return func(opts *rangeOptions) {
		opts.Prefix = prefix
	}
}

// Storage is the main interface for key-value storage operations.
// It provides methods for watching changes, transaction management, and range queries.
type Storage interface {
	// Watch streams changes for a specific key, or for a key prefix when key
	// ends with "/".
	Watch(ctx context.Context, key []byte) <-chan watch.Event

	// Tx creates a new transaction.
	// The context manages timeouts and cancellation for the transaction.
	Tx(ctx context.Context) txPkg.Tx

	// TxFactory returns a tx.Factory bound to this Storage. Use it to hand
	// "begin transaction" capability to components that do not need the full
	// Storage interface.
	TxFactory() txPkg.Factory

	// Range queries a range of keys, filtered by WithPrefix.
	Range(ctx context.Context, opts ...RangeOption) ([]kv.KeyValue, error)

	// NewLocker creates a Locker for name. ctx is the locker-lifetime context:
	// cancelling it stops any keepalive goroutine and aborts a blocking Lock.
	NewLocker(ctx context.Context, name string, opts ...locker.Option) (locker.Locker, error)

	// LockerFactory returns a locker.Factory bound to this Storage. Use it to
	// hand "create a lock" capability to components that do not need the full
	// Storage interface.
	LockerFactory() locker.Factory
}

// storage is the concrete implementation of the Storage interface.
type storage struct {
	driver driver.Driver // Underlying storage driver.
}

var _ Storage = (*storage)(nil)

// Watch implements the Storage interface for watching key changes.
func (s storage) Watch(ctx context.Context, key []byte) <-chan watch.Event {
	eventCh, cleanup, err := s.driver.Watch(ctx, key)
	if err != nil {
		// Return a closed channel on error.
		ch := make(chan watch.Event)
		close(ch)

		return ch
	}

	if cleanup != nil {
		var once sync.Once

		wrapperChan := make(chan watch.Event, cap(eventCh))
		done := make(chan struct{})

		go func() {
			defer close(wrapperChan)
			defer close(done)

			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-eventCh:
					if !ok {
						return
					}

					select {
					case <-ctx.Done():
						return
					case wrapperChan <- event:
					}
				}
			}
		}()

		go func() {
			select {
			case <-ctx.Done():
			case <-done:
			}

			once.Do(cleanup)
		}()

		return wrapperChan
	}

	return eventCh
}

// Tx implements the Storage interface for transaction creation.
func (s storage) Tx(ctx context.Context) txPkg.Tx {
	return newTx(ctx, s.driver)
}

// TxFactory implements the Storage interface for transaction-factory creation.
func (s storage) TxFactory() txPkg.Factory {
	return s.Tx
}

// Range implements the Storage interface for range queries.
func (s storage) Range(ctx context.Context, opts ...RangeOption) ([]kv.KeyValue, error) {
	rangeOpts := &rangeOptions{Prefix: ""}
	for _, opt := range opts {
		opt(rangeOpts)
	}

	if rangeOpts.Prefix == "" {
		return nil, nil
	}

	// Create a Get operation with the prefix.
	key := rangeOpts.Prefix
	if key != "" && !strings.HasSuffix(key, "/") {
		key += "/"
	}

	ops := []operation.Operation{operation.Get([]byte(key))}

	response, err := s.driver.Execute(ctx, nil, ops, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute ops: %w", err)
	}

	var kvs []kv.KeyValue
	for _, r := range response.Results {
		kvs = append(kvs, r.Values...)
	}

	return kvs, nil
}

func (s storage) NewLocker(ctx context.Context, name string, opts ...locker.Option) (locker.Locker, error) {
	lock, err := s.driver.NewLocker(ctx, name, opts...)
	if err != nil {
		return nil, fmt.Errorf("new-locker: %w", err)
	}

	return lock, nil
}

// LockerFactory implements the Storage interface for locker-factory creation.
func (s storage) LockerFactory() locker.Factory {
	return s
}

// NewStorage creates a new Storage instance with the specified driver.
func NewStorage(driver driver.Driver) Storage {
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
