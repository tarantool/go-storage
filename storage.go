package storage

import (
	"context"

	"github.com/tarantool/go-storage/driver"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/tx"
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
	Tx(ctx context.Context) tx.Tx

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
func (s storage) Tx(_ context.Context) tx.Tx {
	panic("implement me")
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
