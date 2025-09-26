// Package tkv provides a Tarantool Cartridge storage driver implementation.
// It enables using Tarantool as a distributed key-value storage backend.
package tkv

import (
	"context"
	"errors"
	"fmt"

	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-storage/driver"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

// DoerWatcher is an interface that combines tarantool.Doer and NewWatcher method.
// tarantool.Connection and pool.ConnectionAdapter implement this interface.
type DoerWatcher interface {
	tarantool.Doer

	NewWatcher(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error)
}

// Driver is a Tarantool implementation of the storage driver interface.
// It uses TKV as the underlying key-value storage backend.
type Driver struct {
	conn DoerWatcher // Tarantool connection pool.
}

var (
	_ driver.Driver = &Driver{} //nolint:exhaustruct

	// ErrUnexpectedResponse is returned when the response from tarantool has unexpected format.
	ErrUnexpectedResponse = errors.New("unexpected response from tarantool")
)

// New creates a new Tarantool driver instance.
// It establishes connections to Tarantool instances using the provided addresses.
func New(doer DoerWatcher) *Driver {
	return &Driver{conn: doer}
}

// Execute executes a transactional operation with conditional logic.
// It processes predicates to determine whether to execute thenOps or elseOps.
func (d Driver) Execute(
	ctx context.Context,
	predicates []predicate.Predicate,
	thenOps []operation.Operation,
	elseOps []operation.Operation,
) (tx.Response, error) {
	txnArg := newTxnRequest(predicates, thenOps, elseOps)

	req := tarantool.NewCallRequest("config.storage.txn").
		Args([]any{txnArg}).Context(ctx)

	var result []txnResponse

	switch err := d.conn.Do(req).GetTyped(&result); {
	case err != nil:
		return tx.Response{}, fmt.Errorf("failed to execute transaction: %w", err)
	case len(result) != 1:
		return tx.Response{}, fmt.Errorf("%w: expected 1 response, got %d", ErrUnexpectedResponse, len(result))
	}

	return result[0].asTxnResponse(), nil
}

// Watch monitors changes to a specific key and returns a stream of events.
// It supports optional watch configuration through the opts parameter.
// To watch for config storage key "config.storage:" prefix should be used.
func (d Driver) Watch(ctx context.Context, key []byte, _ ...watch.Option) (<-chan watch.Event, func(), error) {
	rvChan := make(chan watch.Event, 1)

	watcher, err := d.conn.NewWatcher("config.storage:"+string(key), func(_ tarantool.WatchEvent) {
		select {
		case rvChan <- watch.Event{Prefix: key}:
		default:
		}
	})
	if err != nil {
		close(rvChan)
		return nil, nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	isStopped := make(chan struct{})

	go func() {
		defer func() {
			// When watcher.Unregister() will finish it's execution - means watcher won't call any more callbacks,
			// that will write messages to rvChan, so we can close it.
			watcher.Unregister()
			close(rvChan)
		}()

		select {
		case <-ctx.Done():
		case <-isStopped:
		}
	}()

	return rvChan, func() { close(isStopped) }, nil
}
