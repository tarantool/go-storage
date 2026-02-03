// Package etcd provides an etcd implementation of the storage driver interface.
// It enables using etcd as a distributed key-value storage backend.
package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-storage/driver"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

// Client defines the minimal interface needed for etcd operations.
// This allows for easier testing and mock implementations.
type Client interface {
	// Txn creates a new transaction.
	Txn(ctx context.Context) etcd.Txn
}

// Watcher defines the interface for watching etcd changes.
// This extends the etcd.Watcher interface to match our usage pattern.
type Watcher interface {
	// Watch watches for changes on a key (using etcd's signature).
	Watch(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan
	// Close closes the watcher.
	Close() error
}

// WatcherFactory creates new watchers from a client.
type WatcherFactory interface {
	// NewWatcher creates a new watcher.
	NewWatcher(client Client) Watcher
}

// Driver is an etcd implementation of the storage driver interface.
// It uses etcd as the underlying key-value storage backend.
type Driver struct {
	client         Client         // etcd client interface.
	watcherFactory WatcherFactory // factory for creating watchers.
}

var (
	_ driver.Driver = &Driver{} //nolint:exhaustruct

	// Static error definitions to avoid dynamic errors.
	errUnsupportedPredicateTarget  = errors.New("unsupported predicate target")
	errValuePredicateRequiresBytes = errors.New("value predicate requires []byte value")
	errUnsupportedValueOperation   = errors.New("unsupported operation for value predicate")
	errVersionPredicateRequiresInt = errors.New("version predicate requires int64 value")
	errUnsupportedVersionOperation = errors.New("unsupported operation for version predicate")
	errUnsupportedOperationType    = errors.New("unsupported operation type")
)

// etcdClientAdapter wraps etcd.Client to implement our Client interface.
type etcdClientAdapter struct {
	client *etcd.Client
}

func (a *etcdClientAdapter) Txn(ctx context.Context) etcd.Txn {
	return a.client.Txn(ctx)
}

// etcdWatcherAdapter wraps etcd.Watcher to implement our Watcher interface.
type etcdWatcherAdapter struct {
	watcher etcd.Watcher
}

func (a *etcdWatcherAdapter) Watch(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
	return a.watcher.Watch(ctx, key, opts...)
}

func (a *etcdWatcherAdapter) Close() error {
	return fmt.Errorf("failed to close: %w", a.watcher.Close())
}

// etcdWatcherFactory implements WatcherFactory for etcd clients.
type etcdWatcherFactory struct{}

func (f *etcdWatcherFactory) NewWatcher(client Client) Watcher {
	// For etcd clients, we need access to the underlying client.
	if adapter, ok := client.(*etcdClientAdapter); ok {
		return &etcdWatcherAdapter{
			watcher: etcd.NewWatcher(adapter.client),
		}
	}

	// For other implementations, return a no-op watcher.
	return &noopWatcher{}
}

// noopWatcher is a no-op implementation of Watcher for non-etcd clients.
type noopWatcher struct{}

func (w *noopWatcher) Watch(_ context.Context, _ string, _ ...etcd.OpOption) etcd.WatchChan {
	ch := make(chan etcd.WatchResponse)
	close(ch)

	return ch
}

func (w *noopWatcher) Close() error {
	return nil
}

// New creates a new etcd driver instance using an existing etcd client.
// The client should be properly configured and connected to an etcd cluster.
func New(client *etcd.Client) *Driver {
	return &Driver{
		client:         &etcdClientAdapter{client: client},
		watcherFactory: &etcdWatcherFactory{},
	}
}

// Execute executes a transactional operation with conditional logic.
// It processes predicates to determine whether to execute thenOps or elseOps.
func (d Driver) Execute(
	ctx context.Context,
	predicates []predicate.Predicate,
	thenOps []operation.Operation,
	elseOps []operation.Operation,
) (tx.Response, error) {
	txn := d.client.Txn(ctx)

	convertedPredicates, err := predicatesToCmps(predicates)
	if err != nil {
		return tx.Response{}, fmt.Errorf("failed to convert predicates: %w", err)
	}

	txn.If(convertedPredicates...)

	thenEtcdOps, err := operationsToEtcdOps(thenOps)
	if err != nil {
		return tx.Response{}, fmt.Errorf("failed to convert then operations: %w", err)
	}

	txn.Then(thenEtcdOps...)

	elseEtcdOps, err := operationsToEtcdOps(elseOps)
	if err != nil {
		return tx.Response{}, fmt.Errorf("failed to convert else operations: %w", err)
	}

	txn.Else(elseEtcdOps...)

	resp, err := txn.Commit()
	if err != nil {
		return tx.Response{}, fmt.Errorf("transaction failed: %w", err)
	}

	return etcdResponseToTxResponse(resp), nil
}

const (
	eventChannelSize = 100
)

// Watch monitors changes to a specific key and returns a stream of events.
// It supports optional watch configuration through the opts parameter.
func (d Driver) Watch(ctx context.Context, key []byte, _ ...watch.Option) (<-chan watch.Event, func(), error) {
	eventCh := make(chan watch.Event, eventChannelSize)

	parentWatcher := d.watcherFactory.NewWatcher(d.client)

	var opts []etcd.OpOption
	if bytes.HasSuffix(key, []byte("/")) {
		opts = append(opts, etcd.WithPrefix())
	}

	watchChan := parentWatcher.Watch(ctx, string(key), opts...)

	go func() {
		defer close(eventCh)

		for {
			select {
			case <-ctx.Done():
				return
			case watchResp, ok := <-watchChan:
				if !ok {
					return
				}

				if watchResp.Err() != nil {
					continue
				}

				for range watchResp.Events {
					select {
					case eventCh <- watch.Event{
						Prefix: key,
					}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return eventCh, func() {
		_ = parentWatcher.Close()
	}, nil
}

// etcdResponseToTxResponse converts an etcd transaction response to tx.Response.
func etcdResponseToTxResponse(resp *etcd.TxnResponse) tx.Response {
	results := make([]tx.RequestResponse, 0, len(resp.Responses))

	for _, etcdResp := range resp.Responses {
		var values []kv.KeyValue

		switch {
		case etcdResp.GetResponseRange() != nil:
			getResp := etcdResp.GetResponseRange()
			for _, etcdKv := range getResp.Kvs {
				values = append(values, kv.KeyValue{
					Key:         etcdKv.Key,
					Value:       etcdKv.Value,
					ModRevision: etcdKv.ModRevision,
				})
			}
		case etcdResp.GetResponsePut() != nil:
			// Put operations don't return data.
		case etcdResp.GetResponseDeleteRange() != nil:
			deleteResp := etcdResp.GetResponseDeleteRange()
			for _, etcdKv := range deleteResp.PrevKvs {
				values = append(values, kv.KeyValue{
					Key:         etcdKv.Key,
					Value:       etcdKv.Value,
					ModRevision: etcdKv.ModRevision,
				})
			}
		}

		results = append(results, tx.RequestResponse{
			Values: values,
		})
	}

	return tx.Response{
		Succeeded: resp.Succeeded,
		Results:   results,
	}
}
