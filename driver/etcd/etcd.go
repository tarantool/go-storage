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

// Client defines the minimal interface needed for etcd operations. It
// is compatible with etcdclientv3.Client.
//
// This allows for easier testing and mock implementations.
type Client interface {
	// Watch watches for changes on a key (using etcd's signature).
	Watch(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan
	// Txn creates a new transaction.
	Txn(ctx context.Context) etcd.Txn
}

// Driver is an etcd implementation of the storage driver interface.
// It uses etcd as the underlying key-value storage backend.
type Driver struct {
	client Client // etcd client interface.
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

// New creates a new etcd driver instance using an existing etcd client.
// The client should be properly configured and connected to an etcd cluster.
func New(client Client) *Driver {
	return &Driver{
		client: client,
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

	ctx, cancel := context.WithCancel(ctx)

	var opts []etcd.OpOption
	if bytes.HasSuffix(key, []byte("/")) {
		opts = append(opts, etcd.WithPrefix())
	}

	watchChan := d.client.Watch(ctx, string(key), opts...)

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

	return eventCh, cancel, nil
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
