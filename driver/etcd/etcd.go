// Package etcd provides an etcd implementation of the storage driver interface.
// It enables using etcd as a distributed key-value storage backend.
package etcd

import (
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

// Driver is an etcd implementation of the storage driver interface.
// It uses etcd as the underlying key-value storage backend.
type Driver struct {
	client *etcd.Client // etcd client instance..
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

// New creates a new etcd driver instance.
// It establishes a connection to the etcd cluster using the provided endpoints.
func New(ctx context.Context, endpoints []string) (*Driver, error) {
	client, err := etcd.New(etcd.Config{
		Context:               ctx,
		Endpoints:             endpoints,
		AutoSyncInterval:      0,
		DialTimeout:           0,
		DialKeepAliveTime:     0,
		DialKeepAliveTimeout:  0,
		MaxCallSendMsgSize:    0,
		MaxCallRecvMsgSize:    0,
		TLS:                   nil,
		Username:              "",
		Password:              "",
		RejectOldCluster:      false,
		DialOptions:           nil,
		Logger:                nil,
		LogConfig:             nil,
		PermitWithoutStream:   false,
		MaxUnaryRetries:       0,
		BackoffWaitBetween:    0,
		BackoffJitterFraction: 0,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	return &Driver{client: client}, nil
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

	for _, p := range predicates {
		cmp, err := predicateToCmp(p)
		if err != nil {
			return tx.Response{}, fmt.Errorf("failed to convert predicate: %w", err)
		}

		txn = txn.If(cmp)
	}

	thenEtcdOps, err := operationsToEtcdOps(thenOps)
	if err != nil {
		return tx.Response{}, fmt.Errorf("failed to convert then operations: %w", err)
	}

	txn = txn.Then(thenEtcdOps...)

	elseEtcdOps, err := operationsToEtcdOps(elseOps)
	if err != nil {
		return tx.Response{}, fmt.Errorf("failed to convert else operations: %w", err)
	}

	txn = txn.Else(elseEtcdOps...)

	resp, err := txn.Commit()
	if err != nil {
		return tx.Response{}, fmt.Errorf("transaction failed: %w", err)
	}

	return etcdResponseToTxResponse(resp), nil
}

// Watch monitors changes to a specific key and returns a stream of events.
// It supports optional watch configuration through the opts parameter.
func (d Driver) Watch(ctx context.Context, key []byte, _ ...watch.Option) <-chan watch.Event {
	// TODO: Apply watch options when implemented.
	eventCh := make(chan watch.Event, 100) //nolint:mnd

	go func() {
		defer close(eventCh)

		watchChan := d.client.Watch(ctx, string(key))

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

				for _, etcdEvent := range watchResp.Events {
					event := etcdEventToWatchEvent(etcdEvent)
					select {
					case eventCh <- event:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return eventCh
}

// predicateToCmp converts a predicate to an etcd comparison.
func predicateToCmp(pred predicate.Predicate) (etcd.Cmp, error) {
	switch pred.Target() {
	case predicate.TargetValue:
		return valuePredicateToCmp(pred)
	case predicate.TargetVersion:
		return versionPredicateToCmp(pred)
	default:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedPredicateTarget, pred.Target())
	}
}

// valuePredicateToCmp converts a value predicate to an etcd comparison.
func valuePredicateToCmp(pred predicate.Predicate) (etcd.Cmp, error) {
	key := string(pred.Key())
	value, ok := pred.Value().([]byte)

	if !ok {
		return etcd.Cmp{}, errValuePredicateRequiresBytes
	}

	switch pred.Operation() {
	case predicate.OpEqual:
		return etcd.Compare(etcd.Value(key), "=", value), nil
	case predicate.OpNotEqual:
		return etcd.Compare(etcd.Value(key), "!=", value), nil
	case predicate.OpGreater:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedValueOperation, pred.Operation())
	case predicate.OpLess:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedValueOperation, pred.Operation())
	default:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedValueOperation, pred.Operation())
	}
}

// versionPredicateToCmp converts a version predicate to an etcd comparison.
func versionPredicateToCmp(pred predicate.Predicate) (etcd.Cmp, error) {
	key := string(pred.Key())
	version, ok := pred.Value().(int64)

	if !ok {
		return etcd.Cmp{}, errVersionPredicateRequiresInt
	}

	switch pred.Operation() {
	case predicate.OpEqual:
		return etcd.Compare(etcd.Version(key), "=", version), nil
	case predicate.OpNotEqual:
		return etcd.Compare(etcd.Version(key), "!=", version), nil
	case predicate.OpGreater:
		return etcd.Compare(etcd.Version(key), ">", version), nil
	case predicate.OpLess:
		return etcd.Compare(etcd.Version(key), "<", version), nil
	default:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedVersionOperation, pred.Operation())
	}
}

// operationsToEtcdOps converts operations to etcd operations.
func operationsToEtcdOps(ops []operation.Operation) ([]etcd.Op, error) {
	etcdOps := make([]etcd.Op, 0, len(ops))
	for _, op := range ops {
		etcdOp, err := operationToEtcdOp(op)
		if err != nil {
			return nil, err
		}

		etcdOps = append(etcdOps, etcdOp)
	}

	return etcdOps, nil
}

// operationToEtcdOp converts an operation to an etcd operation.
func operationToEtcdOp(storageOperation operation.Operation) (etcd.Op, error) {
	key := string(storageOperation.Key())

	switch storageOperation.Type() {
	case operation.TypeGet:
		return etcd.OpGet(key), nil
	case operation.TypePut:
		return etcd.OpPut(key, string(storageOperation.Value())), nil
	case operation.TypeDelete:
		return etcd.OpDelete(key), nil
	default:
		return etcd.Op{}, fmt.Errorf("%w: %v", errUnsupportedOperationType, storageOperation.Type())
	}
}

// etcdResponseToTxResponse converts an etcd transaction response to tx.Response.
func etcdResponseToTxResponse(resp *etcd.TxnResponse) tx.Response {
	results := make([]tx.RequestResponse, 0, len(resp.Responses))

	for _, etcdResp := range resp.Responses {
		reqResp := tx.RequestResponse{
			Success:  true,
			KeyValue: nil,
			Error:    nil,
		}

		switch {
		case etcdResp.GetResponseRange() != nil:
			getResp := etcdResp.GetResponseRange()
			if len(getResp.Kvs) > 0 {
				etcdKv := getResp.Kvs[0]

				reqResp.KeyValue = &kv.KeyValue{
					Key:            etcdKv.Key,
					Value:          etcdKv.Value,
					CreateRevision: etcdKv.CreateRevision,
					ModRevision:    etcdKv.ModRevision,
					Version:        etcdKv.Version,
				}
			}
		case etcdResp.GetResponsePut() != nil:
			// Put operations don't return data.
		case etcdResp.GetResponseDeleteRange() != nil:
			// Delete operations don't return data.
		}

		results = append(results, reqResp)
	}

	return tx.Response{
		Succeeded: resp.Succeeded,
		Results:   results,
	}
}

// etcdEventToWatchEvent converts an etcd event to a watch event.
func etcdEventToWatchEvent(etcdEvent *etcd.Event) watch.Event {
	event := watch.Event{
		Type:  watch.EventType(0), // Will be set below.
		Key:   etcdEvent.Kv.Key,
		Value: nil, // Will be set below.
		Rev:   etcdEvent.Kv.ModRevision,
	}

	switch etcdEvent.Type {
	case etcd.EventTypePut:
		event.Type = watch.EventPut
		event.Value = etcdEvent.Kv.Value
	case etcd.EventTypeDelete:
		event.Type = watch.EventDelete
		event.Value = nil
	}

	return event
}
