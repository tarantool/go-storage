package etcd

import (
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-storage/operation"
)

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

	var ops []etcd.OpOption
	if storageOperation.IsPrefix() {
		ops = append(ops, etcd.WithPrefix())
	}

	switch storageOperation.Type() {
	case operation.TypeGet:
		return etcd.OpGet(key, ops...), nil
	case operation.TypePut:
		return etcd.OpPut(key, string(storageOperation.Value()), ops...), nil
	case operation.TypeDelete:
		ops = append(ops, etcd.WithPrevKV())
		return etcd.OpDelete(key, ops...), nil
	default:
		return etcd.Op{}, fmt.Errorf("%w: %v", errUnsupportedOperationType, storageOperation.Type())
	}
}
