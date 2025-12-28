package etcd

import (
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-storage/operation"
)

func optionsToEtcdOptions(opts []operation.Option) []etcd.OpOption {
	etcdOpts := make([]etcd.OpOption, 0, len(opts))

	for _, opt := range opts {
		if opt.WithPrefix {
			etcdOpts = append(etcdOpts, etcd.WithPrefix())
		}
	}

	return etcdOpts
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

	ops := optionsToEtcdOptions(storageOperation.Options())

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
