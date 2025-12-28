package etcd //nolint:testpackage

import (
	"testing"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/operation"
)

func TestOptionsToEtcdOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		opts         []operation.Option
		expectedOpts int
	}{
		{
			name: "delete options with prefix",
			opts: []operation.Option{
				{
					WithPrefix: true,
				},
			},
			expectedOpts: 1,
		},
		{
			name:         "empty options",
			opts:         []operation.Option{},
			expectedOpts: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			etcdOpts := optionsToEtcdOptions(tt.opts)
			require.Len(t, etcdOpts, tt.expectedOpts)
		})
	}
}

func TestOperationsToEtcdOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		operations  []operation.Operation
		expectedOps int
	}{
		{
			name:        "empty operations slice",
			operations:  []operation.Operation{},
			expectedOps: 0,
		},
		{
			name: "single get operation",
			operations: []operation.Operation{
				operation.Get([]byte("test-key")),
			},
			expectedOps: 1,
		},
		{
			name: "multiple operations",
			operations: []operation.Operation{
				operation.Get([]byte("key1")),
				operation.Put([]byte("key2"), []byte("value2")),
				operation.Delete([]byte("key3")),
			},
			expectedOps: 3,
		},
		{
			name: "single put operation",
			operations: []operation.Operation{
				operation.Put([]byte("test-key"), []byte("test-value")),
			},
			expectedOps: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			etcdOps, err := operationsToEtcdOps(tt.operations)

			require.NoError(t, err)

			if tt.expectedOps > 0 {
				assert.Len(t, etcdOps, tt.expectedOps)
			} else {
				assert.Empty(t, etcdOps)
			}
		})
	}
}

func TestOperationToEtcdOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		operation   operation.Operation
		checkerFunc func(op etcd.Op)
	}{
		{
			name:      "get operation",
			operation: operation.Get([]byte("test-key")),
			checkerFunc: func(op etcd.Op) {
				assert.True(t, op.IsGet())
				assert.Equal(t, op.KeyBytes(), []byte("test-key"))
			},
		},
		{
			name:      "put operation",
			operation: operation.Put([]byte("test-key"), []byte("test-value")),
			checkerFunc: func(op etcd.Op) {
				assert.True(t, op.IsPut())
				assert.Equal(t, op.KeyBytes(), []byte("test-key"))
				assert.Equal(t, op.ValueBytes(), []byte("test-value"))
			},
		},
		{
			name:      "delete operation",
			operation: operation.Delete([]byte("test-key")),
			checkerFunc: func(op etcd.Op) {
				assert.True(t, op.IsDelete())
				assert.Equal(t, op.KeyBytes(), []byte("test-key"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			etcdOp, err := operationToEtcdOp(tt.operation)

			require.NoError(t, err)
			assert.NotNil(t, etcdOp)

			tt.checkerFunc(etcdOp)
		})
	}
}
