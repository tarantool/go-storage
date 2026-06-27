// Package connect provides utilities for connecting to storage backends.
package connect

import (
	"context"
	"fmt"

	"github.com/tarantool/go-storage/v2"
	etcddriver "github.com/tarantool/go-storage/v2/driver/etcd"
	tcsdriver "github.com/tarantool/go-storage/v2/driver/tcs"
)

// CleanupFunc is a function type for cleaning up resources.
type CleanupFunc func()

// NewEtcdStorage creates a new storage instance connected to an etcd cluster.
func NewEtcdStorage(ctx context.Context, cfg Config) (storage.Storage, CleanupFunc, error) {
	client, cleanup, err := createEtcdClient(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	// NewWithLocker so storage built via connect supports locking; client is the
	// concrete *etcd.Client the concurrency package requires.
	drv := etcddriver.NewWithLocker(client)

	return storage.NewStorage(drv), cleanup, nil
}

// NewTCSStorage creates a new storage instance connected to a Tarantool Cartridge Server (TCS).
func NewTCSStorage(ctx context.Context, cfg Config) (storage.Storage, CleanupFunc, error) {
	conn, cleanup, err := createTCSConnection(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	drv := tcsdriver.New(conn)

	return storage.NewStorage(drv), cleanup, nil
}

// Connect creates a storage instance by probing backends: it tries etcd first,
// then falls back to TCS. For a specific backend use NewEtcdStorage or
// NewTCSStorage directly.
func Connect(ctx context.Context, cfg Config) (storage.Storage, CleanupFunc, error) {
	stor, cleanup, err := NewEtcdStorage(ctx, cfg)
	if err == nil {
		return stor, cleanup, nil
	}

	etcdErr := err

	stor, cleanup, err = NewTCSStorage(ctx, cfg)
	if err == nil {
		return stor, cleanup, nil
	}

	return nil, nil, fmt.Errorf("failed to connect to etcd: %w; failed to connect to TCS: %w",
		etcdErr, err)
}
