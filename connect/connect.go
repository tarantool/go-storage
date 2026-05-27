// Package connect provides utilities for connecting to storage backends.
package connect

import (
	"context"
	"fmt"

	"github.com/tarantool/go-storage"
	etcddriver "github.com/tarantool/go-storage/driver/etcd"
	tcsdriver "github.com/tarantool/go-storage/driver/tcs"
)

// CleanupFunc is a function type for cleaning up resources.
type CleanupFunc func()

// NewEtcdStorage creates a new storage instance connected to an etcd cluster.
func NewEtcdStorage(ctx context.Context, cfg Config) (storage.Storage, CleanupFunc, error) {
	client, cleanup, err := createEtcdClient(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	drv := etcddriver.New(client)

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

// NewStorage creates a new storage instance by trying to connect to etcd first, then TCS.
func NewStorage(_ context.Context, cfg Config) (storage.Storage, CleanupFunc, error) {
	etcdCtx, etcdCancel := context.WithTimeout(context.Background(), cfg.dialTimeout())
	defer etcdCancel()

	stor, cleanup, err := NewEtcdStorage(etcdCtx, cfg) //nolint:contextcheck
	if err == nil {
		return stor, cleanup, nil
	}

	etcdErr := err

	tcsCtx, tcsCancel := context.WithTimeout(context.Background(), cfg.dialTimeout())
	defer tcsCancel()

	stor, cleanup, err = NewTCSStorage(tcsCtx, cfg) //nolint:contextcheck
	if err == nil {
		return stor, cleanup, nil
	}

	return nil, nil, fmt.Errorf("failed to connect to etcd: %w; failed to connect to TCS: %w",
		etcdErr, err)
}
