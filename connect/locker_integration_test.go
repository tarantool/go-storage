//go:build integration

//nolint:paralleltest
package connect_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/connect"
	"github.com/tarantool/go-storage/locker"
)

// TestNewEtcdStorage_NewLocker verifies that the Storage returned by
// connect.NewEtcdStorage supports locking end-to-end. The connect path builds
// the driver with a concrete *etcdclient.Client, so the etcd driver's runtime
// *etcd.Client assertion in NewLocker must succeed — i.e. NewLocker must
// neither return locker.ErrUnsupported nor a nil locker.
func TestNewEtcdStorage_NewLocker(t *testing.T) {
	cfg := createEtcdTestConfig(t)

	ctx := t.Context()
	stor, cancel, err := connect.NewEtcdStorage(ctx, cfg)
	require.NoError(t, err)

	defer cancel()

	lk, err := stor.NewLocker(ctx, "/"+t.Name()+"/lock")
	require.NoError(t, err)
	require.False(t, errors.Is(err, locker.ErrUnsupported),
		"NewEtcdStorage builds the driver with a concrete *etcd.Client, so NewLocker must be supported")
	assert.NotNil(t, lk)

	// Acquire and release to confirm the locker is wired correctly.
	require.NoError(t, lk.Lock(ctx))
	require.NoError(t, lk.Unlock(ctx))
}
