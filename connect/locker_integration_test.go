//go:build integration

//nolint:paralleltest
package connect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/connect"
	"github.com/tarantool/go-storage/v2/locker"
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

	lock, err := stor.NewLocker(ctx, "/"+t.Name()+"/lock")
	require.NoError(t, err)
	require.NotErrorIs(t, err, locker.ErrUnsupported,
		"NewEtcdStorage builds the driver with a concrete *etcd.Client, so NewLocker must be supported")
	assert.NotNil(t, lock)

	// Acquire and release to confirm the locker is wired correctly.
	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Unlock(ctx))
}
