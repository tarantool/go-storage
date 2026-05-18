//nolint:paralleltest // see integration_test.go for the package-level rationale.
package etcd_test

import (
	"context"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdclient "go.etcd.io/etcd/client/v3"
	etcdfintegration "go.etcd.io/etcd/tests/v3/framework/integration"

	etcddriver "github.com/tarantool/go-storage/driver/etcd"
	"github.com/tarantool/go-storage/internal/mocks"
	"github.com/tarantool/go-storage/internal/testing/etcd"
	"github.com/tarantool/go-storage/locker"
)

const (
	// lockerTestTimeout is generous to absorb -race + slow CI overhead on
	// etcd lease/raft round-trips. It is used both as per-test outer
	// deadline and as the internal time.After bound for cross-locker waits.
	lockerTestTimeout = 30 * time.Second
)

// testLockName derives a per-test lock name so different tests cannot
// accidentally interfere via shared etcd keys (each test runs against its
// own cluster, but a per-test prefix is a cheap belt-and-braces guard).
func testLockName(t *testing.T) string {
	t.Helper()

	return "/test/locker/" + t.Name()
}

// createTestDriverConcrete builds a Driver from a concrete *etcd.Client
// (the locker requires the concrete type).
func createTestDriverConcrete(t *testing.T) (*etcddriver.Driver, *etcdclient.Client) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	etcdfintegration.BeforeTest(etcd.NewSilentTB(t), etcdfintegration.WithoutGoLeakDetection())

	cluster := etcd.NewLazyCluster()

	t.Cleanup(func() { cluster.Terminate() })

	endpoints := cluster.EndpointsGRPC()

	client, err := etcdclient.New(etcdclient.Config{ //nolint:exhaustruct
		Endpoints:   endpoints,
		DialTimeout: testDialTimeout,
	})
	require.NoError(t, err, "failed to create etcd client")
	t.Cleanup(func() { _ = client.Close() })

	return etcddriver.New(client), client
}

func createSecondClient(t *testing.T, endpoints []string) *etcdclient.Client {
	t.Helper()

	client, err := etcdclient.New(etcdclient.Config{ //nolint:exhaustruct
		Endpoints:   endpoints,
		DialTimeout: testDialTimeout,
	})
	require.NoError(t, err, "failed to create second etcd client")
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestLocker_AcquireRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	driver, _ := createTestDriverConcrete(t)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)
	require.NotNil(t, lock)

	assert.Empty(t, lock.Key())

	require.NoError(t, lock.Lock(ctx))

	assert.NotEmpty(t, lock.Key())

	require.NoError(t, lock.Unlock(ctx))
}

func TestLocker_ReLock_IsNoOp(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	driver, _ := createTestDriverConcrete(t)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Unlock(ctx))
}

func TestLocker_Unlock_NeverLocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	driver, _ := createTestDriverConcrete(t)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	err = lock.Unlock(ctx)
	require.ErrorIs(t, err, locker.ErrLockReleased)
}

func TestLocker_DoubleUnlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	driver, _ := createTestDriverConcrete(t)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Unlock(ctx))

	err = lock.Unlock(ctx)
	require.ErrorIs(t, err, locker.ErrLockReleased)
}

func TestLocker_Contention(t *testing.T) {
	// Outer timeout is generous (3x per-op timeout) because the test does
	// several Lock/Unlock round-trips and an internal time.After wait of
	// lockerTestTimeout. The point is only to prevent infinite hangs.
	ctx, cancel := context.WithTimeout(t.Context(), 3*lockerTestTimeout)
	t.Cleanup(cancel)

	driverA, clientA := createTestDriverConcrete(t)
	endpoints := clientA.Endpoints()
	clientB := createSecondClient(t, endpoints)
	driverB := etcddriver.New(clientB)

	lockName := testLockName(t)

	lockA, err := driverA.NewLocker(ctx, lockName)
	require.NoError(t, err)

	lockB, err := driverB.NewLocker(ctx, lockName)
	require.NoError(t, err)

	require.NoError(t, lockA.Lock(ctx))

	bAcquired := make(chan error, 1)

	go func() {
		bAcquired <- lockB.Lock(ctx)
	}()

	select {
	case err := <-bAcquired:
		t.Fatalf("B acquired lock unexpectedly before A released (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, lockA.Unlock(ctx))

	select {
	case err := <-bAcquired:
		require.NoError(t, err, "B should have acquired lock after A released")
	case <-time.After(lockerTestTimeout):
		t.Fatal("B did not acquire lock within timeout after A released")
	}

	require.NoError(t, lockB.Unlock(ctx))
}

func TestLocker_TryLock_Contended(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	driverA, clientA := createTestDriverConcrete(t)
	endpoints := clientA.Endpoints()
	clientB := createSecondClient(t, endpoints)
	driverB := etcddriver.New(clientB)

	lockName := testLockName(t)

	lockA, err := driverA.NewLocker(ctx, lockName)
	require.NoError(t, err)

	lockB, err := driverB.NewLocker(ctx, lockName)
	require.NoError(t, err)

	require.NoError(t, lockA.Lock(ctx))

	err = lockB.TryLock(ctx)
	require.ErrorIs(t, err, locker.ErrLocked)

	require.NoError(t, lockA.Unlock(ctx))
}

func TestLocker_CtxCancellation_AbortsLock(t *testing.T) {
	// 3x per-op timeout to cover several Lock/Unlock round-trips plus an
	// internal time.After(lockerTestTimeout) wait for the C locker.
	ctx, cancel := context.WithTimeout(t.Context(), 3*lockerTestTimeout)
	t.Cleanup(cancel)

	driverA, clientA := createTestDriverConcrete(t)
	endpoints := clientA.Endpoints()
	clientB := createSecondClient(t, endpoints)
	driverB := etcddriver.New(clientB)

	lockName := testLockName(t)

	lockA, err := driverA.NewLocker(ctx, lockName)
	require.NoError(t, err)

	require.NoError(t, lockA.Lock(ctx))

	callCtx, callCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer callCancel()

	lockB, err := driverB.NewLocker(ctx, lockName)
	require.NoError(t, err)

	start := time.Now()

	err = lockB.Lock(callCtx)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, 2*time.Second)

	require.NoError(t, lockA.Unlock(ctx))

	clientC := createSecondClient(t, endpoints)
	driverC := etcddriver.New(clientC)

	lockC, err := driverC.NewLocker(ctx, lockName)
	require.NoError(t, err)

	acquiredC := make(chan error, 1)

	go func() {
		acquiredC <- lockC.Lock(ctx)
	}()

	select {
	case err := <-acquiredC:
		require.NoError(t, err, "fresh locker C should acquire after A released and B's ctx cancelled")
	case <-time.After(lockerTestTimeout):
		t.Fatal("fresh locker C did not acquire within timeout")
	}

	require.NoError(t, lockC.Unlock(ctx))
}

func TestLocker_NewLockerOnNonConcreteClient_ReturnsErrUnsupported(t *testing.T) {
	mc := minimock.NewController(t)

	client := mocks.NewEtcdClientMock(mc)
	driver := etcddriver.New(client)

	lock, err := driver.NewLocker(t.Context(), testLockName(t))
	assert.Nil(t, lock)
	require.ErrorIs(t, err, locker.ErrUnsupported)
}
