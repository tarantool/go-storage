//nolint:paralleltest // see integration_test.go for the package-level rationale.
package etcd_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdclient "go.etcd.io/etcd/client/v3"

	etcddriver "github.com/tarantool/go-storage/v2/driver/etcd"
	"github.com/tarantool/go-storage/v2/internal/mocks"
	"github.com/tarantool/go-storage/v2/locker"
)

const (
	// lockerTestTimeout is generous to absorb -race + slow CI overhead on
	// etcd lease/raft round-trips. It is used both as per-test outer
	// deadline and as the internal time.After bound for cross-locker waits.
	//
	// The outer ctx is started AFTER createTestDriverConcrete so the budget
	// covers only the actual locker operations, not the embedded etcd
	// cluster bootstrap — which can take >20s on slow CI runners and would
	// otherwise eat the whole budget, leaving Unlock to fail at
	// session.Close→Revoke with "context deadline exceeded".
	lockerTestTimeout = 30 * time.Second
)

// lockerTestSeq guarantees a fresh lock name on every invocation, even when
// the same test function is re-run under `-count=N` against the shared etcd
// cluster. t.Name() alone would collide across counts (no `#NN` suffix is
// added) and leave a previous iteration's contender blocking on a key the
// next iteration is also trying to acquire.
//
//nolint:gochecknoglobals
var lockerTestSeq atomic.Uint64

// testLockName derives a unique lock name so different tests — and successive
// runs of the same test under -count=N — cannot interfere via the shared
// etcd cluster's keyspace.
func testLockName(t *testing.T) string {
	t.Helper()

	return fmt.Sprintf("/test/locker/%s/%d", t.Name(), lockerTestSeq.Add(1))
}

// createTestDriverConcrete builds a Driver from a concrete *etcd.Client
// (the locker requires the concrete type). It reuses sharedEtcdCluster from
// integration_test.go.
func createTestDriverConcrete(t *testing.T) (*etcddriver.Driver, *etcdclient.Client) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	endpoints := sharedEtcdCluster.EndpointsGRPC()

	client, err := etcdclient.New(etcdclient.Config{ //nolint:exhaustruct
		Endpoints:   endpoints,
		DialTimeout: testDialTimeout,
	})
	require.NoError(t, err, "failed to create etcd client")
	t.Cleanup(func() { _ = client.Close() })

	return etcddriver.NewWithLocker(client), client
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
	driver, _ := createTestDriverConcrete(t)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)
	require.NotNil(t, lock)

	assert.Empty(t, lock.Key())

	require.NoError(t, lock.Lock(ctx))

	assert.NotEmpty(t, lock.Key())

	require.NoError(t, lock.Unlock(ctx))
}

func TestLocker_ReLock_IsNoOp(t *testing.T) {
	driver, _ := createTestDriverConcrete(t)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Unlock(ctx))
}

func TestLocker_Unlock_NeverLocked(t *testing.T) {
	driver, _ := createTestDriverConcrete(t)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	err = lock.Unlock(ctx)
	require.ErrorIs(t, err, locker.ErrLockReleased)
}

func TestLocker_DoubleUnlock(t *testing.T) {
	driver, _ := createTestDriverConcrete(t)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Unlock(ctx))

	err = lock.Unlock(ctx)
	require.ErrorIs(t, err, locker.ErrLockReleased)
}

func TestLocker_Contention(t *testing.T) {
	driverA, clientA := createTestDriverConcrete(t)
	endpoints := clientA.Endpoints()
	clientB := createSecondClient(t, endpoints)
	driverB := etcddriver.NewWithLocker(clientB)

	// Outer timeout is generous (3x per-op timeout) because the test does
	// several Lock/Unlock round-trips and an internal time.After wait of
	// lockerTestTimeout. The point is only to prevent infinite hangs.
	ctx, cancel := context.WithTimeout(t.Context(), 3*lockerTestTimeout)
	t.Cleanup(cancel)

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
	driverA, clientA := createTestDriverConcrete(t)
	endpoints := clientA.Endpoints()
	clientB := createSecondClient(t, endpoints)
	driverB := etcddriver.NewWithLocker(clientB)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

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
	driverA, clientA := createTestDriverConcrete(t)
	endpoints := clientA.Endpoints()
	clientB := createSecondClient(t, endpoints)
	driverB := etcddriver.NewWithLocker(clientB)

	// 3x per-op timeout to cover several Lock/Unlock round-trips plus an
	// internal time.After(lockerTestTimeout) wait for the C locker.
	ctx, cancel := context.WithTimeout(t.Context(), 3*lockerTestTimeout)
	t.Cleanup(cancel)

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
	driverC := etcddriver.NewWithLocker(clientC)

	lockC, err := driverC.NewLocker(ctx, lockName)
	require.NoError(t, err)

	// Lock is bounded by ctx (3*lockerTestTimeout), so it cannot hang the
	// test. An independent, tighter time.After tripwire here used to flake
	// on slow CI runners under -race when an etcd round-trip stalled, even
	// though Lock would still have acquired within ctx.
	require.NoError(t, lockC.Lock(ctx),
		"fresh locker C should acquire after A released and B's ctx cancelled")

	require.NoError(t, lockC.Unlock(ctx))
}

func TestLocker_Done_NeverLocked_IsClosed(t *testing.T) {
	driver, _ := createTestDriverConcrete(t)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	select {
	case <-lock.Done():
	default:
		t.Fatal("Done() on a never-locked locker must be already closed")
	}
}

func TestLocker_Done_WhileHeld_IsOpen(t *testing.T) {
	driver, _ := createTestDriverConcrete(t)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	t.Cleanup(func() { _ = lock.Unlock(ctx) })

	select {
	case <-lock.Done():
		t.Fatal("Done() must not be closed while the lock is held")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestLocker_Done_ClosedAfterUnlock(t *testing.T) {
	driver, _ := createTestDriverConcrete(t)

	ctx, cancel := context.WithTimeout(t.Context(), lockerTestTimeout)
	t.Cleanup(cancel)

	lock, err := driver.NewLocker(ctx, testLockName(t))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))

	doneCh := lock.Done()
	require.NoError(t, lock.Unlock(ctx))

	select {
	case <-doneCh:
	case <-time.After(lockerTestTimeout):
		t.Fatal("Done() channel must be closed after Unlock")
	}
}

func TestLocker_NewLockerWithoutLockSupport_ReturnsErrUnsupported(t *testing.T) {
	mc := minimock.NewController(t)

	// A driver built with New (rather than NewWithLocker) does not support
	// locking regardless of the client type.
	client := mocks.NewEtcdClientMock(mc)
	driver := etcddriver.New(client)

	lock, err := driver.NewLocker(t.Context(), testLockName(t))
	assert.Nil(t, lock)
	require.ErrorIs(t, err, locker.ErrUnsupported)
}
