package tcs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/driver/tcs"
	"github.com/tarantool/go-storage/locker"
)

const lockerTestTTL = 3 * time.Second

// lockerTestSlack is extra time beyond lockerTestTTL for a key to expire.
const lockerTestSlack = 4 * time.Second

func testLockerKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("/locker-test/%s/%s", t.Name(), suffix)
}

// createTestLockerDriver builds a TCS driver and skips the test when the
// server does not advertise ttl+keepalive.
func createTestLockerDriver(ctx context.Context, t *testing.T) (*tcs.Driver, func()) {
	t.Helper()

	driver, done := createTestDriver(ctx, t)

	// Probing features requires going through NewLocker — infoFeatures is
	// package-private.
	_, err := driver.NewLocker(ctx, "/probe", locker.WithTTL(lockerTestTTL))
	if errors.Is(err, tcs.ErrUnsupportedFeatures) {
		done()
		t.Skip("TCS instance does not support ttl+keepalive — skipping locker integration tests")
	}

	require.NoError(t, err)

	return driver, done
}

func TestLocker_NewLocker_InvalidName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	t.Cleanup(done)

	cases := []struct {
		name string
		desc string
	}{
		{"no-leading-slash", "no leading slash"},
		{"/trailing/slash/", "trailing slash"},
		{"", "empty string"},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			_, err := driver.NewLocker(ctx, tc.name, locker.WithTTL(lockerTestTTL))
			require.Error(t, err, "expected error for name %q", tc.name)
		})
	}
}

func TestLocker_AcquireRelease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "basic")

	lock, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	assert.NotEmpty(t, lock.Key(), "Key() must be non-empty after Lock")

	require.NoError(t, lock.Unlock(ctx))
	assert.Empty(t, lock.Key(), "Key() must be empty after Unlock")
}

func TestLocker_Unlock_NotHeld(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "not-held")

	lock, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	err = lock.Unlock(ctx)
	require.ErrorIs(t, err, locker.ErrLockReleased)
}

func TestLocker_Lock_Idempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "idempotent")

	lock, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	require.NoError(t, lock.Lock(ctx), "re-Lock on held locker must be a no-op")
	require.NoError(t, lock.Unlock(ctx))
}

func TestLocker_Contention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "contention")

	lockerA, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lockerA.Lock(ctx))

	lockerB, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	bDone := make(chan error, 1)

	go func() {
		bDone <- lockerB.Lock(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	select {
	case err := <-bDone:
		t.Fatalf("B acquired lock before A released it (err=%v)", err)
	default:
	}

	require.NoError(t, lockerA.Unlock(ctx))

	select {
	case err := <-bDone:
		require.NoError(t, err, "B should acquire lock after A unlocks")
	case <-time.After(10 * time.Second):
		t.Fatal("B did not acquire lock within timeout after A released")
	}

	require.NoError(t, lockerB.Unlock(ctx))
}

func TestLocker_TryLock_Contended(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "try-lock")

	lockerA, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lockerA.Lock(ctx))

	lockerB, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	err = lockerB.TryLock(ctx)
	require.ErrorIs(t, err, locker.ErrLocked, "TryLock must return ErrLocked when another holder is present")

	require.NoError(t, lockerA.Unlock(ctx))
}

func TestLocker_TryLock_Free(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "try-lock-free")

	lock, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lock.TryLock(ctx))
	assert.NotEmpty(t, lock.Key())
	require.NoError(t, lock.Unlock(ctx))
}

func TestLocker_CtxCancel_CleansUp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "ctx-cancel")

	lockerA, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lockerA.Lock(ctx))

	cancelCtx, cancel := context.WithCancel(ctx)

	lockerB, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	bDone := make(chan error, 1)

	go func() {
		bDone <- lockerB.Lock(cancelCtx)
	}()

	time.Sleep(500 * time.Millisecond)

	cancel()

	select {
	case err := <-bDone:
		require.Error(t, err, "B.Lock should return an error after ctx cancel")
	case <-time.After(5 * time.Second):
		t.Fatal("B did not unblock within timeout after ctx cancel")
	}

	require.NoError(t, lockerA.Unlock(ctx))

	lockerC, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lockerC.TryLock(ctx), "C should acquire lock after A and B are gone")
	require.NoError(t, lockerC.Unlock(ctx))
}

//nolint:paralleltest // uses a short TTL + real time.Sleep; running in parallel races on shared TTL state.
func TestLocker_LifetimeCtxCancel_KeyExpires(t *testing.T) {
	ctx := context.Background()

	driver, done := createTestLockerDriver(ctx, t)
	defer done()

	name := testLockerKey(t, "lifetime-expire")

	lifeCtx, lifeCancel := context.WithCancel(ctx)

	lockerA, err := driver.NewLocker(lifeCtx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lockerA.Lock(ctx))

	// Cancel the lifetime context: keepalive stops, TTL is no longer renewed,
	// and the key expires within lockerTestTTL.
	lifeCancel()

	time.Sleep(lockerTestTTL + lockerTestSlack)

	lockerB, err := driver.NewLocker(ctx, name, locker.WithTTL(lockerTestTTL))
	require.NoError(t, err)

	require.NoError(t, lockerB.TryLock(ctx), "B should acquire lock after A's TTL expired")
	require.NoError(t, lockerB.Unlock(ctx))
}
