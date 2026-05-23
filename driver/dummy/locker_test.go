package dummy_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/locker"
)

func newLocker(t *testing.T, name string) locker.Locker {
	t.Helper()

	ctx := context.Background()
	d := dummy.New()
	lk, err := d.NewLocker(ctx, name)
	require.NoError(t, err)
	require.NotNil(t, lk)

	return lk
}

func TestDummyLocker_LockUnlock_HappyPath(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "test-lock")

	assert.Empty(t, lock.Key())

	err := lock.Lock(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "test-lock", lock.Key())

	err = lock.Unlock(context.Background())
	require.NoError(t, err)

	assert.Empty(t, lock.Key())
}

func TestDummyLocker_ReLock_IsNoOp(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "relock-test")

	err := lock.Lock(context.Background())
	require.NoError(t, err)

	err = lock.Lock(context.Background())
	require.NoError(t, err)

	err = lock.Unlock(context.Background())
	require.NoError(t, err)
}

func TestDummyLocker_TryLock_Contended(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d := dummy.New()

	lockA, err := d.NewLocker(ctx, "trylock-contend")
	require.NoError(t, err)

	lockB, err := d.NewLocker(ctx, "trylock-contend")
	require.NoError(t, err)

	require.NoError(t, lockA.Lock(ctx))

	err = lockB.TryLock(ctx)
	require.ErrorIs(t, err, locker.ErrLocked)

	require.NoError(t, lockA.Unlock(ctx))
}

func TestDummyLocker_TryLock_Free(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "trylock-free")

	err := lock.TryLock(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "trylock-free", lock.Key())

	require.NoError(t, lock.Unlock(context.Background()))
}

func TestDummyLocker_TryLock_AlreadyHeld(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "trylock-held")

	require.NoError(t, lock.Lock(context.Background()))
	require.NoError(t, lock.TryLock(context.Background()))
	require.NoError(t, lock.Unlock(context.Background()))
}

func TestDummyLocker_DoubleUnlock(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "double-unlock")

	require.NoError(t, lock.Lock(context.Background()))
	require.NoError(t, lock.Unlock(context.Background()))

	err := lock.Unlock(context.Background())
	require.ErrorIs(t, err, locker.ErrLockReleased)
}

func TestDummyLocker_Unlock_NeverLocked(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "never-locked")

	err := lock.Unlock(context.Background())
	require.ErrorIs(t, err, locker.ErrLockReleased)
}

func TestDummyLocker_Lock_CtxCancellation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := dummy.New()

	lockA, err := driver.NewLocker(ctx, "ctx-cancel")
	require.NoError(t, err)
	require.NoError(t, lockA.Lock(ctx))

	lockB, err := driver.NewLocker(ctx, "ctx-cancel")
	require.NoError(t, err)

	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	start := time.Now()

	err = lockB.Lock(callCtx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, callCtx.Err())
	assert.Less(t, elapsed, 1*time.Second)

	err = lockB.Unlock(ctx)
	require.ErrorIs(t, err, locker.ErrLockReleased)

	require.NoError(t, lockA.Unlock(ctx))
}

func TestDummyLocker_TwoLockers_SameName_Contend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := dummy.New()

	lockA, err := driver.NewLocker(ctx, "shared-name")
	require.NoError(t, err)

	lockB, err := driver.NewLocker(ctx, "shared-name")
	require.NoError(t, err)

	require.NoError(t, lockA.Lock(ctx))

	err = lockB.TryLock(ctx)
	require.ErrorIs(t, err, locker.ErrLocked)

	require.NoError(t, lockA.Unlock(ctx))

	require.NoError(t, lockB.Lock(ctx))
	require.NoError(t, lockB.Unlock(ctx))
}

func TestDummyLocker_TwoLockers_DifferentNames_DoNotContend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := dummy.New()

	lockA, err := driver.NewLocker(ctx, "name-a")
	require.NoError(t, err)

	lockB, err := driver.NewLocker(ctx, "name-b")
	require.NoError(t, err)

	require.NoError(t, lockA.Lock(ctx))

	lockDone := make(chan error, 1)

	go func() {
		lockDone <- lockB.Lock(ctx)
	}()

	select {
	case err := <-lockDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("lockB.Lock timed out even though it uses a different lock name")
	}

	require.NoError(t, lockA.Unlock(ctx))
	require.NoError(t, lockB.Unlock(ctx))
}

func TestDummyLocker_Done_NeverLocked_IsClosed(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "done-never")

	select {
	case <-lock.Done():
	default:
		t.Fatal("Done() on a never-locked locker must be already closed")
	}
}

func TestDummyLocker_Done_WhileHeld_IsOpen(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "done-held")
	require.NoError(t, lock.Lock(context.Background()))

	t.Cleanup(func() { _ = lock.Unlock(context.Background()) })

	select {
	case <-lock.Done():
		t.Fatal("Done() must not be closed while the lock is held")
	default:
	}
}

func TestDummyLocker_Done_ClosedAfterUnlock(t *testing.T) {
	t.Parallel()

	lock := newLocker(t, "done-unlock")
	require.NoError(t, lock.Lock(context.Background()))

	done := lock.Done()
	require.NoError(t, lock.Unlock(context.Background()))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Done() channel must be closed after Unlock")
	}
}

func TestDummyLocker_LifeCtxCancellation_AbortsBlockingLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := dummy.New()

	holderCtx := context.Background()
	lockA, err := driver.NewLocker(holderCtx, "life-ctx-test")
	require.NoError(t, err)
	require.NoError(t, lockA.Lock(holderCtx))

	lifeCtx, cancelLife := context.WithCancel(ctx)
	lockB, err := driver.NewLocker(lifeCtx, "life-ctx-test")
	require.NoError(t, err)

	var (
		waiters sync.WaitGroup
		lockErr error
	)

	waiters.Go(func() {
		lockErr = lockB.Lock(context.Background())
	})

	time.Sleep(30 * time.Millisecond)

	cancelLife()

	done := make(chan struct{})

	go func() {
		waiters.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Lock did not abort after lifetime context cancellation")
	}

	require.Error(t, lockErr)

	require.NoError(t, lockA.Unlock(holderCtx))
}
