package locker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/driver/dummy"
	"github.com/tarantool/go-storage/v2/locker"
)

func dummyFactory(t *testing.T) locker.Factory {
	t.Helper()

	return dummy.New()
}

func TestDo_HappyPath(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var ran bool

	err := locker.Do(ctx, dummyFactory(t), "/locks/do-happy", func(_ context.Context) error {
		ran = true

		return nil
	})
	require.NoError(t, err)
	assert.True(t, ran)

	// After Do returns, the lock is released — a fresh Lock must succeed.
	lock, err := dummyFactory(t).NewLocker(ctx, "/locks/do-happy")
	require.NoError(t, err)

	lockCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	require.NoError(t, lock.Lock(lockCtx))
	assert.Equal(t, "/locks/do-happy", lock.Key())

	doneOpen := true

	select {
	case <-lock.Done():
		doneOpen = false
	default:
	}

	assert.True(t, doneOpen)
	require.NoError(t, lock.Unlock(ctx))
}

func TestDo_FnErrorPropagates_LockReleased(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	factory := dummyFactory(t)

	sentinel := errors.New("domain failure")

	err := locker.Do(ctx, factory, "/locks/do-fn-err", func(_ context.Context) error {
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	// Lock must be released even though fn failed.
	lock, err := factory.NewLocker(ctx, "/locks/do-fn-err")
	require.NoError(t, err)

	lockCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	require.NoError(t, lock.Lock(lockCtx))
	require.NoError(t, lock.Unlock(ctx))
}

func TestDo_FactoryError_FnNotCalled(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	factoryErr := errors.New("create failed")

	var called bool

	factory := locker.FactoryFunc(func(_ context.Context, _ string, _ ...locker.Option) (locker.Locker, error) {
		return nil, factoryErr
	})

	err := locker.Do(ctx, factory, "/locks/do-create-err", func(_ context.Context) error {
		called = true

		return nil
	})
	require.ErrorIs(t, err, factoryErr)
	assert.False(t, called, "fn must not be called when factory fails")
}

// lockErrorLocker is a Locker whose Lock fails — used to drive Do down the
// "Lock returned error" branch with a predictable error.
type lockErrorLocker struct {
	err error
}

func (l *lockErrorLocker) Lock(_ context.Context) error    { return l.err }
func (l *lockErrorLocker) TryLock(_ context.Context) error { return l.err }
func (l *lockErrorLocker) Unlock(_ context.Context) error  { return nil }
func (l *lockErrorLocker) Key() string                     { return "" }
func (l *lockErrorLocker) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}

func TestDo_LockError_FnNotCalled(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	lockErr := errors.New("lock failed")

	var called bool

	factory := locker.FactoryFunc(func(_ context.Context, _ string, _ ...locker.Option) (locker.Locker, error) {
		return &lockErrorLocker{err: lockErr}, nil
	})

	err := locker.Do(ctx, factory, "/locks/do-lock-err", func(_ context.Context) error {
		called = true

		return nil
	})
	require.ErrorIs(t, err, lockErr)
	assert.False(t, called, "fn must not be called when Lock fails")
}

func TestDo_CtxCancelDuringFn_UnlockStillRuns(t *testing.T) {
	t.Parallel()

	parentCtx := t.Context()
	factory := dummyFactory(t)

	fnCtx, cancel := context.WithCancel(parentCtx)

	err := locker.Do(fnCtx, factory, "/locks/do-cancel", func(ctx context.Context) error {
		cancel()

		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)

	// Lock must be released despite ctx cancellation.
	lock, err := factory.NewLocker(parentCtx, "/locks/do-cancel")
	require.NoError(t, err)

	lockCtx, lockCancel := context.WithTimeout(parentCtx, time.Second)
	defer lockCancel()

	require.NoError(t, lock.Lock(lockCtx))
	require.NoError(t, lock.Unlock(parentCtx))
}
