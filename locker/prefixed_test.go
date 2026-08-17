package locker_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storage "github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/driver/dummy"
	"github.com/tarantool/go-storage/v2/locker"
)

func TestPrefixed_RejectsTrailingSlash(t *testing.T) {
	t.Parallel()

	inner := storage.NewStorage(dummy.New()).LockerFactory()

	cases := []string{"/", "/ns/", "/a/b/"}
	for _, prefix := range cases {
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()

			f, err := locker.Prefixed(prefix, inner)
			assert.Nil(t, f)
			require.ErrorIs(t, err, locker.ErrPrefixTrailingSlash)
		})
	}
}

func TestPrefixed_RejectsMissingLeadingSlash(t *testing.T) {
	t.Parallel()

	inner := storage.NewStorage(dummy.New()).LockerFactory()

	// Note: "ns/" triggers the leading-slash check first (per implementation
	// order), matching storage.Prefixed's behavior.
	cases := []string{"ns", "a/b", "ns/"}
	for _, prefix := range cases {
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()

			f, err := locker.Prefixed(prefix, inner)
			assert.Nil(t, f)
			require.ErrorIs(t, err, locker.ErrPrefixNoLeadingSlash)
		})
	}
}

func TestPrefixed_NameRewriting(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	inner := storage.NewStorage(dummy.New()).LockerFactory()

	scoped, err := locker.Prefixed("/ns", inner)
	require.NoError(t, err)

	lock, err := scoped.NewLocker(ctx, "/lock")
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	t.Cleanup(func() { _ = lock.Unlock(ctx) })

	assert.Equal(t, "/ns/lock", lock.Key(),
		"Prefixed must concatenate prefix to caller name with no separator")
}

func TestPrefixed_EmptyPrefixPassesThrough(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	inner := storage.NewStorage(dummy.New()).LockerFactory()

	pass, err := locker.Prefixed("", inner)
	require.NoError(t, err)

	lock, err := pass.NewLocker(ctx, "/caller-name")
	require.NoError(t, err)

	require.NoError(t, lock.Lock(ctx))
	t.Cleanup(func() { _ = lock.Unlock(ctx) })

	assert.Equal(t, "/caller-name", lock.Key(),
		"empty prefix must not alter the caller's name")
}

func TestPrefixed_NamespaceIsolation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	inner := storage.NewStorage(dummy.New()).LockerFactory()

	vaultA, err := locker.Prefixed("/vault-a", inner)
	require.NoError(t, err)

	vaultB, err := locker.Prefixed("/vault-b", inner)
	require.NoError(t, err)

	lockA, err := vaultA.NewLocker(ctx, "/shared")
	require.NoError(t, err)

	lockB, err := vaultB.NewLocker(ctx, "/shared")
	require.NoError(t, err)

	require.NoError(t, lockA.TryLock(ctx),
		"acquiring /vault-a/shared must not contend with anything")
	require.NoError(t, lockB.TryLock(ctx),
		"acquiring /vault-b/shared must succeed concurrently because the namespaces differ")

	assert.Equal(t, "/vault-a/shared", lockA.Key())
	assert.Equal(t, "/vault-b/shared", lockB.Key())

	require.NoError(t, lockA.Unlock(ctx))
	require.NoError(t, lockB.Unlock(ctx))
}

func TestPrefixed_NestedComposition(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	inner := storage.NewStorage(dummy.New()).LockerFactory()

	// Nested wrappers are flattened at construction so the outer prefix is
	// the leftmost segment, mirroring storage.Prefixed. Prefixed("/a",
	// Prefixed("/b", inner)) is equivalent to Prefixed("/a/b", inner).
	innerB, err := locker.Prefixed("/b", inner)
	require.NoError(t, err)

	composed, err := locker.Prefixed("/a", innerB)
	require.NoError(t, err)

	flat, err := locker.Prefixed("/a/b", inner)
	require.NoError(t, err)

	lockComposed, err := composed.NewLocker(ctx, "/lock-nested")
	require.NoError(t, err)

	lockFlat, err := flat.NewLocker(ctx, "/lock-flat")
	require.NoError(t, err)

	require.NoError(t, lockComposed.Lock(ctx))
	t.Cleanup(func() { _ = lockComposed.Unlock(ctx) })

	require.NoError(t, lockFlat.Lock(ctx))
	t.Cleanup(func() { _ = lockFlat.Unlock(ctx) })

	assert.Equal(t, "/a/b/lock-nested", lockComposed.Key())
	assert.Equal(t, "/a/b/lock-flat", lockFlat.Key())
}

func TestPrefixed_InnerErrorPropagated(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	stub := locker.FactoryFunc(func(_ context.Context, _ string, _ ...locker.Option) (locker.Locker, error) {
		return nil, locker.ErrUnsupported
	})

	scoped, err := locker.Prefixed("/ns", stub)
	require.NoError(t, err)

	lock, err := scoped.NewLocker(ctx, "/lock")
	assert.Nil(t, lock)
	require.ErrorIs(t, err, locker.ErrUnsupported)
}

func TestPrefixed_CapturesNamePassedToInner(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var capturedName string

	stub := locker.FactoryFunc(func(_ context.Context, name string, _ ...locker.Option) (locker.Locker, error) {
		capturedName = name
		return nil, nil //nolint:nilnil // test stub: caller only inspects capturedName.
	})

	scoped, err := locker.Prefixed("/ns", stub)
	require.NoError(t, err)

	_, _ = scoped.NewLocker(ctx, "/lock")

	assert.Equal(t, "/ns/lock", capturedName)
}
