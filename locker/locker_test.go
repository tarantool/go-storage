// Package locker provides the Locker interface and shared types for distributed lock drivers.
package locker_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/locker"
)

func TestApplyOptions_DefaultTTL(t *testing.T) {
	t.Parallel()

	opts := locker.ApplyOptions(nil)
	assert.Equal(t, locker.DefaultTTL, opts.TTL)
}

func TestApplyOptions_Override(t *testing.T) {
	t.Parallel()

	custom := 30 * time.Second
	opts := locker.ApplyOptions([]locker.Option{locker.WithTTL(custom)})
	assert.Equal(t, custom, opts.TTL)
}

func TestApplyOptions_EmptySlice(t *testing.T) {
	t.Parallel()

	opts := locker.ApplyOptions([]locker.Option{})
	assert.Equal(t, locker.DefaultTTL, opts.TTL)
}

func TestApplyOptions_MultipleOverrides_LastWins(t *testing.T) {
	t.Parallel()

	first := 10 * time.Second
	second := 45 * time.Second
	opts := locker.ApplyOptions([]locker.Option{locker.WithTTL(first), locker.WithTTL(second)})
	assert.Equal(t, second, opts.TTL)
}

func TestSentinelErrors_Identity(t *testing.T) {
	t.Parallel()

	errs := []error{
		locker.ErrLocked,
		locker.ErrSessionExpired,
		locker.ErrLockReleased,
		locker.ErrUnsupported,
	}

	for i := range errs {
		for j := i + 1; j < len(errs); j++ {
			require.NotEqual(t, errs[i], errs[j],
				"sentinel errors at index %d and %d must differ", i, j)
		}
	}
}

func TestSentinelErrors_Messages(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "locker: held by another session", locker.ErrLocked.Error())
	assert.Equal(t, "locker: session expired", locker.ErrSessionExpired.Error())
	assert.Equal(t, "locker: lock already released", locker.ErrLockReleased.Error())
	assert.Equal(t, "locker: not supported by this driver instance", locker.ErrUnsupported.Error())
}

func TestDefaultTTL_Value(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 60*time.Second, locker.DefaultTTL)
}
