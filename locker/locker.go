// Package locker provides the Locker interface and shared types for distributed lock drivers.
package locker

import (
	"context"
	"errors"
	"time"
)

const DefaultTTL = 60 * time.Second

var (
	ErrLocked         = errors.New("locker: held by another session")
	ErrSessionExpired = errors.New("locker: session expired")
	ErrLockReleased   = errors.New("locker: lock already released")
	ErrUnsupported    = errors.New("locker: not supported by this driver instance")
)

// Locker acquires and releases a named distributed lock. A single Locker
// instance holds the lock at most once at a time: re-Lock on an already-held
// Locker is a no-op that returns nil. Unlock on a never-locked or
// already-released Locker returns ErrLockReleased; implementations never panic
// and never delete a foreign key.
type Locker interface {
	Lock(ctx context.Context) error
	TryLock(ctx context.Context) error
	Unlock(ctx context.Context) error
	Key() string
}

// Factory creates new Lockers bound to a storage instance.
//
// It is the lightest "create a lock" surface — bind once via
// Storage.LockerFactory and pass the resulting Factory around to components
// that only need to acquire locks, instead of handing out the full Storage.
// A Storage's NewLocker method satisfies this signature directly, so a
// Factory can also be obtained as a method value: var f locker.Factory = s.NewLocker.
type Factory func(ctx context.Context, name string, opts ...Option) (Locker, error)

// Options holds configuration for a Locker instance.
type Options struct {
	// TTL bounds how long the backend will hold the lock without a renewal.
	// A zero value means no TTL.
	TTL time.Duration
}

type Option func(*Options)

func WithTTL(d time.Duration) Option {
	return func(o *Options) {
		o.TTL = d
	}
}

func ApplyOptions(opts []Option) Options {
	out := Options{
		TTL: DefaultTTL,
	}
	for _, fn := range opts {
		fn(&out)
	}

	return out
}
