// Package locker provides the Locker interface and shared types for distributed lock drivers.
package locker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const DefaultTTL = 60 * time.Second

var (
	ErrLocked         = errors.New("locker: held by another session")
	ErrSessionExpired = errors.New("locker: session expired")
	ErrLockReleased   = errors.New("locker: lock already released")
	ErrUnsupported    = errors.New("locker: not supported by this driver instance")

	// ErrPrefixTrailingSlash is returned by Prefixed when prefix ends with "/".
	ErrPrefixTrailingSlash = errors.New("locker: prefix must not end with '/'")

	// ErrPrefixNoLeadingSlash is returned by Prefixed when a non-empty prefix
	// does not start with "/".
	ErrPrefixNoLeadingSlash = errors.New("locker: prefix must start with '/'")

	// ErrNameNoLeadingSlash is returned by a driver's NewLocker when name does
	// not start with "/".
	ErrNameNoLeadingSlash = errors.New("locker: name must start with '/'")

	// ErrNameTrailingSlash is returned by a driver's NewLocker when name ends
	// with "/".
	ErrNameTrailingSlash = errors.New("locker: name must not end with '/'")
)

// ValidateName checks that a lock name follows the shared contract: it must
// start with "/" and must not end with "/". Every driver calls it at the top of
// NewLocker so the lock-name contract is uniform across backends.
func ValidateName(name string) error {
	if !strings.HasPrefix(name, "/") {
		return fmt.Errorf("%w: %q", ErrNameNoLeadingSlash, name)
	}

	if strings.HasSuffix(name, "/") {
		return fmt.Errorf("%w: %q", ErrNameTrailingSlash, name)
	}

	return nil
}

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

	// Done returns a channel that is closed when this Locker is no longer
	// holding the lock — either because Unlock was called or because the
	// backend's session was lost (TTL elapsed without renewal, connection
	// dropped, etc.). The channel returned by Done corresponds to the most
	// recent successful Lock/TryLock call; calling Done before any successful
	// acquire returns an already-closed channel. The signal does not
	// distinguish voluntary release from involuntary loss; callers that need
	// to tell them apart must track that themselves.
	Done() <-chan struct{}
}

// Factory creates new Lockers bound to a storage instance.
//
// It is the lightest "create a lock" surface — bind once via
// Storage.LockerFactory and pass the resulting Factory around to components
// that only need to acquire locks, instead of handing out the full Storage.
//
// Any type with a NewLocker method matching this signature satisfies Factory,
// so Storage and Prefixed are already Factory values — no adapter needed for
// production wiring. FactoryFunc adapts a bare function for ad-hoc cases.
type Factory interface {
	NewLocker(ctx context.Context, name string, opts ...Option) (Locker, error)
}

// FactoryFunc adapts a bare function to the Factory interface, mirroring the
// http.HandlerFunc pattern. Use it when you don't have a type with a NewLocker
// method handy — e.g. in tests.
type FactoryFunc func(ctx context.Context, name string, opts ...Option) (Locker, error)

// NewLocker satisfies Factory by invoking f.
func (f FactoryFunc) NewLocker(ctx context.Context, name string, opts ...Option) (Locker, error) {
	return f(ctx, name, opts...)
}

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
