package locker

import (
	"context"
	"errors"
	"fmt"
)

// Do creates a Locker via f, acquires it with Lock, runs fn while the lock
// is held, then releases the lock. Returns whatever fn returns, joined with
// any error from creation, Lock, or Unlock via errors.Join — fn's result is
// the leading error so callers can errors.Is against domain errors without
// having to peel off lock-machinery errors.
//
// The same ctx is used for create, Lock, fn, and Unlock. Callers that need
// a separate lifetime context (where session keepalive outlives the work
// context) should build the lock manually.
//
//nolint:varnamelen // fn is the canonical name for the user-provided critical section.
func Do(ctx context.Context, f Factory, name string, fn func(context.Context) error, opts ...Option) error {
	lock, err := f.NewLocker(ctx, name, opts...)
	if err != nil {
		return fmt.Errorf("locker.Do: create: %w", err)
	}

	err = lock.Lock(ctx)
	if err != nil {
		return fmt.Errorf("locker.Do: lock: %w", err)
	}

	fnErr := fn(ctx)
	unlockErr := lock.Unlock(ctx)

	if fnErr != nil && unlockErr != nil {
		return errors.Join(fnErr, fmt.Errorf("locker.Do: unlock: %w", unlockErr))
	}

	if fnErr != nil {
		return fnErr
	}

	if unlockErr != nil {
		return fmt.Errorf("locker.Do: unlock: %w", unlockErr)
	}

	return nil
}
