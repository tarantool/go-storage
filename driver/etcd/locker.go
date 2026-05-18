package etcd

import (
	"context"
	"errors"
	"fmt"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/tarantool/go-storage/locker"
)

type etcdLocker struct {
	session *concurrency.Session
	mu      *concurrency.Mutex
	name    string

	stateMu sync.Mutex
	held    bool
}

var _ locker.Locker = (*etcdLocker)(nil)

// NewLocker returns locker.ErrUnsupported when the Driver was built with a
// Client that is not a concrete *etcd.Client — the concurrency package needs
// the concrete type which the Client interface does not expose.
func (d Driver) NewLocker(ctx context.Context, name string, opts ...locker.Option) (locker.Locker, error) {
	concrete, ok := d.client.(*etcd.Client)
	if !ok {
		return nil, locker.ErrUnsupported
	}

	options := locker.ApplyOptions(opts)

	session, err := concurrency.NewSession(
		concrete,
		concurrency.WithTTL(int(options.TTL.Seconds())),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("etcd locker: create session: %w", err)
	}

	mu := concurrency.NewMutex(session, name)

	return &etcdLocker{ //nolint:exhaustruct // stateMu and held are zero-initialized by design.
		session: session,
		mu:      mu,
		name:    name,
	}, nil
}

func (l *etcdLocker) Lock(ctx context.Context) error {
	l.stateMu.Lock()
	if l.held {
		l.stateMu.Unlock()
		return nil
	}

	l.stateMu.Unlock()

	err := l.mu.Lock(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			return locker.ErrLocked
		}

		return fmt.Errorf("etcd locker: lock: %w", err)
	}

	l.stateMu.Lock()
	l.held = true
	l.stateMu.Unlock()

	return nil
}

func (l *etcdLocker) TryLock(ctx context.Context) error {
	l.stateMu.Lock()
	if l.held {
		l.stateMu.Unlock()
		return nil
	}

	l.stateMu.Unlock()

	err := l.mu.TryLock(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			return locker.ErrLocked
		}

		return fmt.Errorf("etcd locker: try-lock: %w", err)
	}

	l.stateMu.Lock()
	l.held = true
	l.stateMu.Unlock()

	return nil
}

func (l *etcdLocker) Unlock(ctx context.Context) error {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()

	if !l.held {
		return locker.ErrLockReleased
	}

	err := l.mu.Unlock(ctx)
	if err != nil {
		return fmt.Errorf("etcd locker: unlock: %w", err)
	}

	// Flip held before session.Close so a subsequent Unlock returns
	// ErrLockReleased even if Close fails.
	l.held = false

	err = l.session.Close()
	if err != nil {
		return fmt.Errorf("etcd locker: close session: %w", err)
	}

	return nil
}

func (l *etcdLocker) Key() string {
	return l.mu.Key()
}
