package dummy

import (
	"context"
	"fmt"
	"sync"

	"github.com/tarantool/go-storage/v2/locker"
)

type dummyLockEntry struct {
	mu sync.Mutex
}

func (d *Driver) lockEntryFor(name string) *dummyLockEntry {
	v, _ := d.lockRegistry.LoadOrStore(name, &dummyLockEntry{}) //nolint:exhaustruct
	entry, _ := v.(*dummyLockEntry)

	return entry
}

type dummyLocker struct {
	entry *dummyLockEntry
	//nolint:containedctx // lifeCtx scopes the locker; cancellation aborts a blocking Lock.
	lifeCtx context.Context
	name    string

	mu   sync.Mutex
	held bool
	key  string
	done chan struct{}
}

//nolint:gochecknoglobals // shared pre-closed channel returned when no acquire has happened.
var closedDone = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}()

var _ locker.Locker = (*dummyLocker)(nil)

func (d *Driver) NewLocker(ctx context.Context, name string, opts ...locker.Option) (locker.Locker, error) {
	_ = locker.ApplyOptions(opts)

	return &dummyLocker{ //nolint:exhaustruct // mu/held/key are zero-initialized by design.
		entry:   d.lockEntryFor(name),
		lifeCtx: ctx,
		name:    name,
	}, nil
}

func (dl *dummyLocker) Lock(ctx context.Context) error {
	dl.mu.Lock()
	if dl.held {
		dl.mu.Unlock()
		return nil
	}

	dl.mu.Unlock()

	// sync.Mutex.Lock is not cancellable; run it in a goroutine and select on
	// whichever context fires first.
	acquired := make(chan struct{})

	go func() {
		dl.entry.mu.Lock()
		close(acquired)
	}()

	select {
	case <-acquired:
		dl.mu.Lock()
		dl.held = true
		dl.key = dl.name
		dl.done = make(chan struct{})
		dl.mu.Unlock()

		return nil

	case <-ctx.Done():
		go func() {
			<-acquired
			dl.entry.mu.Unlock()
		}()

		return fmt.Errorf("dummy locker: lock: %w", ctx.Err())

	case <-dl.lifeCtx.Done():
		go func() {
			<-acquired
			dl.entry.mu.Unlock()
		}()

		return fmt.Errorf("dummy locker: lock: %w", dl.lifeCtx.Err())
	}
}

func (dl *dummyLocker) TryLock(ctx context.Context) error {
	dl.mu.Lock()
	if dl.held {
		dl.mu.Unlock()
		return nil
	}

	dl.mu.Unlock()

	if !dl.entry.mu.TryLock() {
		return locker.ErrLocked
	}

	dl.mu.Lock()
	dl.held = true
	dl.key = dl.name
	dl.done = make(chan struct{})
	dl.mu.Unlock()

	return nil
}

func (dl *dummyLocker) Unlock(_ context.Context) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if !dl.held {
		return locker.ErrLockReleased
	}

	dl.held = false
	dl.key = ""
	close(dl.done)
	dl.entry.mu.Unlock()

	return nil
}

func (dl *dummyLocker) Done() <-chan struct{} {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.done == nil {
		return closedDone
	}

	return dl.done
}

func (dl *dummyLocker) Key() string {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	return dl.key
}
