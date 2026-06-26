package tcs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v3"

	"github.com/tarantool/go-storage/v2/locker"
)

var (
	errInvalidLockerName = errors.New("tcs locker: name must start with '/' and must not end with '/'")

	// ErrUnsupportedFeatures is returned by NewLocker when the TCS instance does
	// not advertise both the ttl and keepalive features required by the locker.
	ErrUnsupportedFeatures = errors.New("tcs locker: server does not support ttl+keepalive — schema upgrade required")
)

// tcsLocker implements "smallest mod_revision wins" locking over the prefix
// `name + "/"`. Each attempt writes a unique ephemeral key with a TTL renewed
// at ttl/3 cadence, which assumes replication_synchro_timeout + RTT < ttl/3
// on a healthy cluster.
type tcsLocker struct {
	conn   DoerWatcher
	name   string
	ttlSec int
	//nolint:containedctx // lifeCtx scopes the locker; cancellation aborts a blocking Lock.
	lifeCtx context.Context

	mu          sync.Mutex
	held        bool
	myKey       string
	myRev       int64
	stopKA      chan struct{}
	expired     chan struct{}
	expiredOnce *sync.Once
}

//nolint:gochecknoglobals // shared pre-closed channel returned when no acquire has happened.
var closedDone = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}()

var _ locker.Locker = (*tcsLocker)(nil)

// NewLocker requires name to start with '/' and not end with '/' (e.g.
// "/my/lock"). Cancel ctx to stop the keepalive goroutine.
//
// Returns ErrUnsupportedFeatures if the TCS server does not advertise
// features.ttl and features.keepalive.
func (d *Driver) NewLocker(ctx context.Context, name string, opts ...locker.Option) (locker.Locker, error) {
	if !strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return nil, errInvalidLockerName
	}

	hasTTL, hasKeepalive, err := infoFeatures(ctx, d.conn)
	if err != nil {
		return nil, fmt.Errorf("tcs locker: NewLocker: %w", err)
	}

	if !hasTTL || !hasKeepalive {
		return nil, ErrUnsupportedFeatures
	}

	options := locker.ApplyOptions(opts)

	ttlSec := max(int(options.TTL.Seconds()), 1)

	return &tcsLocker{ //nolint:exhaustruct // mutable state fields are zero-initialized by design.
		conn:    d.conn,
		name:    name,
		ttlSec:  ttlSec,
		lifeCtx: ctx,
	}, nil
}

func (l *tcsLocker) Lock(ctx context.Context) error {
	l.mu.Lock()
	if l.held {
		l.mu.Unlock()
		return nil
	}

	l.mu.Unlock()

	myKey := l.name + "/" + uuid.NewString()

	myRev, err := putWithTTL(ctx, l.conn, myKey, "", l.ttlSec)
	if err != nil {
		return fmt.Errorf("tcs locker: lock %s: %w", l.name, err)
	}

	stopKA := make(chan struct{})
	expired := make(chan struct{})
	expiredOnce := &sync.Once{}

	go l.keepaliveLoop(myKey, stopKA, expired, expiredOnce)

	cleanupOnFail := func() { //nolint:contextcheck // cleanup must run even when ctx is already canceled.
		close(stopKA)

		_ = deletePath(context.Background(), l.conn, myKey)
	}

	for {
		entries, err := getPrefix(ctx, l.conn, l.name+"/")
		if err != nil {
			cleanupOnFail()
			return fmt.Errorf("tcs locker: lock %s: list: %w", l.name, err)
		}

		if isSmallestRev(entries, myRev) {
			l.mu.Lock()
			l.held = true
			l.myKey = myKey
			l.myRev = myRev
			l.stopKA = stopKA
			l.expired = expired
			l.expiredOnce = expiredOnce
			l.mu.Unlock()

			return nil
		}

		watchKey := predecessorKey(entries, myRev, l.name+"/")

		notified := make(chan struct{}, 1)

		watcher, werr := l.conn.NewWatcher("config.storage:"+watchKey, func(_ tarantool.WatchEvent) {
			select {
			case notified <- struct{}{}:
			default:
			}
		})
		if werr != nil {
			cleanupOnFail()
			return fmt.Errorf("tcs locker: lock %s: watch: %w", l.name, werr)
		}

		select {
		case <-notified:
		case <-ctx.Done():
			watcher.Unregister()
			cleanupOnFail()

			return fmt.Errorf("tcs locker: lock %s: %w", l.name, ctx.Err())
		case <-l.lifeCtx.Done():
			watcher.Unregister()
			cleanupOnFail()

			return fmt.Errorf("tcs locker: lock %s: lifetime context: %w", l.name, l.lifeCtx.Err())
		case <-expired:
			watcher.Unregister()
			// TTL expiry already removed our key — no cleanup needed.
			return fmt.Errorf("tcs locker: lock %s: %w", l.name, locker.ErrSessionExpired)
		}

		watcher.Unregister()
	}
}

func (l *tcsLocker) TryLock(ctx context.Context) error {
	l.mu.Lock()
	if l.held {
		l.mu.Unlock()
		return nil
	}

	l.mu.Unlock()

	myKey := l.name + "/" + uuid.NewString()

	myRev, err := putWithTTL(ctx, l.conn, myKey, "", l.ttlSec)
	if err != nil {
		return fmt.Errorf("tcs locker: try-lock %s: %w", l.name, err)
	}

	entries, err := getPrefix(ctx, l.conn, l.name+"/")
	if err != nil {
		//nolint:contextcheck // cleanup must run even when ctx is already canceled.
		_ = deletePath(context.Background(), l.conn, myKey)
		return fmt.Errorf("tcs locker: try-lock %s: list: %w", l.name, err)
	}

	if !isSmallestRev(entries, myRev) {
		//nolint:contextcheck // cleanup must run even when ctx is already canceled.
		_ = deletePath(context.Background(), l.conn, myKey)

		return locker.ErrLocked
	}

	stopKA := make(chan struct{})
	expired := make(chan struct{})
	expiredOnce := &sync.Once{}

	go l.keepaliveLoop(myKey, stopKA, expired, expiredOnce)

	l.mu.Lock()
	l.held = true
	l.myKey = myKey
	l.myRev = myRev
	l.stopKA = stopKA
	l.expired = expired
	l.expiredOnce = expiredOnce
	l.mu.Unlock()

	return nil
}

func (l *tcsLocker) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held {
		return locker.ErrLockReleased
	}

	close(l.stopKA)
	l.expiredOnce.Do(func() { close(l.expired) })

	myKey := l.myKey

	l.held = false
	l.myKey = ""
	l.myRev = 0
	l.stopKA = nil

	err := deletePath(ctx, l.conn, myKey)
	if err != nil {
		return fmt.Errorf("tcs locker: unlock %s: %w", l.name, err)
	}

	return nil
}

func (l *tcsLocker) Done() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.expired == nil {
		return closedDone
	}

	return l.expired
}

func (l *tcsLocker) Key() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.myKey
}

// keepaliveDivisor sets keepalive cadence to ttl/keepaliveDivisor; the divisor
// must satisfy replication_synchro_timeout + RTT < ttl/keepaliveDivisor.
const keepaliveDivisor = 3

func (l *tcsLocker) keepaliveLoop(myKey string, stop <-chan struct{}, expired chan struct{}, once *sync.Once) {
	interval := max(time.Duration(l.ttlSec)*time.Second/keepaliveDivisor, time.Second)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-l.lifeCtx.Done():
			return
		case <-ticker.C:
			err := keepalivePath(l.lifeCtx, l.conn, myKey, l.ttlSec)
			if err != nil {
				once.Do(func() { close(expired) })
				return
			}
		}
	}
}

func isSmallestRev(entries []lockEntry, myRev int64) bool {
	if len(entries) == 0 {
		return true
	}

	for _, e := range entries {
		if e.ModRevision < myRev {
			return false
		}
	}

	return true
}

// predecessorKey returns the entry with the largest mod_revision strictly less
// than myRev, or prefix when none exists (so the watcher fires on any change).
func predecessorKey(entries []lockEntry, myRev int64, prefix string) string {
	sorted := make([]lockEntry, len(entries))
	copy(sorted, entries)
	slices.SortFunc(sorted, func(a, b lockEntry) int {
		switch {
		case a.ModRevision < b.ModRevision:
			return -1
		case a.ModRevision > b.ModRevision:
			return 1
		default:
			return 0
		}
	})

	predecessor := ""

	for _, e := range sorted {
		if e.ModRevision < myRev {
			predecessor = e.Path
		}
	}

	if predecessor == "" {
		return prefix
	}

	return predecessor
}
