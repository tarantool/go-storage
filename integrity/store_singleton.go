package integrity

import (
	"context"

	"github.com/tarantool/go-storage/internal/options"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/watch"
)

// SingletonStore[T] is a Store[T] bound to one fixed name. It is intended for
// configuration-style objects that live at a single, known key (e.g.
// "/settings/auth") rather than under a directory of "/settings/auth/<id>"
// items. The name is supplied once at bind time and threaded through every
// operation, so call sites no longer carry the noisy identifier.
//
// Construct one via Codec[T].BindSingleton. The wire layout is identical to
// what Store[T] produces for the same name — no separate codec, no separate
// namer.
type SingletonStore[T any] struct {
	store *Store[T]
	name  string
}

// Get reads the singleton's value and verifies its hashes/signatures.
func (s *SingletonStore[T]) Get(
	ctx context.Context,
	opts ...options.OptionCallback[getOptions],
) (ValidatedResult[T], error) {
	return s.store.Get(ctx, s.name, opts...)
}

// Put writes value under the singleton's bound name with integrity protection.
// WithPutPredicates(...) makes the write conditional; ErrPredicateFailed is
// returned if any predicate fails.
func (s *SingletonStore[T]) Put(
	ctx context.Context,
	value T,
	opts ...options.OptionCallback[putOptions],
) error {
	return s.store.Put(ctx, s.name, value, opts...)
}

// Delete removes the singleton's value, hash, and signature keys.
// WithDeletePredicates(...) makes the delete conditional; ErrPredicateFailed
// is returned if any predicate fails.
func (s *SingletonStore[T]) Delete(
	ctx context.Context,
	opts ...options.OptionCallback[deleteOptions],
) error {
	return s.store.Delete(ctx, s.name, opts...)
}

// Watch returns a channel that receives events when the singleton's
// value-layer key changes. Hash and signature key changes are not surfaced —
// consumers re-fetch on signal, which re-runs verification.
func (s *SingletonStore[T]) Watch(ctx context.Context) (<-chan watch.Event, error) {
	return s.store.Watch(ctx, s.name)
}

// TxGet enqueues a Get for the singleton onto branch b, returning a future
// whose Result() is populated after Commit. Mirrors Codec[T].TxGet without
// the name parameter — the bound name is used.
func (s *SingletonStore[T]) TxGet(
	b Branchable,
	opts ...options.OptionCallback[getOptions],
) *GetFuture[T] {
	return s.store.codec.TxGet(b, s.name, opts...)
}

// TxPut enqueues a Put for the singleton onto branch b. Mirrors
// Codec[T].TxPut without the name parameter.
func (s *SingletonStore[T]) TxPut(b Branchable, value T) error {
	return s.store.codec.TxPut(b, s.name, value)
}

// TxDelete enqueues a Delete for the singleton onto branch b. Mirrors
// Codec[T].TxDelete without the name parameter.
func (s *SingletonStore[T]) TxDelete(
	b Branchable,
	opts ...options.OptionCallback[deleteOptions],
) error {
	return s.store.codec.TxDelete(b, s.name, opts...)
}

// BindPredicate resolves the singleton's value-layer key and applies p to it,
// returning a concrete predicate.Predicate ready for use in Tx.If.
func (s *SingletonStore[T]) BindPredicate(pred Predicate) (predicate.Predicate, error) {
	return s.store.codec.BindPredicate(s.name, pred)
}
