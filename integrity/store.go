package integrity

import (
	"context"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/internal/options"
	"github.com/tarantool/go-storage/v2/predicate"
	"github.com/tarantool/go-storage/v2/watch"
)

// Store[T] is a Codec[T] bound to a concrete storage.Storage. Each method is
// a thin wrapper that builds a *Tx, enqueues one op via the codec, commits,
// and returns the future's result — observably indistinguishable from a
// single-op Tx.Commit. Store[T] holds nothing beyond the codec and storage
// handle, so binding the same codec to different storages is cheap.
type Store[T any] struct {
	codec   *Codec[T]
	storage storage.Storage
}

// Get retrieves and validates a single named value from storage.
func (s *Store[T]) Get(
	ctx context.Context,
	name string,
	opts ...GetOption,
) (ValidatedResult[T], error) {
	tx := NewTx(s.storage)
	fut := s.codec.TxGet(tx, name, opts...)

	_, err := tx.Commit(ctx)
	if err != nil {
		return ValidatedResult[T]{}, err
	}

	return fut.Result()
}

// Put stores a named value with integrity protection. Accepts
// WithPutPredicates(...) to add conditional predicates; if any predicate
// fails, ErrPredicateFailed is returned.
func (s *Store[T]) Put(
	ctx context.Context,
	name string,
	value T,
	vOpts ...PutOption,
) error {
	opts := options.ApplyOptions[putOptions](nil, vOpts)

	txn := NewTx(s.storage)

	if len(opts.Predicates) > 0 {
		boundPreds, err := s.bindPredicates(name, opts.Predicates)
		if err != nil {
			return err
		}

		txn.If(boundPreds...)
	}

	err := s.codec.TxPut(txn, name, value)
	if err != nil {
		return err
	}

	resp, err := txn.Commit(ctx)
	if err != nil {
		return err
	}

	if len(opts.Predicates) > 0 && !resp.Succeeded {
		return ErrPredicateFailed
	}

	return nil
}

// Delete removes a named value with integrity protection. Accepts
// WithDeletePredicates(...) and WithPrefix(); if any predicate fails,
// ErrPredicateFailed is returned.
func (s *Store[T]) Delete(
	ctx context.Context,
	name string,
	vOpts ...DeleteOption,
) error {
	opts := options.ApplyOptions[deleteOptions](nil, vOpts)

	txn := NewTx(s.storage)

	if len(opts.Predicates) > 0 {
		boundPreds, err := s.bindPredicates(name, opts.Predicates)
		if err != nil {
			return err
		}

		txn.If(boundPreds...)
	}

	err := s.codec.TxDelete(txn, name, vOpts...)
	if err != nil {
		return err
	}

	resp, err := txn.Commit(ctx)
	if err != nil {
		return err
	}

	if len(opts.Predicates) > 0 && !resp.Succeeded {
		return ErrPredicateFailed
	}

	return nil
}

// Range retrieves and validates all values under the given name prefix.
func (s *Store[T]) Range(
	ctx context.Context,
	name string,
	opts ...GetOption,
) ([]ValidatedResult[T], error) {
	tx := NewTx(s.storage)
	fut := s.codec.TxRange(tx, name, opts...)

	_, err := tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return fut.Result()
}

// Watch returns a channel that receives events for values under the given
// name prefix. Watch is not transactional and bypasses Tx; it calls
// s.storage.Watch directly.
//
// Under the signal-only Event.Prefix contract every event carries the watched
// prefix verbatim (driver-stripped of its trailing "/"), so filtering on
// event.Key is a no-op — callers must Range/Get to learn what changed.
func (s *Store[T]) Watch(ctx context.Context, name string) (<-chan watch.Event, error) {
	if !checkRangeName(name) {
		return storeClosedChan, ErrInvalidName
	}

	key := s.codec.namer.Prefix(name, strings.HasSuffix(name, "/"))

	return s.storage.Watch(ctx, []byte(key)), nil
}

// ValueKey returns the on-disk value-layer key for name — the namer's
// value key prefixed with the bound storage's prefix (if it is wrapped
// with [storage.Prefixed]). See [Codec.ValueKey] for the namer-relative
// form.
func (s *Store[T]) ValueKey(name string) (string, error) {
	key, err := s.codec.ValueKey(name)
	if err != nil {
		return "", err
	}

	if prefix := storage.StoragePrefix(s.storage); len(prefix) > 0 {
		return string(prefix) + key, nil
	}

	return key, nil
}

// FullKeys returns every on-disk key the bound codec would produce for
// name (value + hashes + signatures), each prefixed with the bound
// storage's prefix if it is wrapped with [storage.Prefixed]. See
// [Codec.FullKeys] for the namer-relative form.
func (s *Store[T]) FullKeys(name string) ([]string, error) {
	keys, err := s.codec.FullKeys(name)
	if err != nil {
		return nil, err
	}

	prefix := storage.StoragePrefix(s.storage)
	if len(prefix) == 0 {
		return keys, nil
	}

	for i, key := range keys {
		keys[i] = string(prefix) + key
	}

	return keys, nil
}

func (s *Store[T]) bindPredicates(name string, preds []Predicate) ([]predicate.Predicate, error) {
	boundPreds := make([]predicate.Predicate, 0, len(preds))

	for _, p := range preds {
		bound, err := s.codec.BindPredicate(name, p)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to bind predicate", err)
		}

		boundPreds = append(boundPreds, bound)
	}

	return boundPreds, nil
}

var (
	storeClosedChan = make(chan watch.Event) //nolint:gochecknoglobals
)

func init() { //nolint:gochecknoinits
	close(storeClosedChan)
}
