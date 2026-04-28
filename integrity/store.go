package integrity

import (
	"context"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/internal/options"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/watch"
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

// Bind creates a new Store[T] by binding c to the given storage. Bind is a
// cheap struct literal — no caching, no validation.
func (c *Codec[T]) Bind(s storage.Storage) *Store[T] {
	return &Store[T]{codec: c, storage: s}
}

// Get retrieves and validates a single named value from storage.
func (s *Store[T]) Get(
	ctx context.Context,
	name string,
	opts ...options.OptionCallback[getOptions],
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
	vOpts ...options.OptionCallback[putOptions],
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
	vOpts ...options.OptionCallback[deleteOptions],
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
	opts ...options.OptionCallback[getOptions],
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
func (s *Store[T]) Watch(ctx context.Context, name string) (<-chan watch.Event, error) {
	if !checkRangeName(name) {
		return storeClosedChan, ErrInvalidName
	}

	key := s.codec.namer.Prefix(name, strings.HasSuffix(name, "/"))

	rawCh := s.storage.Watch(ctx, []byte(key))
	filteredCh := make(chan watch.Event)

	go func() {
		defer close(filteredCh)

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-rawCh:
				if !ok {
					return
				}

				parsedKey, err := s.codec.namer.ParseKey(string(event.Prefix))
				if err != nil {
					// Drop events on keys this codec does not own (e.g., a
					// neighbour codec writing under the same storage prefix).
					continue
				}

				if !strings.HasPrefix(parsedKey.Name(), name) {
					continue
				}

				select {
				case <-ctx.Done():
					return
				case filteredCh <- event:
				}
			}
		}
	}()

	return filteredCh, nil
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
