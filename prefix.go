package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/locker"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	txPkg "github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

// ErrPrefixTrailingSlash is returned by Prefixed when the prefix ends with
// "/". The codec namer prepends "/" to every key, so a trailing slash here
// would produce keys like "/foo//objectLocation/name".
var ErrPrefixTrailingSlash = errors.New("storage.Prefixed: prefix must not end with '/'")

// ErrPrefixNoLeadingSlash is returned by Prefixed when a non-empty prefix does
// not start with "/". Keys are rooted at "/", so a prefix must be an absolute
// path segment.
var ErrPrefixNoLeadingSlash = errors.New("storage.Prefixed: prefix must start with '/'")

// Prefixed returns a Storage that scopes every operation, predicate, range,
// and watch under prefix. Keys returned to the caller (RequestResponse.Values,
// kv.KeyValue, watch.Event.Prefix) have prefix stripped — callers never see
// absolute keys.
//
// Composition: Prefixed("/a", Prefixed("/b", base)) ≡ Prefixed("/a/b", base).
// Nested wrappers are flattened at construction so the outer prefix is the
// leftmost segment.
//
// An empty prefix yields a transparent wrapper. A non-empty prefix must start
// with "/" (returns ErrPrefixNoLeadingSlash) and must not end with "/" (returns
// ErrPrefixTrailingSlash) — the codec namer prepends "/" to every key, so a
// trailing slash here would produce keys like "/foo//objectLocation/name".
// Interior "/" separators are allowed (e.g. "/foo/bar").
func Prefixed(prefix string, inner Storage) (Storage, error) {
	if prefix != "" {
		if !strings.HasPrefix(prefix, "/") {
			return nil, fmt.Errorf("%w: %q", ErrPrefixNoLeadingSlash, prefix)
		}

		if strings.HasSuffix(prefix, "/") {
			return nil, fmt.Errorf("%w: %q", ErrPrefixTrailingSlash, prefix)
		}
	}

	if existing, ok := inner.(*prefixed); ok {
		merged := make([]byte, 0, len(prefix)+len(existing.prefix))

		merged = append(merged, prefix...)
		merged = append(merged, existing.prefix...)

		return &prefixed{
			prefix: merged,
			inner:  existing.inner,
		}, nil
	}

	return &prefixed{
		prefix: []byte(prefix),
		inner:  inner,
	}, nil
}

type prefixed struct {
	prefix []byte
	inner  Storage
}

var _ Storage = (*prefixed)(nil)

func (p *prefixed) Watch(ctx context.Context, key []byte, opts ...watch.Option) <-chan watch.Event {
	absKey := concatKey(p.prefix, key)
	innerCh := p.inner.Watch(ctx, absKey, opts...)

	out := make(chan watch.Event, cap(innerCh))

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-innerCh:
				if !ok {
					return
				}

				event.Prefix = stripPrefix(p.prefix, event.Prefix)

				select {
				case <-ctx.Done():
					return
				case out <- event:
				}
			}
		}
	}()

	return out
}

func (p *prefixed) Tx(ctx context.Context) txPkg.Tx {
	return &prefixedTx{
		inner:  p.inner.Tx(ctx),
		prefix: p.prefix,
	}
}

func (p *prefixed) TxFactory() txPkg.Factory {
	return p.Tx
}

// NewLocker scopes the lock name under the wrapper's prefix so that locks
// created through different Prefixed wrappers cannot collide. The prefix is
// concatenated to name with no separator, matching the key-space convention
// used by Tx and Range — Prefixed("/ns", inner).NewLocker(ctx, "/lock") asks
// the inner Storage for a lock named "/ns/lock".
func (p *prefixed) NewLocker(ctx context.Context, name string, opts ...locker.Option) (locker.Locker, error) {
	lock, err := p.inner.NewLocker(ctx, string(p.prefix)+name, opts...)
	if err != nil {
		return nil, fmt.Errorf("new-locker: %w", err)
	}

	return lock, nil
}

// LockerFactory returns a locker.Factory bound to this Prefixed wrapper so the
// prefix-scoping logic in NewLocker is preserved for callers that only need a
// lock binder.
func (p *prefixed) LockerFactory() locker.Factory {
	return p
}

func (p *prefixed) Range(ctx context.Context, opts ...RangeOption) ([]kv.KeyValue, error) {
	rangeOpts := &rangeOptions{Prefix: "", Limit: 0}
	for _, opt := range opts {
		opt(rangeOpts)
	}

	var newOpts []RangeOption

	if rangeOpts.Prefix != "" {
		newOpts = append(newOpts, WithPrefix(string(p.prefix)+rangeOpts.Prefix))
	}

	if rangeOpts.Limit != 0 {
		newOpts = append(newOpts, WithLimit(rangeOpts.Limit))
	}

	kvs, err := p.inner.Range(ctx, newOpts...)
	if err != nil {
		return nil, fmt.Errorf("range: %w", err)
	}

	for i := range kvs {
		kvs[i].Key = stripPrefix(p.prefix, kvs[i].Key)
	}

	return kvs, nil
}

type prefixedTx struct {
	inner  txPkg.Tx
	prefix []byte
}

func (t *prefixedTx) If(predicates ...predicate.Predicate) txPkg.Tx {
	rewritten := make([]predicate.Predicate, len(predicates))
	for i, pred := range predicates {
		rewritten[i] = rewritePredicate(t.prefix, pred)
	}

	t.inner.If(rewritten...)

	return t
}

func (t *prefixedTx) Then(operations ...operation.Operation) txPkg.Tx {
	rewritten := make([]operation.Operation, len(operations))
	for i, op := range operations {
		rewritten[i] = rewriteOperation(t.prefix, op)
	}

	t.inner.Then(rewritten...)

	return t
}

func (t *prefixedTx) Else(operations ...operation.Operation) txPkg.Tx {
	rewritten := make([]operation.Operation, len(operations))
	for i, op := range operations {
		rewritten[i] = rewriteOperation(t.prefix, op)
	}

	t.inner.Else(rewritten...)

	return t
}

func (t *prefixedTx) Commit() (txPkg.Response, error) {
	resp, err := t.inner.Commit()
	if err != nil {
		return resp, fmt.Errorf("tx commit: %w", err)
	}

	for i := range resp.Results {
		for j := range resp.Results[i].Values {
			resp.Results[i].Values[j].Key = stripPrefix(t.prefix, resp.Results[i].Values[j].Key)
		}
	}

	return resp, nil
}

func rewriteOperation(prefix []byte, oper operation.Operation) operation.Operation {
	newKey := concatKey(prefix, oper.Key())

	switch oper.Type() {
	case operation.TypeGet:
		return operation.Get(newKey, oper.Options()...)
	case operation.TypePut:
		return operation.Put(newKey, oper.Value(), oper.Options()...)
	case operation.TypeDelete:
		return operation.Delete(newKey, oper.Options()...)
	}

	panic(fmt.Sprintf("storage.Prefixed: unknown operation type %s", oper.Type()))
}

func rewritePredicate(prefix []byte, pred predicate.Predicate) predicate.Predicate {
	newKey := concatKey(prefix, pred.Key())
	predOp := pred.Operation()
	target := pred.Target()
	val := pred.Value()

	switch target {
	case predicate.TargetValue:
		switch predOp {
		case predicate.OpEqual:
			return predicate.ValueEqual(newKey, val)
		case predicate.OpNotEqual:
			return predicate.ValueNotEqual(newKey, val)
		case predicate.OpGreater, predicate.OpLess:
			// Greater/Less comparisons are not supported for value targets.
		}
	case predicate.TargetVersion:
		version, ok := val.(int64)
		if !ok {
			panic(fmt.Sprintf("storage.Prefixed: predicate value for version target must be int64, got %T", val))
		}

		switch predOp {
		case predicate.OpEqual:
			return predicate.VersionEqual(newKey, version)
		case predicate.OpNotEqual:
			return predicate.VersionNotEqual(newKey, version)
		case predicate.OpGreater:
			return predicate.VersionGreater(newKey, version)
		case predicate.OpLess:
			return predicate.VersionLess(newKey, version)
		}
	}

	panic(fmt.Sprintf("storage.Prefixed: unsupported predicate target=%s op=%s", target, predOp))
}

func concatKey(prefix, key []byte) []byte {
	if len(prefix) == 0 {
		return key
	}

	out := make([]byte, len(prefix)+len(key))
	copy(out, prefix)
	copy(out[len(prefix):], key)

	return out
}

func stripPrefix(prefix, key []byte) []byte {
	if bytes.HasPrefix(key, prefix) {
		return key[len(prefix):]
	}

	return key
}
