// Package integrity provides typed storage with built-in data integrity protection.
// It automatically computes and verifies hashes and signatures for stored values.
//
// See [TypedBuilder] for configuration options and [Typed] for available operations.
package integrity

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/internal/options"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/namer"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

// Typed provides integrity-protected storage operations for typed values.
type Typed[T any] struct {
	base  storage.Storage
	gen   Generator[T]
	val   Validator[T]
	namer namer.Namer
}

func checkName(name string) bool {
	switch {
	case len(name) == 0:
		return false
	case strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/"):
		return false
	default:
		return true
	}
}

func checkPrefix(prefix string) bool {
	switch {
	case len(prefix) == 0:
		return false
	case strings.HasPrefix(prefix, "/") && len(prefix) > 1:
		return false
	case !strings.HasSuffix(prefix, "/"):
		return false
	default:
		return true
	}
}

func checkRangeName(name string) bool {
	return !strings.HasPrefix(name, "/")
}

type Predicate func(key []byte) predicate.Predicate

// ValueEqual creates a predicate that checks if a key's value equals the specified value.
func (t *Typed[T]) ValueEqual(value T) (Predicate, error) {
	return t.valuePredicate(value, predicate.ValueEqual)
}

// ValueNotEqual creates a predicate that checks if a key's value is not equal to the specified value.
func (t *Typed[T]) ValueNotEqual(value T) (Predicate, error) {
	return t.valuePredicate(value, predicate.ValueNotEqual)
}

// VersionEqual creates a predicate that checks if a key's version equals the specified version.
func (t *Typed[T]) VersionEqual(value int64) Predicate {
	return t.versionPredicate(value, predicate.VersionEqual)
}

// VersionNotEqual creates a predicate that checks if a key's version is not equal to the specified version.
func (t *Typed[T]) VersionNotEqual(value int64) Predicate {
	return t.versionPredicate(value, predicate.VersionNotEqual)
}

// VersionGreater creates a predicate that checks if a key's version is greater than the specified version.
func (t *Typed[T]) VersionGreater(value int64) Predicate {
	return t.versionPredicate(value, predicate.VersionGreater)
}

// VersionLess creates a predicate that checks if a key's version is less than the specified version.
func (t *Typed[T]) VersionLess(value int64) Predicate {
	return t.versionPredicate(value, predicate.VersionLess)
}

// valuePredicate uses for predicate that need a marshaller.
func (t *Typed[T]) valuePredicate(value T,
	predFunc func(key []byte, value any) predicate.Predicate) (Predicate, error) {
	mValue, err := t.gen.marshaller.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to marshal predicate value", err)
	}

	return func(key []byte) predicate.Predicate {
		return predFunc(key, mValue)
	}, nil
}

// versionPredicate uses for predicate with int64 type value,
// so they don't need an error to be returned.
func (t *Typed[T]) versionPredicate(
	value int64,
	predFunc func(key []byte, value int64) predicate.Predicate) Predicate {
	return func(key []byte) predicate.Predicate { return predFunc(key, value) }
}

type getOptions struct {
	ignoreVerificationError bool
	ignoreMoreThanOneResult bool
}

type putOptions struct {
	Predicates []Predicate
}

type deleteOptions struct {
	withPrefix bool
	Predicates []Predicate
}

// WithPutPredicates configures predicates for conditional Put operations.
// The Put operation will only succeed if all predicates evaluate to true.
// If predicates are specified but fail, [ErrPredicateFailed] is returned.
func WithPutPredicates(predicates ...Predicate) options.OptionCallback[putOptions] {
	return func(opts *putOptions) {
		opts.Predicates = append(opts.Predicates, predicates...)
	}
}

// WithDeletePredicates configures predicates for conditional Delete operations.
// The Delete operation will only succeed if all predicates evaluate to true.
// If predicates are specified but fail, [ErrPredicateFailed] is returned.
func WithDeletePredicates(predicates ...Predicate) options.OptionCallback[deleteOptions] {
	return func(opts *deleteOptions) {
		opts.Predicates = append(opts.Predicates, predicates...)
	}
}

// WithPrefix configures the ability to delete keys by a prefix.
func WithPrefix() options.OptionCallback[deleteOptions] {
	return func(opts *deleteOptions) {
		opts.withPrefix = true
	}
}

// IgnoreVerificationError returns an option that allows Get and Range operations
// to return results even if hash or signature verification fails.
// The returned result will still contain the Error field with verification details.
func IgnoreVerificationError() options.OptionCallback[getOptions] {
	return func(opts *getOptions) {
		opts.ignoreVerificationError = true
	}
}

// IgnoreMoreThanOneResult returns an option that allows Get operation
// to succeed when multiple results are returned for a single name.
// By default, Get returns ErrMoreThanOneResult in such cases.
func IgnoreMoreThanOneResult() options.OptionCallback[getOptions] {
	return func(opts *getOptions) {
		opts.ignoreMoreThanOneResult = true
	}
}

var (
	ErrNotFound                  = errors.New("not found")
	ErrMoreThanOneResult         = errors.New("more than one result was returned")
	ErrInvalidPredicateValueType = errors.New("invalid predicate value type")
	ErrNoValueKey                = errors.New("no value key found in generated keys")
	// ErrPredicateFailed is returned by Put or Delete when predicates are specified
	// but the transaction predicate check fails (i.e., the conditions are not met).
	// Use [WithPutPredicates] or [WithDeletePredicates] to specify predicates.
	ErrPredicateFailed = errors.New("predicate check failed")
)

func flattenResults(response tx.Response) []kv.KeyValue {
	kvs := make([]kv.KeyValue, 0, len(response.Results))
	for _, r := range response.Results {
		kvs = append(kvs, r.Values...)
	}

	return kvs
}

// Get retrieves and validates a single named value from storage.
func (t *Typed[T]) Get(
	ctx context.Context,
	name string,
	vOpts ...options.OptionCallback[getOptions],
) (ValidatedResult[T], error) {
	if !checkName(name) {
		return ValidatedResult[T]{}, ErrInvalidName
	}

	opts := options.ApplyOptions[getOptions](nil, vOpts)

	keys, err := t.namer.GenerateNames(name)
	if err != nil {
		return ValidatedResult[T]{}, fmt.Errorf("%w: failed to generate keys", err)
	}

	ops := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, operation.Get([]byte(key.Build())))
	}

	response, err := t.base.Tx(ctx).Then(ops...).Commit()
	if err != nil {
		return ValidatedResult[T]{}, fmt.Errorf("%w: failed to execute", err)
	}

	kvs := flattenResults(response)

	results, err := t.val.Validate(kvs)
	switch {
	case err != nil:
		return ValidatedResult[T]{}, fmt.Errorf("%w: failed to validate", err)
	case len(results) == 0:
		return ValidatedResult[T]{}, ErrNotFound
	case len(results) > 1 && !opts.ignoreMoreThanOneResult:
		return ValidatedResult[T]{}, ErrMoreThanOneResult
	}

	if results[0].Error != nil {
		// results[0].Value.IsZero() means we've failed to decode results.
		failedToDecodeResult := results[0].Value.IsZero()

		if opts.ignoreVerificationError && !failedToDecodeResult {
			return results[0], nil
		}

		return ValidatedResult[T]{}, results[0].Error
	}

	if results[0].Value.IsZero() {
		panic("unreachable")
	}

	return results[0], nil
}

// Put stores a named value with integrity protection.
// Use [WithPutPredicates] to specify conditions that must be met for the operation to succeed.
// If predicates are specified but fail, [ErrPredicateFailed] is returned.
func (t *Typed[T]) Put(ctx context.Context, name string, val T, vOpts ...options.OptionCallback[putOptions]) error {
	if !checkName(name) {
		return ErrInvalidName
	}

	kvs, err := t.gen.Generate(name, val)
	if err != nil {
		return fmt.Errorf("%w: failed to generate", err)
	}

	opts := options.ApplyOptions[putOptions](nil, vOpts)

	var predicates []predicate.Predicate

	if len(opts.Predicates) > 0 {
		var vKey []byte

		for _, kValue := range kvs {
			key, err := t.namer.ParseKey(string(kValue.Key))
			if err != nil {
				return fmt.Errorf("%w: failed to parse key", err)
			}

			if key.Type() == namer.KeyTypeValue {
				vKey = kValue.Key
				break
			}
		}

		if vKey == nil {
			return ErrNoValueKey
		}

		predicates = make([]predicate.Predicate, 0, len(opts.Predicates))
		for _, p := range opts.Predicates {
			predicates = append(predicates, p(vKey))
		}
	}

	ops := make([]operation.Operation, 0, len(kvs))
	for _, kv := range kvs {
		ops = append(ops, operation.Put(kv.Key, kv.Value))
	}

	txn := t.base.Tx(ctx)
	if len(predicates) > 0 {
		txn = txn.If(predicates...)
	}

	resp, err := txn.Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("%w: failed to execute", err)
	}

	if len(predicates) > 0 && !resp.Succeeded {
		return ErrPredicateFailed
	}

	return nil
}

// Delete removes a named value with integrity protection.
// Use [WithPrefix] to delete all values under a prefix.
// Use [WithDeletePredicates] to specify conditions that must be met for the operation to succeed.
// If predicates are specified but fail, [ErrPredicateFailed] is returned.
func (t *Typed[T]) Delete(ctx context.Context, name string, vOpts ...options.OptionCallback[deleteOptions]) error {
	opts := options.ApplyOptions[deleteOptions](nil, vOpts)

	if !opts.withPrefix && !checkName(name) {
		return ErrInvalidName
	}

	if opts.withPrefix && !checkPrefix(name) {
		return ErrInvalidName
	}

	keys, err := t.namer.GenerateNames(name)
	if err != nil {
		return fmt.Errorf("%w: failed to generate keys", err)
	}

	var predicates []predicate.Predicate

	if len(opts.Predicates) > 0 {
		var vKey []byte

		for _, key := range keys {
			if key.Type() == namer.KeyTypeValue {
				vKey = []byte(key.Build())
				break
			}
		}

		if vKey == nil {
			return ErrNoValueKey
		}

		predicates = make([]predicate.Predicate, 0, len(opts.Predicates))
		for _, p := range opts.Predicates {
			predicates = append(predicates, p(vKey))
		}
	}

	ops := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, operation.Delete([]byte(key.Build())))
	}

	txn := t.base.Tx(ctx)
	if len(predicates) > 0 {
		txn = txn.If(predicates...)
	}

	resp, err := txn.Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("%w: failed to execute", err)
	}

	if len(predicates) > 0 && !resp.Succeeded {
		return ErrPredicateFailed
	}

	return nil
}

// Range retrieves and validates all values under the given name prefix.
func (t *Typed[T]) Range(
	ctx context.Context,
	name string,
	vOpts ...options.OptionCallback[getOptions],
) ([]ValidatedResult[T], error) {
	if !checkRangeName(name) {
		return nil, ErrInvalidName
	}

	opts := options.ApplyOptions[getOptions](nil, vOpts)

	var ops []operation.Operation

	switch name {
	case "":
		prefix := t.namer.Prefix(name, true)

		ops = []operation.Operation{operation.Get([]byte(prefix))}
	default:
		keys, err := t.namer.GenerateNames(name)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to generate keys", err)
		}

		ops = make([]operation.Operation, 0, len(keys))
		for _, key := range keys {
			// GenerateNames makes names without suffix "/", so, due to
			// our need in directory we add "/".
			ops = append(ops, operation.Get([]byte(key.Build()+"/")))
		}
	}

	response, err := t.base.Tx(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to execute range", err)
	}

	kvs := flattenResults(response)

	results, err := t.val.Validate(kvs)
	if err != nil {
		return nil, err
	}

	var out []ValidatedResult[T]

	for _, result := range results {
		if result.Error == nil {
			out = append(out, result)
			continue
		}

		// result.Value.IsZero() means we've failed to decode results, skipping.
		failedToDecodeResult := result.Value.IsZero()

		if opts.ignoreVerificationError && !failedToDecodeResult {
			out = append(out, result)
		}
	}

	return out, nil
}

// Watch returns a channel for watching changes to values under the given name prefix.
func (t *Typed[T]) Watch(ctx context.Context, name string) (<-chan watch.Event, error) {
	if !checkRangeName(name) {
		return closedChan, ErrInvalidName
	}

	key := t.namer.Prefix(name, strings.HasSuffix(name, "/"))

	rawCh := t.base.Watch(ctx, []byte(key))
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

				// Parse the key from the event prefix.
				key, err := t.namer.ParseKey(string(event.Prefix))
				if err != nil {
					// Skip events for non-integrity keys.
					continue
				}

				// Filter by name prefix.
				if !strings.HasPrefix(key.Name(), name) {
					continue
				}

				// Forward the event.
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

var (
	closedChan = make(chan watch.Event) //nolint:gochecknoglobals
)

func init() { //nolint:gochecknoinits
	close(closedChan)
}
