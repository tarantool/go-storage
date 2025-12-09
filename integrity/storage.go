package integrity

import (
	"context"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/namer"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/watch"
)

// NamedValue represents a named value with its associated name.
type NamedValue[T any] struct {
	Name  string
	Value T
}

// Typed provides integrity-protected storage operations for typed values.
type Typed[T any] struct {
	base  storage.Storage
	gen   Generator[T]
	val   Validator[T]
	namer *namer.DefaultNamer
}

func checkName(name string) bool {
	return len(name) == 0 || strings.Contains(name, "/")
}

// Get retrieves and validates a named value from storage.
func (t *Typed[T]) Get(ctx context.Context, name string) (NamedValue[T], error) {
	if !checkName(name) {
		return NamedValue[T]{}, fmt.Errorf("%w", ErrInvalidName)
	}

	keys, err := t.namer.GenerateNames(name)
	if err != nil {
		return NamedValue[T]{}, fmt.Errorf("%w: failed to generate keys", err)
	}

	ops := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, operation.Get([]byte(key.Build())))
	}

	response, err := t.base.Tx(ctx).Then(ops...).Commit()
	if err != nil {
		return NamedValue[T]{}, fmt.Errorf("%w: failed to execute", err)
	}

	var kvs []kv.KeyValue
	for _, r := range response.Results {
		kvs = append(kvs, r.Values...)
	}

	out, err := t.val.Validate(kvs)
	if err != nil {
		return NamedValue[T]{}, fmt.Errorf("%w: failed to validate", err)
	}

	return NamedValue[T]{Name: name, Value: out}, nil
}

// Put stores a named value with integrity protection.
func (t *Typed[T]) Put(name string, val T) error {
	if !checkName(name) {
		return fmt.Errorf("%w", ErrInvalidName)
	}

	kvs, err := t.gen.Generate(name, val)
	if err != nil {
		return fmt.Errorf("%w: failed to generate", err)
	}

	ops := make([]operation.Operation, 0, len(kvs))
	for _, kv := range kvs {
		ops = append(ops, operation.Put(kv.Key, kv.Value))
	}

	response, err := t.base.Tx(context.Background()).Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("%w: failed to execute", err)
	}

	_ = response

	return nil
}

// Delete removes a named value and its integrity data from storage.
func (t *Typed[T]) Delete(name string) error {
	if !checkName(name) {
		return fmt.Errorf("%w", ErrInvalidName)
	}

	keys, err := t.namer.GenerateNames(name)
	if err != nil {
		return fmt.Errorf("%w: failed to generate keys", err)
	}

	ops := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, operation.Delete([]byte(key.Build())))
	}

	response, err := t.base.Tx(context.Background()).Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("%w: failed to execute", err)
	}

	_ = response

	return nil
}

// Range retrieves and validates all values under the given name prefix.
func (t *Typed[T]) Range(ctx context.Context, name string) ([]NamedValue[T], error) {
	if !checkName(name) {
		return nil, fmt.Errorf("%w", ErrInvalidName)
	}

	// Create a prefix operation to get all keys with the given name prefix.
	prefix := name
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// Use the storage.Range method to get all keys with this prefix.
	kvs, err := t.base.Range(ctx, storage.WithPrefix(prefix))
	if err != nil {
		return nil, fmt.Errorf("%w: failed to execute range", err)
	}

	// Group key-value pairs by object name.
	kvGroups := make(map[string][]kv.KeyValue)
	for _, item := range kvs {
		// Extract object name from the key by removing the prefix and type info.
		keyStr := string(item.Key)
		if strings.HasPrefix(keyStr, prefix) {
			// Remove the prefix to get the object name.
			objectName := name
			if existing, exists := kvGroups[objectName]; exists {
				kvGroups[objectName] = append(existing, item)
			} else {
				kvGroups[objectName] = []kv.KeyValue{item}
			}
		}
	}

	// Validate each group and create NamedValue results.
	results := make([]NamedValue[T], 0, len(kvGroups))
	for objectName, groupKvs := range kvGroups {
		value, err := t.val.Validate(groupKvs)
		if err != nil {
			continue // Skip invalid entries.
		}

		results = append(results, NamedValue[T]{Name: objectName, Value: value})
	}

	return results, nil
}

// Watch returns a channel for watching changes to values under the given name prefix.
func (t *Typed[T]) Watch(ctx context.Context, name string) <-chan watch.Event {
	if !checkName(name) {
		return closedChan()
	}

	prefix := name
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return t.base.Watch(ctx, []byte(prefix))
}

func closedChan() <-chan watch.Event {
	ch := make(chan watch.Event)
	close(ch)

	return ch
}
