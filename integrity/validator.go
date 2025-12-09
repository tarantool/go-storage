package integrity

import (
	"fmt"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

// Validator verifies integrity-protected key-value pairs.
type Validator[T any] struct {
	namer      *namer.DefaultNamer
	marshaller marshaller.TypedYamlMarshaller[T]
	hashers    map[string]hasher.Hasher
	verifiers  map[string]crypto.Verifier
}

// NewValidator creates a new Validator instance.
func NewValidator[T any](
	namer *namer.DefaultNamer,
	marshaller marshaller.TypedYamlMarshaller[T],
	hashers []hasher.Hasher,
	verifiers []crypto.Verifier,
) Validator[T] {
	hasherMap := make(map[string]hasher.Hasher)
	for _, h := range hashers {
		hasherMap[h.Name()] = h
	}

	verifierMap := make(map[string]crypto.Verifier)
	for _, v := range verifiers {
		verifierMap[v.Name()] = v
	}

	return Validator[T]{
		namer:      namer,
		marshaller: marshaller,
		hashers:    hasherMap,
		verifiers:  verifierMap,
	}
}

func zero[T any]() T {
	var out T
	return out
}

// Validate verifies integrity-protected key-value pairs and returns the validated value.
func (v Validator[T]) Validate(kvs []kv.KeyValue) (T, error) {
	if len(kvs) == 0 {
		return zero[T](), fmt.Errorf("%w", ErrNoKeyValuePairs)
	}

	kvMap, keyStrings := v.buildKVMaps(kvs)

	results, err := v.namer.ParseKeys(keyStrings, false)
	if err != nil {
		return zero[T](), fmt.Errorf("failed to parse keys: %w", err)
	}

	if results.Len() != 1 {
		return zero[T](), fmt.Errorf("%w: got %d", ErrMultipleObjects, results.Len())
	}

	objectName := v.getObjectName(results)

	expectedKeys, err := v.namer.GenerateNames(objectName)
	if err != nil {
		return zero[T](), fmt.Errorf("failed to generate expected keys: %w", err)
	}

	valueData, err := v.validateExpectedKeys(expectedKeys, kvMap)
	if err != nil {
		return zero[T](), err
	}

	err = v.validateHashesAndSignatures(expectedKeys, kvMap, valueData)
	if err != nil {
		return zero[T](), err
	}

	value, err := v.marshaller.Unmarshal(valueData)
	if err != nil {
		return zero[T](), fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return value, nil
}

func (v Validator[T]) buildKVMaps(kvs []kv.KeyValue) (map[string]kv.KeyValue, []string) {
	keyStrings := make([]string, len(kvs))
	kvMap := make(map[string]kv.KeyValue)

	for i, kv := range kvs {
		keyStr := string(kv.Key)

		keyStrings[i] = keyStr
		kvMap[keyStr] = kv
	}

	return kvMap, keyStrings
}

func (v Validator[T]) getObjectName(results namer.Results) string {
	for name := range results.Result() {
		return name
	}

	return ""
}

func (v Validator[T]) validateExpectedKeys(expectedKeys []namer.Key, kvMap map[string]kv.KeyValue) ([]byte, error) {
	var valueData []byte

	for _, key := range expectedKeys {
		keyStr := key.Build()
		kvItem, exists := kvMap[keyStr]

		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrMissingExpectedKey, keyStr)
		}

		if key.Type() == namer.KeyTypeValue {
			valueData = kvItem.Value
		}
	}

	if valueData == nil {
		return nil, fmt.Errorf("%w", ErrNoValueData)
	}

	return valueData, nil
}

func (v Validator[T]) validateHashesAndSignatures(
	expectedKeys []namer.Key,
	kvMap map[string]kv.KeyValue,
	valueData []byte,
) error {
	for _, key := range expectedKeys {
		keyStr := key.Build()
		kvItem := kvMap[keyStr]

		switch key.Type() {
		case namer.KeyTypeHash:
			err := v.validateHash(key, kvItem, valueData)
			if err != nil {
				return err
			}

		case namer.KeyTypeSignature:
			err := v.validateSignature(key, kvItem, valueData)
			if err != nil {
				return err
			}

		case namer.KeyTypeValue:
			// Already handled.
		default:
			return fmt.Errorf("%w: %v", ErrUnknownKeyType, key.Type())
		}
	}

	return nil
}

func (v Validator[T]) validateHash(key namer.Key, kvItem kv.KeyValue, valueData []byte) error {
	hasher, exists := v.hashers[key.Property()]
	if !exists {
		return fmt.Errorf("%w: %s", ErrHasherNotFound, key.Property())
	}

	expectedHash, err := hasher.Hash(valueData)
	if err != nil {
		return fmt.Errorf("failed to compute hash: %w", err)
	}

	if string(expectedHash) != string(kvItem.Value) {
		return fmt.Errorf("%w for hasher '%s'", ErrHashMismatch, key.Property())
	}

	return nil
}

func (v Validator[T]) validateSignature(key namer.Key, kvItem kv.KeyValue, valueData []byte) error {
	verifier, exists := v.verifiers[key.Property()]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVerifierNotFound, key.Property())
	}

	err := verifier.Verify(valueData, kvItem.Value)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSignatureFailed, err)
	}

	return nil
}
