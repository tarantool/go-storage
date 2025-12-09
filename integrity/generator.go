package integrity

import (
	"errors"
	"fmt"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

var (
	// ErrHasherNotFound is returned when a required hasher is not configured.
	ErrHasherNotFound = errors.New("hasher not found")
	// ErrSignerNotFound is returned when a required signer is not configured.
	ErrSignerNotFound = errors.New("signer not found")
	// ErrUnknownKeyType is returned for unexpected key types.
	ErrUnknownKeyType = errors.New("unknown key type")
	// ErrInvalidName is returned for invalid object names.
	ErrInvalidName = errors.New("invalid name")
	// ErrNoKeyValuePairs is returned when no key-value pairs are provided.
	ErrNoKeyValuePairs = errors.New("no key-value pairs provided")
	// ErrMultipleObjects is returned when multiple objects are found instead of one.
	ErrMultipleObjects = errors.New("expected exactly one object")
	// ErrMissingExpectedKey is returned when an expected key is missing.
	ErrMissingExpectedKey = errors.New("missing expected key")
	// ErrNoValueData is returned when no value data is found.
	ErrNoValueData = errors.New("no value data found")
	// ErrHashMismatch is returned when a hash doesn't match the expected value.
	ErrHashMismatch = errors.New("hash mismatch")
	// ErrVerifierNotFound is returned when a required verifier is not configured.
	ErrVerifierNotFound = errors.New("verifier not found")
	// ErrSignatureFailed is returned when signature verification fails.
	ErrSignatureFailed = errors.New("signature verification failed")
)

// Generator creates integrity-protected key-value pairs for storage.
type Generator[T any] struct {
	namer      *namer.DefaultNamer
	marshaller marshaller.TypedYamlMarshaller[T]
	hashers    map[string]hasher.Hasher
	signers    map[string]crypto.Signer
}

// NewGenerator creates a new Generator instance.
func NewGenerator[T any](
	namer *namer.DefaultNamer,
	marshaller marshaller.TypedYamlMarshaller[T],
	hashers []hasher.Hasher,
	signers []crypto.Signer,
) Generator[T] {
	hasherMap := make(map[string]hasher.Hasher)
	for _, h := range hashers {
		hasherMap[h.Name()] = h
	}

	signerMap := make(map[string]crypto.Signer)
	for _, s := range signers {
		signerMap[s.Name()] = s
	}

	return Generator[T]{
		namer:      namer,
		marshaller: marshaller,
		hashers:    hasherMap,
		signers:    signerMap,
	}
}

// Generate creates integrity-protected key-value pairs for the given object.
func (g Generator[T]) Generate(name string, value T) ([]kv.KeyValue, error) {
	keys, err := g.namer.GenerateNames(name)
	if err != nil {
		return nil, fmt.Errorf("failed to generate keys: %w", err)
	}

	marshalledValue, err := g.marshaller.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %w", err)
	}

	results := make([]kv.KeyValue, 0, len(keys))

	for _, key := range keys {
		var valueData []byte

		switch key.Type() {
		case namer.KeyTypeValue:
			valueData = marshalledValue

		case namer.KeyTypeHash:
			hasherInstance, exists := g.hashers[key.Property()]
			if !exists {
				return nil, fmt.Errorf("%w: %s", ErrHasherNotFound, key.Property())
			}

			var err error

			valueData, err = hasherInstance.Hash(marshalledValue)
			if err != nil {
				return nil, fmt.Errorf("failed to compute hash: %w", err)
			}

		case namer.KeyTypeSignature:
			signer, exists := g.signers[key.Property()]
			if !exists {
				return nil, fmt.Errorf("%w: %s", ErrSignerNotFound, key.Property())
			}

			var err error

			valueData, err = signer.Sign(marshalledValue)
			if err != nil {
				return nil, fmt.Errorf("failed to generate signature: %w", err)
			}

		default:
			return nil, fmt.Errorf("%w: %v", ErrUnknownKeyType, key.Type())
		}

		results = append(results, kv.KeyValue{
			Key:         []byte(key.Build()),
			Value:       valueData,
			ModRevision: 0,
		})
	}

	return results, nil
}
