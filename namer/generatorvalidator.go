package namer

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
)

// Generator generates signer K/V pairs.
// Implementation should use `generic` and will used for strong typing of the solution.
type Generator[T any] interface {
	Generate(name string, value T) ([]kv.KeyValue, error)
}

// Validator validates and build the object from K/V.
type Validator[T any] interface {
	Validate(pairs []kv.KeyValue) (T, error)
}

// DefaultGeneratorValidator represent default generator-validator.
type DefaultGeneratorValidator[T any] struct {
	Namer          Namer
	Hasher         hasher.Hasher
	SignerVerifier crypto.SignerVerifier
	Marshaller     marshaller.Marshallable
}

// NewDefaultGeneratorValidator returns new object.
func NewDefaultGeneratorValidator[T any](
	namer Namer,
	hasher hasher.Hasher,
	signverifier crypto.SignerVerifier,
	marshaller marshaller.Marshallable,
) DefaultGeneratorValidator[T] {
	return DefaultGeneratorValidator[T]{
		Namer:          namer,
		Hasher:         hasher,
		SignerVerifier: signverifier,
		Marshaller:     marshaller,
	}
}

// Generate create KV pairs with value, hash and signature.
func (gv DefaultGeneratorValidator[T]) Generate(name string, value T) ([]kv.KeyValue, error) {
	if name == "" {
		return []kv.KeyValue{}, ErrInvalidInput
	}

	var kvList []kv.KeyValue

	blob, err := gv.Marshaller.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal: %w", err)
	}

	hash, err := gv.Hasher.Hash(blob)
	if err != nil {
		return nil, fmt.Errorf("failed to hash: %w", err)
	}

	signature, err := gv.SignerVerifier.Sign(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	names := gv.Namer.GenerateNames(name)
	keys := gv.Namer.ParseNames(names)

	for _, key := range keys {
		switch key.Type {
		case KeyTypeValue:
			{
				kvList = append(kvList, kv.KeyValue{
					Key:         []byte(key.Name),
					Value:       blob,
					ModRevision: 1,
				})
			}
		case KeyTypeHash:
			{
				kvList = append(kvList, kv.KeyValue{
					Key:         []byte(key.Name),
					Value:       hash,
					ModRevision: 1,
				})
			}
		case KeyTypeSignature:
			{
				kvList = append(kvList, kv.KeyValue{
					Key:         []byte(key.Name),
					Value:       signature,
					ModRevision: 1,
				})
			}
		}
	}

	return kvList, nil
}

// Validate checks hash match, verify signature, unmarshall object and return it.
func (gv DefaultGeneratorValidator[T]) Validate(pairs []kv.KeyValue) (T, error) {
	var value T

	var blob []byte

	var hash, signature []byte

	for _, keyvalue := range pairs {
		switch {
		case strings.Contains(string(keyvalue.Key), hashName):
			hash = keyvalue.Value
		case strings.Contains(string(keyvalue.Key), sigName):
			signature = keyvalue.Value
		default:
			blob = keyvalue.Value
		}
	}

	if blob == nil || hash == nil || signature == nil {
		return value, ErrInvalidKey
	}

	err := gv.SignerVerifier.Verify(hash, signature)
	if err != nil {
		return value, fmt.Errorf("signature verification failed: %w", err)
	}

	computedHash, err := gv.Hasher.Hash(blob)
	if !bytes.Equal(computedHash, hash) || err != nil {
		return value, ErrHashMismatch
	}

	err = gv.Marshaller.Unmarshal(blob, &value)
	if err != nil {
		return value, fmt.Errorf("failed to unmarshal: %w", err)
	}

	return value, nil
}
