// Package namer represent interface to templates creation.
package namer

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
)

// KeyType represents key types.
type KeyType int

const (
	// KeyTypeValue represents data type.
	KeyTypeValue KeyType = iota + 1
	// KeyTypeHash represents hash of the data type.
	KeyTypeHash
	// KeyTypeSignature represents signature of the data type.
	KeyTypeSignature
)

const (
	hashName    = "hash"
	sigName     = "sig"
	namesNumber = 3
)

var (
	// ErrInvalidKey is returned when missing key, hash or signature.
	ErrInvalidKey = errors.New("missing key, hash or signature")
	// ErrHashMismatch is returned when hash mismatch.
	ErrHashMismatch = errors.New("hash mismatch")
)

// Key implements internal realization.
type Key struct {
	Name     string  // Object identificator.
	Type     KeyType // Type of the object.
	Property string  // Additional information (version/algorithm).
}

// Namer represents keys naming strategy.
type Namer interface {
	GenerateNames(name string) []string // Object's keys generation.
	ParseNames(names []string) []Key    // Convert names into keys.
}

// -----------------------------------------------------------------------------

// DefaultNamer represents default namer.
type DefaultNamer struct {
	prefix string
}

// NewDefaultNamer returns new DefaultNamer object.
func NewDefaultNamer(prefix string) *DefaultNamer {
	return &DefaultNamer{
		prefix: prefix,
	}
}

// GenerateNames generates set on names from basic name.
func (n *DefaultNamer) GenerateNames(name string) []string {
	return []string{
		n.prefix + "/" + name,
		n.prefix + "/" + hashName + "/" + name,
		n.prefix + "/" + sigName + "/" + name,
	}
}

// ParseNames returns set of Keys with different types.
func (n *DefaultNamer) ParseNames(names []string) []Key {
	keys := make([]Key, 0, namesNumber)

	for _, name := range names {
		var key Key

		// Remove prefix.
		result := strings.ReplaceAll(name, n.prefix, "")

		parts := strings.Split(result, "/")

		key.Name = name

		switch parts[1] {
		case "hash":
			{
				key.Property = ""
				key.Type = KeyTypeHash
			}
		case "sig":
			{
				key.Property = ""
				key.Type = KeyTypeSignature
			}
		default:
			{
				key.Property = ""
				key.Type = KeyTypeValue
			}
		}

		keys = append(keys, key)
	}

	return keys
}

//-----------------------------------------------------------------------------

// Generator generates signer K/V pairs.
// Implementation should use `generic` and will used for strong typing of the solution.
type Generator[T any] interface {
	Generate(name string, value T) ([]kv.KeyValue, error)
}

// Validator validates and build the object from K/V.
type Validator[T any] interface {
	Validate(pairs []kv.KeyValue) (T, error)
}

//-----------------------------------------------------------------------------

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
