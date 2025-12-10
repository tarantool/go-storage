// Package namer represent interface to templates creation.
package namer

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/verification"

	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/marshaller"
)

const (
	hashName      = "hash"
	signatureName = "sig"
	keysPerName   = 3
)

var (
	// ErrInvalidKey is returned when missing key, hash or signature.
	ErrInvalidKey = errors.New("missing key, hash or signature")
	// ErrHashMismatch is returned when hash mismatch.
	ErrHashMismatch = errors.New("hash mismatch")
	// ErrInvalidInput is returned when input data is invalid.
	ErrInvalidInput = errors.New("failed to generate: invalid input data")
	// ErrInvalidPrefix is returned when prefix didn't match.
	ErrInvalidPrefix = errors.New("invalid prefix")
)

//-----------------------------------------------------------------------------

// Results represents Namer working result.
type Results struct {
	IsSingle     bool             // True if result contains only one object name.
	IsSingleName string           // Cached name when isSingle=true.
	Result       map[string][]Key // Grouped keys: object name -> key list.
}

// SelectSingle gets keys for single-name case (if applicable).
func (r *Results) SelectSingle() ([]Key, bool) {
	if r.IsSingle {
		return r.Result[r.IsSingleName], true
	}

	return nil, false
}

// Items return iterator over all name->keys groups.
func (r *Results) Items() iter.Seq2[string, []Key] {
	return func(yield func(str string, res []Key) bool) {
		for i, v := range r.Result {
			if !yield(i, v) {
				return
			}
		}
	}
}

// Select gets keys for a specific object name.
func (r *Results) Select(name string) ([]Key, bool) {
	if i, ok := r.Result[name]; ok {
		return i, true
	}

	return nil, false
}

// Len returns the number of unique object names.
func (r *Results) Len() int {
	return len(r.Result)
}

//-----------------------------------------------------------------------------

// DefaultNamer represents default namer.
type DefaultNamer struct {
	prefix         string
	hasher         hasher.Hasher
	signerverifier verification.SignerVerifier
	marshaller     marshaller.Marshallable
}

// NewDefaultNamer returns new DefaultNamer object.
func NewDefaultNamer(prefix string, hasher hasher.Hasher, signerverifier verification.SignerVerifier,
	marshaller marshaller.Marshallable,
) *DefaultNamer {
	prefix = strings.TrimPrefix(prefix, "/")

	return &DefaultNamer{
		prefix:         prefix,
		hasher:         hasher,
		signerverifier: signerverifier,
		marshaller:     marshaller,
	}
}

// GenerateMulti generates all keys for an object names.
func (n *DefaultNamer) GenerateMulti(kvs []kv.KeyValue) Results {
	var out Results

	out.Result = make(map[string][]Key)

	for _, keyvalue := range kvs {
		keys, err := n.Generate(keyvalue)
		if err != nil {
			continue
		}

		out.Result[string(keyvalue.Key)] = keys
	}

	if len(kvs) == 1 {
		out.IsSingle = true
		out.IsSingleName = string(kvs[0].Key)
	}

	return out
}

// Generate generates all required keys for an object name.
func (n *DefaultNamer) Generate(keyvalue kv.KeyValue) ([]Key, error) {
	name := string(keyvalue.Key)
	if name == "" {
		return nil, ErrInvalidInput
	}

	marshalled, err := n.marshaller.Marshal(keyvalue.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal: %w", err)
	}

	hash, err := n.hasher.Hash(marshalled)
	if err != nil {
		return nil, fmt.Errorf("failed to hash: %w", err)
	}

	signature, err := n.signerverifier.Sign(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	valK := NewDefaultKey(
		"/"+n.prefix+"/"+name,
		KeyTypeValue,
		"",
		marshalled,
	)
	hashK := NewDefaultKey(
		"/"+n.prefix+"/"+hashName+"/"+n.hasher.Name()+"/"+name,
		KeyTypeHash,
		n.hasher.Name(),
		hash,
	)
	sigK := NewDefaultKey(
		"/"+n.prefix+"/"+signatureName+"/"+name,
		KeyTypeSignature,
		"",
		signature,
	)

	return []Key{valK, hashK, sigK}, nil
}

// ParseKVMulti convert set of raw KVs to set of KVs with original key/values.
func (n *DefaultNamer) ParseKVMulti(kvs []kv.KeyValue) ([]kv.KeyValue, error) {
	out := make([]kv.KeyValue, 0, len(kvs)/keysPerName)

	var temp_results Results

	// Combine keys in threes to get complete set of data to decode the original value.
	for _, keyvalue := range kvs {
		key, err := n.ParseKV(keyvalue)
		if err != nil {
			return nil, err
		}

		temp_results.Result[key.Name()] = append(temp_results.Result[key.Name()], key)
	}

	// Decoding.
	for _, keyset := range temp_results.Items() {
		keyvalue, err := n.DecodeKey(keyset)
		if err != nil {
			return nil, err
		}

		out = append(out, keyvalue)
	}

	return out, nil
}

// ParseKV converts KV to Key to combine it by `name`.
func (n *DefaultNamer) ParseKV(keyvalue kv.KeyValue) (Key, error) {
	var out Key

	result, cut := strings.CutPrefix(string(keyvalue.Key), n.prefix)
	if !cut {
		return DefaultKey{}, ErrInvalidPrefix
	}

	result, _ = strings.CutPrefix(result, "/")

	parts := strings.Split(result, "/")
	partsLen := len(parts)

	switch parts[0] {
	case hashName + "/":
		out = NewDefaultKey(
			parts[partsLen-1],
			KeyTypeHash,
			parts[partsLen-2],
			keyvalue.Value,
		)
	case signatureName + "/":
		out = NewDefaultKey(
			parts[partsLen-1],
			KeyTypeSignature,
			"",
			keyvalue.Value,
		)
	default:
		out = NewDefaultKey(
			parts[partsLen-1],
			KeyTypeValue,
			"",
			keyvalue.Value,
		)
	}

	return out, nil
}

// DecodeKey converts sets of three Keys into original KV.
func (n *DefaultNamer) DecodeKey(keyset []Key) (kv.KeyValue, error) {
	var out kv.KeyValue

	var marshalledValue, hash, signature []byte

	for _, key := range keyset {
		switch {
		case strings.Contains(key.Name(), hashName):
			hash = key.Raw()
		case strings.Contains(key.Name(), signatureName):
			signature = key.Raw()
		default:
			out.Key = []byte(key.Name())
			marshalledValue = key.Raw()
		}
	}

	if out.Key == nil || hash == nil || signature == nil {
		return out, ErrInvalidKey
	}

	err := n.marshaller.Unmarshal(marshalledValue, &out.Value)
	if err != nil {
		return out, fmt.Errorf("failed to unmarshal: %w", err)
	}

	computedHash, err := n.hasher.Hash(out.Key)
	if !bytes.Equal(computedHash, hash) || err != nil {
		return out, ErrHashMismatch
	}

	err = n.signerverifier.Verify(out.Value, signature)
	if err != nil {
		return out, fmt.Errorf("signature verification failed: %w", err)
	}

	return out, nil
}
