package integrity

import (
	"bytes"

	"github.com/tarantool/go-option"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

// Validator verifies integrity-protected key-value pairs.
type Validator[T any] struct {
	namer      namer.Namer
	marshaller marshaller.TypedMarshaller[T]
	hashers    map[string]hasher.Hasher
	verifiers  map[string]crypto.Verifier
}

// NewValidator creates a new Validator instance.
func NewValidator[T any](
	namer namer.Namer,
	marshaller marshaller.TypedMarshaller[T],
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

type extendedKV struct {
	parsedKey namer.Key
	keyValue  kv.KeyValue
}

func (v Validator[T]) aggregate(kvs []kv.KeyValue) (map[string][]extendedKV, error) {
	out := make(map[string][]extendedKV)

	for _, kvi := range kvs {
		parsedKey, err := v.namer.ParseKey(string(kvi.Key))
		if err != nil {
			return nil, errFailedToParseKey(err)
		}

		if _, ok := out[parsedKey.Name()]; !ok {
			out[parsedKey.Name()] = nil
		}

		out[parsedKey.Name()] = append(out[parsedKey.Name()], extendedKV{
			parsedKey: parsedKey,
			keyValue:  kvi,
		})
	}

	return out, nil
}

func (v Validator[T]) constructHashers() map[string]hasher.Hasher {
	out := make(map[string]hasher.Hasher)
	for _, hash := range v.hashers {
		out[hash.Name()] = hash
	}

	return out
}

func (v Validator[T]) constructVerifiers() map[string]crypto.Verifier {
	out := make(map[string]crypto.Verifier)
	for _, verifier := range v.verifiers {
		out[verifier.Name()] = verifier
	}

	return out
}

func (v Validator[T]) validateSingle(name string, kvs []extendedKV) ValidatedResult[T] {
	expectedHashers := v.constructHashers()
	expectedVerifiers := v.constructVerifiers()

	aggregatedError := &FailedToValidateAggregatedError{parent: nil}

	output := ValidatedResult[T]{
		Name:  name,
		Value: option.None[T](),
		Error: nil,
	}

	var (
		body []byte
	)

	for _, kvi := range kvs {
		if kvi.parsedKey.Type() != namer.KeyTypeValue {
			continue
		}

		val, err := v.marshaller.Unmarshal(kvi.keyValue.Value)
		if err != nil {
			output.Error = errFailedToUnmarshal(err)

			return output
		}

		output.Value = option.Some(val)
		body = kvi.keyValue.Value
	}

	for _, kvi := range kvs {
		switch kvi.parsedKey.Type() {
		case namer.KeyTypeValue:
			// Already processed above.
			continue
		case namer.KeyTypeHash:
			hasher, ok := expectedHashers[kvi.parsedKey.Property()]
			if !ok {
				continue // We've got hasher that we don't expect, skip it.
			}

			delete(expectedHashers, kvi.parsedKey.Property())

			hash, err := hasher.Hash(body)
			switch {
			case err != nil:
				aggregatedError.Append(errFailedToComputeHashWith(kvi.parsedKey.Property(), err))
			case !bytes.Equal(hash, kvi.keyValue.Value):
				aggregatedError.Append(errHashMismatch(kvi.parsedKey.Property(), kvi.keyValue.Value, hash))
			}
		case namer.KeyTypeSignature:
			verifier, ok := expectedVerifiers[kvi.parsedKey.Property()]
			if !ok {
				continue // We've got hasher that we don't expect, skip it.
			}

			delete(expectedVerifiers, kvi.parsedKey.Property())

			err := verifier.Verify(body, kvi.keyValue.Value)
			if err != nil {
				aggregatedError.Append(errSignatureVerificationFailed(kvi.parsedKey.Property(), err))
			}
		default:
			// Shouldn't happen, but just in case.
			continue
		}
	}

	for hasherName := range expectedHashers {
		aggregatedError.Append(errHashNotVerifiedMissing(hasherName))
	}

	for verifierName := range expectedVerifiers {
		aggregatedError.Append(errSignatureNotVerifiedMissing(verifierName))
	}

	output.Error = aggregatedError.Finalize()

	return output
}

// Validate verifies integrity-protected key-value pairs and returns the validated value.
func (v Validator[T]) Validate(kvs []kv.KeyValue) ([]ValidatedResult[T], error) {
	parseKeyResult, err := v.aggregate(kvs)
	if err != nil {
		return nil, err
	}

	out := make([]ValidatedResult[T], 0, len(parseKeyResult))
	for name, keys := range parseKeyResult {
		out = append(out, v.validateSingle(name, keys))
	}

	return out, nil
}
