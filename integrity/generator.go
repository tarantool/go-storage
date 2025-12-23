package integrity

import (
	"fmt"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

// Generator creates integrity-protected key-value pairs for storage.
type Generator[T any] struct {
	namer      namer.Namer
	marshaller marshaller.TypedMarshaller[T]
	hashers    map[string]hasher.Hasher
	signers    map[string]crypto.Signer
}

// NewGenerator creates a new Generator instance.
func NewGenerator[T any](
	namer namer.Namer,
	marshaller marshaller.TypedMarshaller[T],
	hashers []hasher.Hasher,
	signers []crypto.Signer,
) Generator[T] {
	hasherMap := make(map[string]hasher.Hasher)
	for _, h := range hashers {
		hasherMap[h.Name()] = h
	}

	fmt.Println(1)

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
		return nil, errFailedToGenerateKeys(err)
	}

	marshalledValue, err := g.marshaller.Marshal(value)
	if err != nil {
		return nil, errFailedToMarshalValue(err)
	}

	results := make([]kv.KeyValue, 0, len(keys))

	for _, key := range keys {
		var valueData []byte

		switch key.Type() {
		case namer.KeyTypeValue:
			valueData = marshalledValue

		case namer.KeyTypeHash:
			hasherInstance, exists := g.hashers[key.Property()]
			if !exists { // Shouldn't happen, but just in case.
				return nil, errImpossibleHasher(key.Property())
			}

			valueData, err = hasherInstance.Hash(marshalledValue)
			if err != nil {
				return nil, errFailedToComputeHash(err)
			}

		case namer.KeyTypeSignature:
			signer, exists := g.signers[key.Property()]
			if !exists { // Shouldn't happen, but just in case.
				return nil, errImpossibleSigner(key.Property())
			}

			valueData, err = signer.Sign(marshalledValue)
			if err != nil {
				return nil, errFailedToGenerateSignature(err)
			}

		default:
			// Shouldn't happen, but just in case.
			return nil, errImpossibleKeyType(key.Type().String())
		}

		results = append(results, kv.KeyValue{
			Key:         []byte(key.Build()),
			Value:       valueData,
			ModRevision: 0,
		})
	}

	return results, nil
}
