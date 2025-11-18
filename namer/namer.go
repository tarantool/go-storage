// Package namer represent interface to templates creation.
package namer

import (
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
)

// KeyType represents key types.
type KeyType int

const (
	KeyTypeValue     KeyType = iota + 1 // Data.
	KeyTypeHash                         // Hash of the data.
	KeyTypeSignature                    // Signature of the data.
)

// Key implements key internal realization.
type Key struct {
	Name     string  // Object identificator.
	Type     KeyType // Type of the object.
	Property string  // Additional information (version/algorithm).
}

// Namer represents keys naming strategy.
type Namer interface {
	Generate(name string) []string // Object's keys generation.
	Parse(names []string) []Key    // Reverse keys parsing.
}

type DefaultNamer struct{}

func NewDefaultNamer(prefix string) *DefaultNamer {
	return &DefaultNamer{}
}

func (n *DefaultNamer) Generate(name string) []string {
	return []string{}
}

func (n *DefaultNamer) Parse(names []string) []Key {
	return []Key{}
}

// Generator generates signer K/V pairs.
// Implementation should use `generic` and will used for strong typing of the solution.
type Generator[T any] interface {
	Generate(name string, value T) ([]kv.KeyValue, error)
}

// Validator validates and build the object from K/V.
type Validator[T any] interface {
	Validate(pairs []kv.KeyValue) (T, error)
}

// DefaultGeneratorValidator.
type DefaultGeneratorValidator struct {
	Namer          Namer
	Hasher         hasher.Hasher
	SignerVerifier crypto.SignerVerifier
	Marshaller     marshaller.Marshallable
}

// NewDefaultGeneratorValidator returns new object.
func NewDefaultGeneratorValidator[T any](n Namer, h []hasher.Hasher, sv crypto.SignerVerifier, m marshaller.YAMLMarshaller) Generator[T] {
	return DefaultGeneratorValidator{
		Namer:          n,
		Hasher:         h,
		SignerVerifier: sv,
		Marshaller:     m,
	}
}

/*
namer.GenerateNames("all") -> ["/tt/config/all", "/tt/config/hash/sha1/all", "/tt/config/sig/all"]

namer.ParseNames(["/tt/config/all", "/tt/config/hash/sha1/all", "/tt/config/sig/all"]) -> [{Type: KeyTypeValue, Property: ""}, {Type: KeyTypeHash, Property: "sha1"}, {Type: KeyTypeSignature, Property: ""}]

generator.Generate("all", "some-object") -> [
    { Key: "/tt/config/all", Value: []byte("some-object"), },
    { Key: "/tt/config/hash/sha1/all", Value: []byte(<sha>), },
    { Key: "/tt/config/sig/all", Value: []byte(<sig>), },
]

// При чтении Validate() проверяет:
// Соответствие хеша (Hasher)
// Корректность подписи (Verifier)
// Делает Unmarshaller и возврат объекта
validator.Validate([
    { Key: "/tt/config/all", Value: []byte("some-object"), },
    { Key: "/tt/config/hash/sha1/all", Value: []byte(<sha>), },
    { Key: "/tt/config/sig/all", Value: []byte(<sig>), },
]) -> ("some-object", nil) OR (nil, "invalid-signature")
*/
