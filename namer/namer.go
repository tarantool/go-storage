// Package namer represent interface to templates creation.
package namer

import (
	"github.com/tarantool/go-storage/kv"
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

// Generator generates signer K/V pairs.
// Implementation should use `generic` and will used for strong typing of the solution.
type Generator[T any] interface {
	Generate(name string, value T) ([]kv.KeyValue, error)
}

// Validator validates and build the object from K/V.
type Validator[T any] interface {
	Validate(pairs []kv.KeyValue) (T, error)
}
