// Package namer represent interface to templates creation.
package namer

import (
	"errors"
	"strings"
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
	// ErrInvalidInput is returned when input data is invalid.
	ErrInvalidInput = errors.New("failed to generate: invalid input data")
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

// GenerateNames generates set of names from basic name.
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
		result, _ := strings.CutPrefix(name, n.prefix)

		parts := strings.Split(result, "/")

		key.Name = name

		switch parts[1] {
		case hashName:
			{
				key.Property = ""
				key.Type = KeyTypeHash
			}
		case sigName:
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
