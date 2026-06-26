package namer

import (
	"fmt"
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

func (t KeyType) String() string {
	switch t {
	case KeyTypeValue:
		return "value"
	case KeyTypeHash:
		return "hash"
	case KeyTypeSignature:
		return "signature"
	default:
		return fmt.Sprintf("KeyType(%d)", t)
	}
}

// Key is a single key emitted or parsed by a Namer.
type Key struct {
	name     string  // Object identifier.
	keyType  KeyType // Type of object (hash/signature/value).
	property string  // Additional metadata (version/algorithm).
	raw      string  // Raw key string.
}

// NewKey returns a new Key.
func NewKey(name string, keytype KeyType, property string, raw string) Key {
	return Key{
		name:     name,
		keyType:  keytype,
		property: property,
		raw:      raw,
	}
}

// Name returns name of the key.
func (k Key) Name() string {
	return k.name
}

// Type returns type of the key.
func (k Key) Type() KeyType {
	return k.keyType
}

// Property returns property of the key.
func (k Key) Property() string {
	return k.property
}

// Build reconstructs key string.
func (k Key) Build() string {
	return k.raw
}
