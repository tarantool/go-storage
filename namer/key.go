package namer

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

// Key defines the minimal interface required by keys.
type Key interface {
	Name() string     // Get object name.
	Type() KeyType    // Get key type.
	Property() string // Get metadata (e.g., algorithm version).
	Build() string    // Reconstruct raw key string.
}

// DefaultKey implements default realization.
type DefaultKey struct {
	name     string  // Object identifier.
	keyType  KeyType // Type of object (hash/signature/value).
	property string  // Additional metadata (version/algorithm).
	raw      string  // Raw key string.
}

// NewDefaultKey returns new Key object.
func NewDefaultKey(name string, keytype KeyType, property string, raw string) DefaultKey {
	return DefaultKey{
		name:     name,
		keyType:  keytype,
		property: property,
		raw:      raw,
	}
}

// Name returns name of the key.
func (k DefaultKey) Name() string {
	return k.name
}

// Type returns type of the key.
func (k DefaultKey) Type() KeyType {
	return k.keyType
}

// Property returns property of the key.
func (k DefaultKey) Property() string {
	return k.property
}

// Build reconstructs key string.
func (k DefaultKey) Build() string {
	return k.raw
}
