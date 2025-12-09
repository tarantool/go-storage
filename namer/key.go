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
	Raw() []byte      // Get raw data.
	Build() string    // Reconstruct raw key string.
}

// DefaultKey implements default realization.
type DefaultKey struct {
	name     string  // Object identifier.
	keytype  KeyType // Type of object (hash/signature/value).
	property string  // Additional metadata (version/algorithm).
	raw      []byte  // Raw key string.
}

// NewDefaultKey returns new Key object.
func NewDefaultKey(n string, k KeyType, p string, r []byte) DefaultKey {
	return DefaultKey{
		name:     n,
		keytype:  k,
		property: p,
		raw:      r,
	}
}

// Name returns name of the key.
func (k DefaultKey) Name() string {
	return k.name
}

// Type returns type of the key.
func (k DefaultKey) Type() KeyType {
	return k.keytype
}

// Property returns property of the key.
func (k DefaultKey) Property() string {
	return k.property
}

// Raw returns raw of the key.
func (k DefaultKey) Raw() []byte {
	return k.raw
}

// Build should reconstruct key from signature and digest or not?
func (k DefaultKey) Build() string {
	return string(k.raw)
}

// String returns string representation of the `raw` field.
func (k DefaultKey) String() string {
	return string(k.raw)
}
