// Package namer represent interface to templates creation.
package namer

// Namer defines the interface for generating and parsing storage key names.
type Namer interface {
	GenerateNames(name string) ([]Key, error)
	ParseKey(name string) (Key, error)
	ParseKeys(names []string, ignoreError bool) (Results, error)
	Prefix(val string, isPrefix bool) string
	// Prefixes returns one prefix per key category (value, hash, sig) so a
	// range walk can fetch every key the namer owns. For namers whose
	// categories share a single root the returned slice may have a single
	// element. Use this instead of Prefix when validating integrity-protected
	// data — Prefix only covers the value layer, which makes the validator
	// report missing hash/sig keys.
	Prefixes(val string, isPrefix bool) []string
}
