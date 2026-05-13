// Package namer represent interface to templates creation.
package namer

import (
	"strings"
)

const (
	hashName      = "hashes"
	signatureName = "sig"
	maxKeyParts   = 3
)

// Namer defines the interface for generating and parsing storage key names.
type Namer interface {
	GenerateNames(name string) ([]Key, error)
	ParseKey(name string) (DefaultKey, error)
	ParseKeys(names []string, ignoreError bool) (Results, error)
	Prefix(val string, isPrefix bool) string
	// Prefixes returns one prefix per key category (value, hash, sig) so a
	// range walk can fetch every key the namer owns. For namers whose
	// categories share a single root (DefaultNamer) the returned slice may
	// have a single element. Use this instead of Prefix when validating
	// integrity-protected data — Prefix only covers the value layer, which
	// makes the validator report missing hash/sig keys.
	Prefixes(val string, isPrefix bool) []string
}

// DefaultNamer represents default namer.
type DefaultNamer struct {
	prefix    string // Key prefix (e.g., "storage/").
	hashNames []string
	sigNames  []string
}

// NewDefaultNamer returns new DefaultNamer object with hash/signature names configuration.
func NewDefaultNamer(prefix string, hashNames []string, sigNames []string) Namer {
	return &DefaultNamer{
		prefix:    strings.Trim(prefix, "/"),
		hashNames: hashNames,
		sigNames:  sigNames,
	}
}

// GenerateNames all keys for an object name.
func (n *DefaultNamer) GenerateNames(name string) ([]Key, error) {
	if name == "" {
		return nil, errInvalidName(name, "should not be empty")
	}

	name = strings.TrimPrefix(name, "/")

	out := make([]Key, 0, len(n.hashNames)+len(n.sigNames)+1)

	out = append(out,
		NewDefaultKey(
			name,
			KeyTypeValue,
			"",
			n.join(name),
		))

	for _, hash := range n.hashNames {
		out = append(out,
			NewDefaultKey(
				name,
				KeyTypeHash,
				hash,
				n.join(hashName, hash, name),
			),
		)
	}

	for _, sig := range n.sigNames {
		out = append(out,
			NewDefaultKey(
				name,
				KeyTypeSignature,
				sig,
				n.join(signatureName, sig, name),
			),
		)
	}

	return out, nil
}

// ParseKey parses a raw key name into a structured DefaultKey.
func (n *DefaultNamer) ParseKey(name string) (DefaultKey, error) {
	originalName := name

	name, found := strings.CutPrefix(strings.TrimPrefix(name, "/"), n.prefix)
	if !found {
		return DefaultKey{}, errInvalidKey(originalName, "prefix not found")
	}

	name = strings.TrimPrefix(name, "/")

	nameParts := strings.SplitN(name, "/", maxKeyParts)

	switch {
	case len(nameParts) == 0 || len(nameParts) > maxKeyParts:
		panic("illegal state") // Unreachable.

	case nameParts[0] == signatureName:
		return n.parseSignatureKey(nameParts, originalName)
	case nameParts[0] == hashName:
		return n.parseHashKey(nameParts, originalName)
	default:
		return n.parseValueKey(name, originalName)
	}
}

// ParseKeys combine multiple raw keys into grouped results.
func (n *DefaultNamer) ParseKeys(names []string, ignoreError bool) (Results, error) {
	out := map[string][]Key{}

	for _, name := range names {
		key, err := n.ParseKey(name)
		switch {
		case err != nil && ignoreError:
			continue
		case err != nil:
			return Results{}, err
		}

		out[key.name] = append(out[key.name], key)
	}

	return NewResults(out), nil
}

// Prefix returns the prefix used by this namer.
func (n *DefaultNamer) Prefix(val string, isPrefix bool) string {
	suffix := strings.Trim(val, "/")

	var builder strings.Builder

	builder.WriteByte('/')

	if n.prefix != "" {
		builder.WriteString(n.prefix)
		builder.WriteByte('/')
	}

	if suffix != "" {
		builder.WriteString(suffix)

		if isPrefix {
			builder.WriteByte('/')
		}
	}

	return builder.String()
}

// Prefixes returns the range prefixes for every key category. DefaultNamer
// stores all categories under a single /<prefix>/ root, so the empty-name
// case collapses to a single prefix. For non-empty names the per-category
// fan-out matters because each category interleaves an extra path segment.
func (n *DefaultNamer) Prefixes(val string, isPrefix bool) []string {
	suffix := strings.Trim(val, "/")
	if suffix == "" {
		return []string{n.Prefix(val, isPrefix)}
	}

	out := make([]string, 0, 1+len(n.hashNames)+len(n.sigNames))

	out = append(out, n.prefixWith(isPrefix, suffix))

	for _, h := range n.hashNames {
		out = append(out, n.prefixWith(isPrefix, hashName, h, suffix))
	}

	for _, s := range n.sigNames {
		out = append(out, n.prefixWith(isPrefix, signatureName, s, suffix))
	}

	return out
}

// prefixWith builds a /<prefix>/<elems...> path, optionally suffixed with "/"
// when isPrefix is true. The trailing slash matches the convention used by
// Prefix() — callers compose Get(prefix) range scans on this output.
func (n *DefaultNamer) prefixWith(isPrefix bool, elems ...string) string {
	out := n.join(elems...)
	if isPrefix {
		out += "/"
	}

	return out
}

func (n *DefaultNamer) join(elems ...string) string {
	var parts []string

	parts = append(parts, "")
	if n.prefix != "" {
		parts = append(parts, n.prefix)
	}

	parts = append(parts, elems...)

	return strings.Join(parts, "/")
}

// parseSignatureKey parses a signature key from name parts.
func (n *DefaultNamer) parseSignatureKey(nameParts []string, originalName string) (DefaultKey, error) {
	switch {
	case len(nameParts) != maxKeyParts:
		return DefaultKey{}, errInvalidKey(originalName, "found sig prefix, but key name is incomplete")
	case len(nameParts[1]) == 0:
		return DefaultKey{}, errInvalidKey(originalName, "found sig prefix, but hash name is invalid")
	case len(nameParts[2]) == 0:
		return DefaultKey{}, errInvalidKey(originalName, "found sig prefix, but key name is invalid")
	case strings.HasSuffix(nameParts[2], "/"):
		return DefaultKey{}, errInvalidKey(originalName, "found hash prefix, but key name is prefix")
	}

	return NewDefaultKey(
		nameParts[2],
		KeyTypeSignature,
		nameParts[1],
		originalName,
	), nil
}

// parseHashKey parses a hash key from name parts.
func (n *DefaultNamer) parseHashKey(nameParts []string, originalName string) (DefaultKey, error) {
	switch {
	case len(nameParts) != maxKeyParts:
		return DefaultKey{}, errInvalidKey(originalName, "found hash prefix, but key name is incomplete")
	case len(nameParts[1]) == 0:
		return DefaultKey{}, errInvalidKey(originalName, "found hash prefix, but hash name is invalid")
	case len(nameParts[2]) == 0:
		return DefaultKey{}, errInvalidKey(originalName, "found hash prefix, but key name is invalid")
	case strings.HasSuffix(nameParts[2], "/"):
		return DefaultKey{}, errInvalidKey(originalName, "found hash prefix, but key name is prefix")
	}

	return NewDefaultKey(
		nameParts[2],
		KeyTypeHash,
		nameParts[1],
		originalName,
	), nil
}

// parseValueKey parses a value key from name parts.
func (n *DefaultNamer) parseValueKey(name string, originalName string) (DefaultKey, error) {
	if strings.HasSuffix(name, "/") {
		return DefaultKey{}, errInvalidKey(originalName, "key name should not be prefix")
	}

	return NewDefaultKey(
		name,
		KeyTypeValue,
		"",
		originalName,
	), nil
}
