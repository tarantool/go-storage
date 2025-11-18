// Package namer represent interface to templates creation.
package namer

import (
	"fmt"
	"strings"
)

const (
	hashName      = "hash"
	signatureName = "sig"
	maxKeyParts   = 3
)

// DefaultNamer represents default namer.
type DefaultNamer struct {
	prefix    string // Key prefix (e.g., "storage/").
	hashNames []string
	sigNames  []string
}

// NewDefaultNamer returns new DefaultNamer object with hash/signature names configuration.
func NewDefaultNamer(prefix string, hashNames []string, sigNames []string) *DefaultNamer {
	return &DefaultNamer{
		prefix:    strings.Trim(prefix, "/"),
		hashNames: hashNames,
		sigNames:  sigNames,
	}
}

// GenerateNames all keys for an object name.
func (n *DefaultNamer) GenerateNames(name string) ([]Key, error) {
	switch {
	case name == "":
		return nil, errInvalidName(name, "should not be empty")
	case strings.HasSuffix(name, "/"):
		return nil, errInvalidName(name, "should not be prefix")
	}

	name = strings.TrimPrefix(name, "/")

	out := make([]Key, 0, len(n.hashNames)+len(n.sigNames)+1)

	out = append(out,
		NewDefaultKey(
			name,
			KeyTypeValue,
			"",
			fmt.Sprintf("/%s/%s", n.prefix, name),
		))

	for _, hash := range n.hashNames {
		out = append(out,
			NewDefaultKey(
				name,
				KeyTypeHash,
				hash,
				fmt.Sprintf("/%s/%s/%s/%s", n.prefix, hashName, hash, name),
			),
		)
	}

	for _, sig := range n.sigNames {
		out = append(out,
			NewDefaultKey(
				name,
				KeyTypeSignature,
				sig,
				fmt.Sprintf("/%s/%s/%s/%s", n.prefix, signatureName, sig, name),
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
