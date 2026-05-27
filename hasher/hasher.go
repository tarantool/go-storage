// Package hasher provides types and interfaces for hash calculating.
package hasher

import (
	"crypto/sha1" //nolint:gosec
	"crypto/sha256"
	"fmt"
	"hash"
)

// Hasher is the interface that storage hashers must implement.
// It provides low-level operations for hash calculating.
//
// Implementations must accept nil and zero-length input and return the
// well-defined hash of the empty bit string for both. This matters because
// storage backends round-trip empty values as nil, so a read of a
// legitimately-empty value must hash without error.
type Hasher interface {
	Name() string
	Hash(data []byte) ([]byte, error)
}

type sha256Hasher struct {
	hash hash.Hash
}

// NewSHA256Hasher creates a new sha256Hasher instance.
func NewSHA256Hasher() Hasher {
	return &sha256Hasher{
		hash: sha256.New(),
	}
}

// Name implements Hasher interface.
func (h *sha256Hasher) Name() string {
	return "sha256"
}

// Hash implements Hasher interface.
func (h *sha256Hasher) Hash(data []byte) ([]byte, error) {
	h.hash.Reset()

	n, err := h.hash.Write(data)
	if n < len(data) || err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	return h.hash.Sum(nil), nil
}

type sha1Hasher struct {
	hash hash.Hash
}

// NewSHA1Hasher creates a new NewSHA1Hasher instance.
func NewSHA1Hasher() Hasher {
	return &sha1Hasher{
		hash: sha1.New(), //nolint:gosec
	}
}

// Name implements Hasher interface.
func (h *sha1Hasher) Name() string {
	return "sha1"
}

// Hash implements Hasher interface.
func (h *sha1Hasher) Hash(data []byte) ([]byte, error) {
	h.hash.Reset()

	n, err := h.hash.Write(data)
	if n < len(data) || err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	return h.hash.Sum(nil), nil
}
