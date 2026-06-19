// Package hasher provides types and interfaces for hash calculating.
package hasher

import (
	"bytes"
	"crypto/sha1" //nolint:gosec
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
)

// ErrHashMismatch is returned by Verify when the stored digest does not match
// any encoding of the computed digest accepted under the hasher's Mode.
var ErrHashMismatch = errors.New("hash mismatch")

// Hasher is the interface that storage hashers must implement.
// It provides low-level operations for hash calculating.
//
// The encoding of the produced digest and the encodings accepted by Verify are
// controlled by the hasher's Mode (see ModeAuto, ModeHex, ModeBin). By default
// (ModeAuto) Hash returns the raw digest bytes and Verify accepts a stored
// digest in either raw or hex form.
//
// Implementations must accept nil and zero-length input and return the
// well-defined hash of the empty bit string for both. This matters because
// storage backends round-trip empty values as nil, so a read of a
// legitimately-empty value must hash without error.
type Hasher interface {
	Name() string
	// Hash returns the digest of data, encoded according to the hasher's Mode.
	Hash(data []byte) ([]byte, error)
	// Verify reports whether stored is an accepted encoding of the digest of
	// data under the hasher's Mode. It returns nil on a match.
	Verify(data, stored []byte) error
}

type sha256Hasher struct {
	hash hash.Hash
	mode Mode
}

// NewSHA256Hasher creates a new SHA-256 hasher. Its encoding behaviour is
// controlled by the given options; see WithMode (default ModeAuto).
func NewSHA256Hasher(opts ...Option) Hasher {
	return &sha256Hasher{
		hash: sha256.New(),
		mode: newConfig(opts...).mode,
	}
}

// Name implements Hasher interface.
func (h *sha256Hasher) Name() string {
	return "sha256"
}

// Hash implements Hasher interface.
func (h *sha256Hasher) Hash(data []byte) ([]byte, error) {
	sum, err := h.sum(data)
	if err != nil {
		return nil, err
	}

	return encode(sum, h.mode), nil
}

// Verify implements Hasher interface.
func (h *sha256Hasher) Verify(data, stored []byte) error {
	sum, err := h.sum(data)
	if err != nil {
		return err
	}

	return verify(sum, stored, h.mode)
}

func (h *sha256Hasher) sum(data []byte) ([]byte, error) {
	h.hash.Reset()

	n, err := h.hash.Write(data)
	if n < len(data) || err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	return h.hash.Sum(nil), nil
}

type sha1Hasher struct {
	hash hash.Hash
	mode Mode
}

// NewSHA1Hasher creates a new SHA-1 hasher. Its encoding behaviour is
// controlled by the given options; see WithMode (default ModeAuto).
func NewSHA1Hasher(opts ...Option) Hasher {
	return &sha1Hasher{
		hash: sha1.New(), //nolint:gosec
		mode: newConfig(opts...).mode,
	}
}

// Name implements Hasher interface.
func (h *sha1Hasher) Name() string {
	return "sha1"
}

// Hash implements Hasher interface.
func (h *sha1Hasher) Hash(data []byte) ([]byte, error) {
	sum, err := h.sum(data)
	if err != nil {
		return nil, err
	}

	return encode(sum, h.mode), nil
}

// Verify implements Hasher interface.
func (h *sha1Hasher) Verify(data, stored []byte) error {
	sum, err := h.sum(data)
	if err != nil {
		return err
	}

	return verify(sum, stored, h.mode)
}

func (h *sha1Hasher) sum(data []byte) ([]byte, error) {
	h.hash.Reset()

	n, err := h.hash.Write(data)
	if n < len(data) || err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	return h.hash.Sum(nil), nil
}

// encode returns sum encoded according to mode: hex for ModeHex, raw bytes for
// ModeAuto and ModeBin.
func encode(sum []byte, mode Mode) []byte {
	if mode != ModeHex {
		return sum
	}

	return hexEncode(sum)
}

// verify reports whether stored is an accepted encoding of the raw digest sum
// under mode. ModeBin accepts only the raw bytes, ModeHex only the hex form,
// ModeAuto accepts either.
func verify(sum, stored []byte, mode Mode) error {
	rawOK := mode != ModeHex && bytes.Equal(stored, sum)
	hexOK := mode != ModeBin && bytes.Equal(stored, hexEncode(sum))

	if rawOK || hexOK {
		return nil
	}

	return fmt.Errorf("%w: stored %x, computed digest %x", ErrHashMismatch, stored, sum)
}

func hexEncode(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)

	return dst
}
