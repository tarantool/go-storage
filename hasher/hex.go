package hasher

import (
	"encoding/hex"
	"fmt"
)

// hexHasher decorates a Hasher so that Hash returns a hex-encoded digest.
// Name is passed through unchanged, so the on-disk key layout is preserved;
// only the stored payload becomes hex.
type hexHasher struct {
	inner Hasher
}

// NewHexHasher wraps h so that Hash returns a lower-case hex-encoded digest.
func NewHexHasher(h Hasher) Hasher {
	return hexHasher{inner: h}
}

// NewHexSHA256Hasher creates a SHA-256 hasher whose Hash output is hex-encoded.
func NewHexSHA256Hasher() Hasher {
	return NewHexHasher(NewSHA256Hasher())
}

// NewHexSHA1Hasher creates a SHA-1 hasher whose Hash output is hex-encoded.
func NewHexSHA1Hasher() Hasher {
	return NewHexHasher(NewSHA1Hasher())
}

// Name implements Hasher interface.
func (h hexHasher) Name() string {
	return h.inner.Name()
}

// Hash implements Hasher interface, returning the inner digest hex-encoded.
func (h hexHasher) Hash(data []byte) ([]byte, error) {
	sum, err := h.inner.Hash(data)
	if err != nil {
		return nil, fmt.Errorf("hex hasher: %w", err)
	}

	dst := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(dst, sum)

	return dst, nil
}
