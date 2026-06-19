package hasher_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/hasher"
)

func TestHexHasher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []byte
		out  string
	}{
		{"nil-input", nil, sha256EmptyHex},
		{"empty-input", []byte(""), sha256EmptyHex},
		{"abc-input", []byte("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := hasher.NewHexSHA256Hasher()

			result, err := h.Hash(test.in)
			require.NoError(t, err)

			// Output is already the ASCII hex string of the digest.
			assert.Equal(t, test.out, string(result))

			// And it round-trips back to the raw digest of the inner hasher.
			raw, err := hex.DecodeString(string(result))
			require.NoError(t, err)

			inner, err := hasher.NewSHA256Hasher().Hash(test.in)
			require.NoError(t, err)
			assert.Equal(t, inner, raw)
		})
	}
}

func TestHexHasherName(t *testing.T) {
	t.Parallel()

	// Name is passed through unchanged so the on-disk key layout is preserved.
	assert.Equal(t, "sha256", hasher.NewHexSHA256Hasher().Name())
	assert.Equal(t, "sha1", hasher.NewHexSHA1Hasher().Name())
}
