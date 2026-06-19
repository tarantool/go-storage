package hasher_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/hasher"
)

// sha1EmptyHex is the hex-encoded SHA-1 of the empty bit string.
const sha1EmptyHex = "da39a3ee5e6b4b0d3255bfef95601890afd80709"

// sha256EmptyHex is the hex-encoded SHA-256 of the empty bit string.
const sha256EmptyHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func TestSHA1Hasher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []byte
		out  string
	}{
		{"nil", nil, sha1EmptyHex},
		{"empty", []byte(""), sha1EmptyHex},
		{"abc", []byte("abc"), "a9993e364706816aba3e25717850c26c9cd0d89d"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Default (ModeAuto) output is the raw digest.
			h := hasher.NewSHA1Hasher()

			result, err := h.Hash(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.out, hex.EncodeToString(result))

			// ModeHex returns lower-case hex.
			hexResult, err := hasher.NewSHA1Hasher(hasher.WithMode(hasher.ModeHex)).Hash(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.out, string(hexResult))

			// ModeBin returns the raw digest.
			binResult, err := hasher.NewSHA1Hasher(hasher.WithMode(hasher.ModeBin)).Hash(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.out, hex.EncodeToString(binResult))
		})
	}
}

// TestSHA1Hasher_NilEqualsEmpty pins the contract that nil and []byte{} are
// indistinguishable inputs — storage backends round-trip empty values as nil,
// so a divergence here would surface as spurious validation failures.
func TestSHA1Hasher_NilEqualsEmpty(t *testing.T) {
	t.Parallel()

	h := hasher.NewSHA1Hasher()

	hashNil, err := h.Hash(nil)
	require.NoError(t, err)

	hashEmpty, err := h.Hash([]byte{})
	require.NoError(t, err)

	assert.Equal(t, hashEmpty, hashNil)
}

func TestSHA256Hasher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       []byte
		expected string
	}{
		{"nil", nil, sha256EmptyHex},
		{"empty", []byte(""), sha256EmptyHex},
		{"abc", []byte("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Default (ModeAuto) output is the raw digest.
			h := hasher.NewSHA256Hasher()

			result, err := h.Hash(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.expected, hex.EncodeToString(result))

			// ModeHex returns lower-case hex.
			hexResult, err := hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeHex)).Hash(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.expected, string(hexResult))

			// ModeBin returns the raw digest.
			binResult, err := hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeBin)).Hash(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.expected, hex.EncodeToString(binResult))
		})
	}
}

// TestHasherVerify_Modes pins the per-mode Verify contract: ModeAuto accepts a
// stored digest in either raw or hex form, ModeHex only hex, ModeBin only raw.
func TestHasherVerify_Modes(t *testing.T) {
	t.Parallel()

	data := []byte("abc")
	raw, err := hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeBin)).Hash(data)
	require.NoError(t, err)

	hexDigest, err := hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeHex)).Hash(data)
	require.NoError(t, err)

	tests := []struct {
		name      string
		mode      hasher.Mode
		stored    []byte
		wantError bool
	}{
		{"auto accepts raw", hasher.ModeAuto, raw, false},
		{"auto accepts hex", hasher.ModeAuto, hexDigest, false},
		{"auto rejects garbage", hasher.ModeAuto, []byte("nope"), true},
		{"hex accepts hex", hasher.ModeHex, hexDigest, false},
		{"hex rejects raw", hasher.ModeHex, raw, true},
		{"bin accepts raw", hasher.ModeBin, raw, false},
		{"bin rejects hex", hasher.ModeBin, hexDigest, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := hasher.NewSHA256Hasher(hasher.WithMode(test.mode))

			err := h.Verify(data, test.stored)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSHA256Hasher_NilEqualsEmpty pins the contract that nil and []byte{} are
// indistinguishable inputs — storage backends round-trip empty values as nil,
// so a divergence here would surface as spurious validation failures.
func TestSHA256Hasher_NilEqualsEmpty(t *testing.T) {
	t.Parallel()

	h := hasher.NewSHA256Hasher()

	hashNil, err := h.Hash(nil)
	require.NoError(t, err)

	hashEmpty, err := h.Hash([]byte{})
	require.NoError(t, err)

	assert.Equal(t, hashEmpty, hashNil)
}
