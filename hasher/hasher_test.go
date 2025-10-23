package hasher_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-storage/hasher"
)

func TestSHA1Hasher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []byte
		out  string
	}{
		{"empty", []byte(""), "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{"abc", []byte("abc"), "a9993e364706816aba3e25717850c26c9cd0d89d"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := hasher.NewSHA1Hasher()

			result, _ := h.Hash(test.in)

			assert.Equal(t, test.out, hex.EncodeToString(result))
		})
	}
}

func TestSHA1Hasher_negative(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		in                []byte
		expectedErrorText string
	}{
		{"nil", nil, "data is nil"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := hasher.NewSHA1Hasher()

			_, err := h.Hash(test.in)

			assert.Contains(t, err.Error(), test.expectedErrorText)
		})
	}
}

func TestSHA256Hasher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []byte
		out  string
	}{
		{"empty", []byte(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", []byte("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := hasher.NewSHA256Hasher()

			result, _ := h.Hash(test.in)

			assert.Equal(t, test.out, hex.EncodeToString(result))
		})
	}
}

func TestSHA256Hasher_negative(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		in                []byte
		expectedErrorText string
	}{
		{"nil", nil, "data is nil"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := hasher.NewSHA256Hasher()

			_, err := h.Hash(test.in)

			assert.Contains(t, err.Error(), test.expectedErrorText)
		})
	}
}
