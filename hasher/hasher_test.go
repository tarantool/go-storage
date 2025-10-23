package hasher_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-storage/hasher"
)

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
