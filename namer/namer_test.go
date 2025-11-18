package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-storage/namer"
)

func TestGenerate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expected []string
	}{
		{"all", []string{"/tt/config/all", "/tt/config/hash/sha1/all", "/tt/config/sig/all"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(tt.name)

			result := dn.Generate(tt.name)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		names    []string
		expected []namer.Key
	}{
		{
			"all",
			[]string{"/tt/config/all", "/tt/config/hash/sha1/all", "/tt/config/sig/all"},
			[]namer.Key{{
				Name: "", Type: namer.KeyTypeValue, Property: ""},
				{Name: "", Type: namer.KeyTypeHash, Property: "sha1"},
				{Name: "", Type: namer.KeyTypeSignature, Property: ""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(tt.name)

			result := dn.Parse(tt.names)
			assert.Equal(t, tt.expected, result)
		})
	}
}
