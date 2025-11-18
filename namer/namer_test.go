package namer_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-storage/namer"
)

func TestGenerateNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefix   string
		expected []string
	}{
		{
			"all",
			"/tt/config",
			[]string{"/tt/config/all", "/tt/config/hash/all", "/tt/config/sig/all"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(tt.prefix)

			result := dn.GenerateNames(tt.name)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestParseNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefix   string
		names    []string
		expected []namer.Key
	}{
		{
			"all",
			"/tt/config",
			[]string{"/tt/config/all", "/tt/config/hash/all", "/tt/config/sig/all"},
			[]namer.Key{
				{Name: "/tt/config/all", Type: namer.KeyTypeValue, Property: ""},
				{Name: "/tt/config/hash/all", Type: namer.KeyTypeHash, Property: ""},
				{Name: "/tt/config/sig/all", Type: namer.KeyTypeSignature, Property: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(tt.prefix)

			result := dn.ParseNames(tt.names)
			require.Equal(t, tt.expected, result)
		})
	}
}
