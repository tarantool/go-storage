package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/namer"
)

const (
	defaultName     = "all"
	defaultType     = namer.KeyTypeHash
	defaultProperty = "sha256"
	defaultRaw      = "/config/hash/sha256/all"
)

func TestNewDefaultKey(t *testing.T) {
	t.Parallel()

	a := namer.NewDefaultKey(defaultName, defaultType, defaultProperty, defaultRaw)
	require.NotEmpty(t, a)
}

func TestDefaultKey_Build(t *testing.T) {
	t.Parallel()

	a := namer.NewDefaultKey(defaultName, defaultType, defaultProperty, defaultRaw)
	require.Equal(t, defaultRaw, a.Build())
}

func TestDefaultKey_Name(t *testing.T) {
	t.Parallel()

	a := namer.NewDefaultKey(defaultName, defaultType, defaultProperty, defaultRaw)
	assert.Equal(t, defaultName, a.Name())
}

func TestDefaultKey_Type(t *testing.T) {
	t.Parallel()

	a := namer.NewDefaultKey(defaultName, defaultType, defaultProperty, defaultRaw)
	assert.Equal(t, defaultType, a.Type())
}

func TestDefaultKey_Property(t *testing.T) {
	t.Parallel()

	a := namer.NewDefaultKey(defaultName, defaultType, defaultProperty, defaultRaw)
	assert.Equal(t, defaultProperty, a.Property())
}

func TestKeyType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keyType  namer.KeyType
		expected string
	}{
		{
			name:     "KeyTypeValue",
			keyType:  namer.KeyTypeValue,
			expected: "KeyTypeValue",
		},
		{
			name:     "KeyTypeHash",
			keyType:  namer.KeyTypeHash,
			expected: "KeyTypeHash",
		},
		{
			name:     "KeyTypeSignature",
			keyType:  namer.KeyTypeSignature,
			expected: "KeyTypeSignature",
		},
		{
			name:     "Unknown key type zero",
			keyType:  0,
			expected: "KeyType[0]",
		},
		{
			name:     "Unknown key type negative",
			keyType:  -1,
			expected: "KeyType[-1]",
		},
		{
			name:     "Unknown key type positive",
			keyType:  100,
			expected: "KeyType[100]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.keyType.String())
		})
	}
}
