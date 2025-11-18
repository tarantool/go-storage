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
