package predicate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-storage/predicate"
)

func TestValueNotEqual(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	value := "test-value"
	p := predicate.ValueNotEqual(key, value)

	assert.Equal(t, key, p.Key())
	assert.Equal(t, predicate.OpNotEqual, p.Operation())
	assert.Equal(t, predicate.TargetValue, p.Target())
	assert.Equal(t, value, p.Value())
}

func TestValueEqual(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	value := 42
	p := predicate.ValueEqual(key, value)

	assert.Equal(t, key, p.Key())
	assert.Equal(t, predicate.OpEqual, p.Operation())
	assert.Equal(t, predicate.TargetValue, p.Target())
	assert.Equal(t, value, p.Value())
}

func TestVersionEqual(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	version := int64(123)
	p := predicate.VersionEqual(key, version)

	assert.Equal(t, key, p.Key())
	assert.Equal(t, predicate.OpEqual, p.Operation())
	assert.Equal(t, predicate.TargetVersion, p.Target())
	assert.Equal(t, version, p.Value())
}

func TestVersionNotEqual(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	version := int64(456)
	p := predicate.VersionNotEqual(key, version)

	assert.Equal(t, key, p.Key())
	assert.Equal(t, predicate.OpNotEqual, p.Operation())
	assert.Equal(t, predicate.TargetVersion, p.Target())
	assert.Equal(t, version, p.Value())
}

func TestVersionGreater(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	version := int64(789)
	p := predicate.VersionGreater(key, version)

	assert.Equal(t, key, p.Key())
	assert.Equal(t, predicate.OpGreater, p.Operation())
	assert.Equal(t, predicate.TargetVersion, p.Target())
	assert.Equal(t, version, p.Value())
}

func TestVersionLess(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	version := int64(999)
	p := predicate.VersionLess(key, version)

	assert.Equal(t, key, p.Key())
	assert.Equal(t, predicate.OpLess, p.Operation())
	assert.Equal(t, predicate.TargetVersion, p.Target())
	assert.Equal(t, version, p.Value())
}
