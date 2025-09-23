package operation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-storage/operation"
)

func TestGet(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	op := operation.Get(key)

	assert.Equal(t, operation.TypeGet, op.Type())
	assert.Equal(t, key, op.Key())
	assert.Nil(t, op.Value())
	assert.Empty(t, op.Options())
}

func TestGetWithOptions(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	op := operation.Get(key, operation.Option{}, operation.Option{})

	assert.Equal(t, operation.TypeGet, op.Type())
	assert.Equal(t, key, op.Key())
	assert.Nil(t, op.Value())
	assert.Len(t, op.Options(), 2)
}

func TestPut(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	value := []byte("test-value")
	op := operation.Put(key, value)

	assert.Equal(t, operation.TypePut, op.Type())
	assert.Equal(t, key, op.Key())
	assert.Equal(t, value, op.Value())
	assert.Empty(t, op.Options())
}

func TestPutWithOptions(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	value := []byte("test-value")
	op := operation.Put(key, value, operation.Option{}, operation.Option{})

	assert.Equal(t, operation.TypePut, op.Type())
	assert.Equal(t, key, op.Key())
	assert.Equal(t, value, op.Value())
	assert.Len(t, op.Options(), 2)
}

func TestDelete(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	op := operation.Delete(key)

	assert.Equal(t, operation.TypeDelete, op.Type())
	assert.Equal(t, key, op.Key())
	assert.Nil(t, op.Value())
	assert.Empty(t, op.Options())
}

func TestDeleteWithOptions(t *testing.T) {
	t.Parallel()

	key := []byte("test-key")
	op := operation.Delete(key, operation.Option{}, operation.Option{})

	assert.Equal(t, operation.TypeDelete, op.Type())
	assert.Equal(t, key, op.Key())
	assert.Nil(t, op.Value())
	assert.Len(t, op.Options(), 2)
}
