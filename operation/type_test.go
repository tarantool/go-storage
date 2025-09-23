package operation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-storage/operation"
)

func TestTypeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		typ      operation.Type
		expected string
	}{
		{"TypeGet", operation.TypeGet, "Get"},
		{"TypePut", operation.TypePut, "Put"},
		{"TypeDelete", operation.TypeDelete, "Delete"},
		{"UnknownType", operation.Type(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.typ.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}
