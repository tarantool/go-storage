package predicate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-storage/predicate"
)

func TestOpString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       predicate.Op
		expected string
	}{
		{"OpEqual", predicate.OpEqual, "Equal"},
		{"OpNotEqual", predicate.OpNotEqual, "NotEqual"},
		{"OpGreater", predicate.OpGreater, "Greater"},
		{"OpLess", predicate.OpLess, "Less"},
		{"UnknownOp", predicate.Op(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.op.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}
