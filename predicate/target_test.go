package predicate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-storage/predicate"
)

func TestTargetString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		target   predicate.Target
		expected string
	}{
		{"TargetVersion", predicate.TargetVersion, "Version"},
		{"TargetValue", predicate.TargetValue, "Value"},
		{"UnknownTarget", predicate.Target(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.target.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}
