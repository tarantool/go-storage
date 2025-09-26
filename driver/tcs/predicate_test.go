//nolint:testpackage
package tcs

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	goPredicate "github.com/tarantool/go-storage/predicate"
)

func TestPredicate_EncodeMsgpack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		predicate   goPredicate.Predicate
		expectError bool
		expected    []byte
	}{
		{
			name:        "value equal predicate with string value",
			predicate:   goPredicate.ValueEqual([]byte("test-key"), "test-value"),
			expectError: false,
			expected: []byte{
				0x94, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xa2, 0x3d, 0x3d,
				0xaa, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x76, 0x61, 0x6c, 0x75,
				0x65, 0xa8, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x6b, 0x65, 0x79,
			},
		},
		{
			name:        "value not equal predicate with int value",
			predicate:   goPredicate.ValueNotEqual([]byte("test-key"), 42),
			expectError: false,
			expected: []byte{
				0x94, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xa2, 0x21, 0x3d,
				0x2a, 0xa8, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x6b, 0x65, 0x79,
			},
		},
		{
			name:        "version equal predicate",
			predicate:   goPredicate.VersionEqual([]byte("test-key"), int64(123)),
			expectError: false,
			expected: []byte{
				0x94, 0xac, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76, 0x69,
				0x73, 0x69, 0x6f, 0x6e, 0xa2, 0x3d, 0x3d, 0xd3, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x7b, 0xa8, 0x74, 0x65, 0x73, 0x74,
				0x2d, 0x6b, 0x65, 0x79,
			},
		},
		{
			name:        "version not equal predicate",
			predicate:   goPredicate.VersionNotEqual([]byte("test-key"), int64(456)),
			expectError: false,
			expected: []byte{
				0x94, 0xac, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76, 0x69,
				0x73, 0x69, 0x6f, 0x6e, 0xa2, 0x21, 0x3d, 0xd3, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x1, 0xc8, 0xa8, 0x74, 0x65, 0x73, 0x74,
				0x2d, 0x6b, 0x65, 0x79,
			},
		},
		{
			name:        "version greater predicate",
			predicate:   goPredicate.VersionGreater([]byte("test-key"), int64(789)),
			expectError: false,
			expected: []byte{
				0x94, 0xac, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76, 0x69,
				0x73, 0x69, 0x6f, 0x6e, 0xa1, 0x3e, 0xd3, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x3, 0x15, 0xa8, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x6b,
				0x65, 0x79,
			},
		},
		{
			name:        "version less predicate",
			predicate:   goPredicate.VersionLess([]byte("test-key"), int64(1000)),
			expectError: false,
			expected: []byte{
				0x94, 0xac, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73,
				0x69, 0x6f, 0x6e, 0xa1, 0x3c, 0xd3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x3, 0xe8, 0xa8, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x6b, 0x65, 0x79,
			},
		},
		{
			name:        "value equal predicate with empty key",
			predicate:   goPredicate.ValueEqual([]byte(""), "empty-key-value"),
			expectError: false,
			expected: []byte{
				0x94, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xa2, 0x3d, 0x3d, 0xaf,
				0x65, 0x6d, 0x70, 0x74, 0x79, 0x2d, 0x6b, 0x65, 0x79, 0x2d, 0x76,
				0x61, 0x6c, 0x75, 0x65, 0xa0,
			},
		},
		{
			name:        "value equal predicate with nil value",
			predicate:   goPredicate.ValueEqual([]byte("test-key"), nil),
			expectError: false,
			expected: []byte{
				0x94, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xa2, 0x3d, 0x3d, 0xc0,
				0xa8, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x6b, 0x65, 0x79,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create the TCS predicate wrapper.
			tcsPredicate := predicate{Predicate: tt.predicate}

			// Encode the predicate.
			var buf bytes.Buffer

			encoder := msgpack.NewEncoder(&buf)

			err := tcsPredicate.EncodeMsgpack(encoder)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Get the encoded bytes.
			encodedBytes := buf.Bytes()

			// Use the golden file comparison function.
			compareGoldenMsgpackAndPrintDiff(t, tt.expected, encodedBytes)
		})
	}
}

func TestPredicate_EncodeMsgpack_UnknownOperator(t *testing.T) {
	t.Parallel()

	// Create a predicate with an unknown operator.
	// We need to create a custom predicate since the public API doesn't allow invalid operators.
	unknownOpPredicate := &struct {
		key    []byte
		op     goPredicate.Op
		target goPredicate.Target
		value  any
	}{
		key:    []byte("test-key"),
		op:     goPredicate.Op(999), // Invalid operator.
		target: goPredicate.TargetValue,
		value:  "test-value",
	}

	// Override the methods to return our custom values.
	// This is a bit hacky but necessary to test the error case.
	var buf bytes.Buffer

	encoder := msgpack.NewEncoder(&buf)

	// We'll test the error case by directly calling the encoding function
	// with a predicate that has invalid operator.
	tcsPredicate := predicate{
		Predicate: &mockPredicate{
			key:       unknownOpPredicate.key,
			operation: unknownOpPredicate.op,
			target:    unknownOpPredicate.target,
			value:     unknownOpPredicate.value,
		},
	}

	err := tcsPredicate.EncodeMsgpack(encoder)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownOperator)
}

func TestPredicate_EncodeMsgpack_UnknownTarget(t *testing.T) {
	t.Parallel()

	// Create a predicate with an unknown target.
	unknownTargetPredicate := &mockPredicate{
		key:       []byte("test-key"),
		operation: goPredicate.OpEqual,
		target:    goPredicate.Target(999), // Invalid target.
		value:     "test-value",
	}

	tcsPredicate := predicate{
		Predicate: unknownTargetPredicate,
	}

	var buf bytes.Buffer

	encoder := msgpack.NewEncoder(&buf)

	err := tcsPredicate.EncodeMsgpack(encoder)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownTarget)
}

// mockPredicate is a helper struct to test error cases
// by allowing us to set invalid operator and target values.
type mockPredicate struct {
	key       []byte
	operation goPredicate.Op
	target    goPredicate.Target
	value     any
}

func (m *mockPredicate) Key() []byte {
	return m.key
}

func (m *mockPredicate) Operation() goPredicate.Op {
	return m.operation
}

func (m *mockPredicate) Target() goPredicate.Target {
	return m.target
}

func (m *mockPredicate) Value() any {
	return m.value
}
