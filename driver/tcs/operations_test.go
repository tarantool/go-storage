//nolint:testpackage
package tcs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	goOperation "github.com/tarantool/go-storage/operation"
)

func logMsgpackAsJSONConvert(t *testing.T, data []byte) {
	t.Helper()

	var decodedMsgpack map[int]any

	decoder := msgpack.NewDecoder(bytes.NewReader(data))
	require.NoError(t, decoder.Decode(&decodedMsgpack))

	decodedConvertedMsgpack := map[string]any{}
	for k, v := range decodedMsgpack {
		decodedConvertedMsgpack[fmt.Sprintf("%s[%d]", iproto.Key(k).String(), k)] = v
	}

	encodedJSON, err := json.MarshalIndent(decodedConvertedMsgpack, "", "  ")
	require.NoError(t, err, "failed to convert msgpack to json")

	for line := range bytes.SplitSeq(encodedJSON, []byte("\n")) {
		t.Log(string(line))
	}
}

func compareGoldenMsgpackAndPrintDiff(t *testing.T, expected []byte, got []byte) {
	t.Helper()

	if assert.Equal(t, expected, got, "golden file content is not equal to actual") {
		return
	}

	t.Logf("expected:\n")
	logMsgpackAsJSONConvert(t, expected)
	t.Logf("actual:\n")
	logMsgpackAsJSONConvert(t, got)
}

func TestOperation_EncodeMsgpack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		operation   goOperation.Operation
		expectError bool
		expected    []byte
	}{
		{
			name:        "put operation with key and value",
			operation:   goOperation.Put([]byte("test-key"), []byte("test-value")),
			expectError: false,
			expected: []byte{
				0x93, 0xa3, 0x70, 0x75, 0x74, 0xa8, 0x74, 0x65, 0x73, 0x74,
				0x2d, 0x6b, 0x65, 0x79, 0xaa, 0x74, 0x65, 0x73, 0x74, 0x2d,
				0x76, 0x61, 0x6c, 0x75, 0x65,
			},
		},
		{
			name:        "get operation with key",
			operation:   goOperation.Get([]byte("test-key")),
			expectError: false,
			expected: []byte{
				0x92, 0xa3, 0x67, 0x65, 0x74, 0xa8, 0x74, 0x65, 0x73, 0x74,
				0x2d, 0x6b, 0x65, 0x79,
			},
		},
		{
			name:        "delete operation with key",
			operation:   goOperation.Delete([]byte("test-key")),
			expectError: false,
			expected: []byte{
				0x92, 0xa6, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0xa8, 0x74,
				0x65, 0x73, 0x74, 0x2d, 0x6b, 0x65, 0x79,
			},
		},
		{
			name:        "put operation with empty key and value",
			operation:   goOperation.Put([]byte(""), []byte("")),
			expectError: false,
			expected:    []byte{0x93, 0xa3, 0x70, 0x75, 0x74, 0xa0, 0xa0},
		},
		{
			name:        "get operation with empty key",
			operation:   goOperation.Get([]byte("")),
			expectError: false,
			expected:    []byte{0x92, 0xa3, 0x67, 0x65, 0x74, 0xa0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create the TCS operation wrapper.
			tcsOp := operation{Operation: tt.operation}

			// Encode the operation.
			var buf bytes.Buffer

			encoder := msgpack.NewEncoder(&buf)

			err := tcsOp.EncodeMsgpack(encoder)

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
