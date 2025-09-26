package tcs_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/driver/tcs"
)

func TestEncodingError_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		error    tcs.EncodingError
		expected string
	}{
		{
			name: "operation encoding error with text and error",
			error: tcs.EncodingError{
				ObjectType: "operation",
				Text:       "test operation",
				Err:        errors.New("encoding failed"),
			},
			expected: "failed to encode operation, test operation: encoding failed",
		},
		{
			name: "predicate encoding error with text and error",
			error: tcs.EncodingError{
				ObjectType: "predicate",
				Text:       "test predicate",
				Err:        errors.New("marshal error"),
			},
			expected: "failed to encode predicate, test predicate: marshal error",
		},
		{
			name: "custom object type encoding error",
			error: tcs.EncodingError{
				ObjectType: "custom",
				Text:       "custom object",
				Err:        errors.New("custom error"),
			},
			expected: "failed to encode custom, custom object: custom error",
		},
		{
			name: "encoding error with empty text",
			error: tcs.EncodingError{
				ObjectType: "operation",
				Text:       "",
				Err:        errors.New("error"),
			},
			expected: "failed to encode operation: error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.error.Error()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEncodingError_Unwrap(t *testing.T) {
	t.Parallel()

	t.Run("some", func(t *testing.T) {
		t.Parallel()

		innerErr := errors.New("inner error")
		decodingErr := tcs.EncodingError{
			ObjectType: "operation",
			Text:       "test operation",
			Err:        innerErr,
		}

		result := decodingErr.Unwrap()
		assert.Equal(t, innerErr, result)
	})

	// Shouldn't be possible, but let's test it anyway.
	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		decodingErr := tcs.DecodingError{
			ObjectType: "operation",
			Text:       "test operation",
			Err:        nil,
		}

		err := decodingErr.Unwrap()
		assert.NoError(t, err)
	})
}

func TestNewOperationEncodingError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		text     string
		err      error
		expected error
	}{
		{
			name: "operation encoding error with text and error",
			text: "test operation",
			err:  errors.New("encoding failed"),
			expected: tcs.EncodingError{
				ObjectType: "operation",
				Text:       "test operation",
				Err:        errors.New("encoding failed"),
			},
		},
		{
			name: "operation encoding error with empty text",
			text: "",
			err:  errors.New("error"),
			expected: tcs.EncodingError{
				ObjectType: "operation",
				Text:       "",
				Err:        errors.New("error"),
			},
		},
		{
			name:     "operation encoding error with nil error",
			text:     "test",
			err:      nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tcs.NewOperationEncodingError(tt.text, tt.err)

			if tt.expected == nil {
				assert.NoError(t, err)
				return
			}

			require.Error(t, err)

			var encodingErr tcs.EncodingError

			{
				ok := errors.As(err, &encodingErr)
				require.True(t, ok, "Expected result to be of type tcs.EncodingError")
			}

			var expectedEncodingErr tcs.EncodingError

			{
				ok := errors.As(tt.expected, &expectedEncodingErr)
				require.True(t, ok, "Expected tt.expected to be of type tcs.EncodingError")
			}

			assert.Equal(t, expectedEncodingErr.ObjectType, encodingErr.ObjectType)
			assert.Equal(t, expectedEncodingErr.Text, encodingErr.Text)

			assert.Equal(t, expectedEncodingErr.Err.Error(), encodingErr.Err.Error())
		})
	}
}

func TestNewPredicateEncodingError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		text     string
		err      error
		expected error
	}{
		{
			name: "predicate encoding error with text and error",
			text: "test predicate",
			err:  errors.New("marshal error"),
			expected: tcs.EncodingError{
				ObjectType: "predicate",
				Text:       "test predicate",
				Err:        errors.New("marshal error"),
			},
		},
		{
			name: "predicate encoding error with empty text",
			text: "",
			err:  errors.New("error"),
			expected: tcs.EncodingError{
				ObjectType: "predicate",
				Text:       "",
				Err:        errors.New("error"),
			},
		},
		{
			name:     "predicate encoding error with nil error",
			text:     "test",
			err:      nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tcs.NewPredicateEncodingError(tt.text, tt.err)

			if tt.expected == nil {
				assert.NoError(t, err)
				return
			}

			require.Error(t, err)

			var encodingErr tcs.EncodingError
			{
				ok := errors.As(err, &encodingErr)
				require.True(t, ok, "Expected result to be of type tcs.EncodingError")
			}

			var expectedEncodingErr tcs.EncodingError
			{
				ok := errors.As(tt.expected, &expectedEncodingErr)
				require.True(t, ok, "Expected tt.expected to be of type tcs.EncodingError")
			}

			assert.Equal(t, expectedEncodingErr.ObjectType, encodingErr.ObjectType)
			assert.Equal(t, expectedEncodingErr.Text, encodingErr.Text)

			assert.Equal(t, expectedEncodingErr.Err.Error(), encodingErr.Err.Error())
		})
	}
}

func TestDecodingError_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		error    tcs.DecodingError
		expected string
	}{
		{
			name: "decoding error with object type and error",
			error: tcs.DecodingError{
				ObjectType: "txnOpResponse",
				Text:       "",
				Err:        errors.New("decoding failed"),
			},
			expected: "failed to decode txnOpResponse: decoding failed",
		},
		{
			name: "decoding error with object type, text and error",
			error: tcs.DecodingError{
				ObjectType: "txnOpResponse",
				Text:       "response data",
				Err:        errors.New("invalid format"),
			},
			expected: "failed to decode txnOpResponse, response data: invalid format",
		},
		{
			name: "decoding error with custom object type",
			error: tcs.DecodingError{
				ObjectType: "customObject",
				Text:       "custom data",
				Err:        errors.New("custom error"),
			},
			expected: "failed to decode customObject, custom data: custom error",
		},
		{
			name: "decoding error with empty text",
			error: tcs.DecodingError{
				ObjectType: "txnOpResponse",
				Text:       "",
				Err:        errors.New("error"),
			},
			expected: "failed to decode txnOpResponse: error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.error.Error()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodingError_Unwrap(t *testing.T) {
	t.Parallel()

	t.Run("some", func(t *testing.T) {
		t.Parallel()

		innerErr := errors.New("inner error")
		decodingErr := tcs.DecodingError{
			ObjectType: "txnOpResponse",
			Text:       "test",
			Err:        innerErr,
		}

		result := decodingErr.Unwrap()
		assert.Equal(t, innerErr, result)
	})

	// Shouldn't be possible, but let's test it anyway.
	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		decodingErr := tcs.DecodingError{
			ObjectType: "txnOpResponse",
			Text:       "test",
			Err:        nil,
		}

		assert.NoError(t, decodingErr.Unwrap())
	})
}

func TestNewTxnOpResponseDecodingError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected error
	}{
		{
			name: "txnOpResponse decoding error with error",
			err:  errors.New("decoding failed"),
			expected: tcs.DecodingError{
				ObjectType: "txnOpResponse",
				Text:       "",
				Err:        errors.New("decoding failed"),
			},
		},
		{
			name:     "txnOpResponse decoding error with nil error",
			err:      nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tcs.NewTxnOpResponseDecodingError(tt.err)

			if tt.expected == nil {
				assert.NoError(t, err)
				return
			}

			require.Error(t, err)

			var decodingErr tcs.DecodingError
			{
				ok := errors.As(err, &decodingErr)
				require.True(t, ok, "Expected result to be of type tcs.DecodingError")
			}

			var expectedDecodingErr tcs.DecodingError
			{
				ok := errors.As(tt.expected, &expectedDecodingErr)
				require.True(t, ok, "Expected tt.expected to be of type tcs.DecodingError")
			}

			assert.Equal(t, expectedDecodingErr.ObjectType, decodingErr.ObjectType)
			assert.Equal(t, expectedDecodingErr.Text, decodingErr.Text)
			assert.Equal(t, expectedDecodingErr.Err.Error(), decodingErr.Err.Error())
		})
	}
}
