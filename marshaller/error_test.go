// Package marshaller provides types for marshalling operations.
package marshaller //nolint:testpackage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalError_Error(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("yaml marshal error")
	err := MarshalError{parent: parentErr}
	assert.Equal(t, "Failed to marshal: yaml marshal error", err.Error())
}

func TestMarshalError_Unwrap(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("yaml marshal error")
	err := MarshalError{parent: parentErr}
	assert.Equal(t, parentErr, err.Unwrap())
}

func Test_errMarshal(t *testing.T) {
	t.Parallel()

	t.Run("with parent error", func(t *testing.T) {
		t.Parallel()

		parentErr := errors.New("yaml marshal error")
		err := errMarshal(parentErr)
		require.Error(t, err)
		assert.Equal(t, "Failed to marshal: yaml marshal error", err.Error())

		var marshalErr MarshalError
		require.ErrorAs(t, err, &marshalErr)
		assert.Equal(t, parentErr, marshalErr.Unwrap())
	})

	t.Run("with nil parent error", func(t *testing.T) {
		t.Parallel()

		err := errMarshal(nil)
		require.NoError(t, err)
	})
}

func TestUnmarshalError_Error(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("yaml unmarshal error")
	err := UnmarshalError{parent: parentErr}
	assert.Equal(t, "Failed to unmarshal: yaml unmarshal error", err.Error())
}

func TestUnmarshalError_Unwrap(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("yaml unmarshal error")
	err := UnmarshalError{parent: parentErr}
	assert.Equal(t, parentErr, err.Unwrap())
}

func Test_errUnmarshal(t *testing.T) {
	t.Parallel()

	t.Run("with parent error", func(t *testing.T) {
		t.Parallel()

		parentErr := errors.New("yaml unmarshal error")
		err := errUnmarshal(parentErr)
		require.Error(t, err)
		assert.Equal(t, "Failed to unmarshal: yaml unmarshal error", err.Error())

		var unmarshalErr UnmarshalError
		require.ErrorAs(t, err, &unmarshalErr)
		assert.Equal(t, parentErr, unmarshalErr.Unwrap())
	})

	t.Run("with nil parent error", func(t *testing.T) {
		t.Parallel()

		err := errUnmarshal(nil)
		require.NoError(t, err)
	})
}

func TestErrorWrapping(t *testing.T) {
	t.Parallel()

	rootErr := errors.New("root cause")

	t.Run("MarshalError wraps parent", func(t *testing.T) {
		t.Parallel()

		marshalErr := MarshalError{parent: rootErr}
		require.ErrorIs(t, marshalErr, rootErr)
	})

	t.Run("UnmarshalError wraps parent", func(t *testing.T) {
		t.Parallel()

		unmarshalErr := UnmarshalError{parent: rootErr}
		require.ErrorIs(t, unmarshalErr, rootErr)
	})

	t.Run("errMarshal wraps parent", func(t *testing.T) {
		t.Parallel()

		err := errMarshal(rootErr)
		require.ErrorIs(t, err, rootErr)
	})

	t.Run("errUnmarshal wraps parent", func(t *testing.T) {
		t.Parallel()

		err := errUnmarshal(rootErr)
		require.ErrorIs(t, err, rootErr)
	})
}
