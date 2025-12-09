// Package integrity provides integrity storage.
package integrity //nolint:testpackage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImpossibleError_Error(t *testing.T) {
	t.Parallel()

	err := ImpossibleError{text: "test error"}
	assert.Equal(t, "test error", err.Error())
}

func Test_errImpossibleHasher(t *testing.T) {
	t.Parallel()

	err := errImpossibleHasher("sha256")
	assert.Equal(t, "hasher not found: sha256", err.Error())

	var impossibleErr ImpossibleError
	require.ErrorAs(t, err, &impossibleErr)
}

func Test_errImpossibleSigner(t *testing.T) {
	t.Parallel()

	err := errImpossibleSigner("rsa")
	assert.Equal(t, "signer not found: rsa", err.Error())

	var impossibleErr ImpossibleError
	require.ErrorAs(t, err, &impossibleErr)
}

func Test_errImpossibleKeyType(t *testing.T) {
	t.Parallel()

	err := errImpossibleKeyType("unknown")
	assert.Equal(t, "unknown key type: unknown", err.Error())

	var impossibleErr ImpossibleError
	require.ErrorAs(t, err, &impossibleErr)
}

func TestValidationError_Error(t *testing.T) {
	t.Parallel()

	// Test with parent error.
	parentErr := errors.New("parent error")
	err := ValidationError{
		text:   "validation failed",
		parent: parentErr,
	}
	assert.Equal(t, "validation failed: parent error", err.Error())

	// Test without parent error.
	err = ValidationError{
		text:   "validation failed",
		parent: nil,
	}
	assert.Equal(t, "validation failed", err.Error())
}

func TestValidationError_Unpack(t *testing.T) {
	t.Parallel()

	// Test with parent error.
	parentErr := errors.New("original error")
	validationErr := ValidationError{
		text:   "validation failed",
		parent: parentErr,
	}
	assert.Equal(t, parentErr, validationErr.Unpack())

	// Test with nil parent.
	validationErr = ValidationError{
		text:   "validation failed",
		parent: nil,
	}
	require.NoError(t, validationErr.Unpack())
}

func TestFailedToGenerateKeysError_Error(t *testing.T) {
	t.Parallel()

	// Test with parent error.
	parentErr := errors.New("namer error")
	err := FailedToGenerateKeysError{parent: parentErr}
	assert.Equal(t, "failed to generate keys: namer error", err.Error())

	// Test without parent error.
	err = FailedToGenerateKeysError{parent: nil}
	assert.Equal(t, "failed to generate keys", err.Error())
}

func TestFailedToGenerateKeysError_Unwrap(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("namer error")
	err := FailedToGenerateKeysError{parent: parentErr}
	assert.Equal(t, parentErr, err.Unwrap())

	err = FailedToGenerateKeysError{parent: nil}
	require.NoError(t, err.Unwrap())
}

func Test_errFailedToGenerateKeys(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("namer error")
	err := errFailedToGenerateKeys(parentErr)
	assert.Equal(t, "failed to generate keys: namer error", err.Error())

	var failedErr FailedToGenerateKeysError
	require.ErrorAs(t, err, &failedErr)
	assert.Equal(t, parentErr, failedErr.Unwrap())

	// Test without parent error.
	err = errFailedToGenerateKeys(nil)
	assert.Equal(t, "failed to generate keys", err.Error())
}

func TestFailedToMarshalValueError_Error(t *testing.T) {
	t.Parallel()

	// Test with parent error.
	parentErr := errors.New("yaml marshal error")
	err := FailedToMarshalValueError{parent: parentErr}
	assert.Equal(t, "failed to marshal value: yaml marshal error", err.Error())

	// Test without parent error.
	err = FailedToMarshalValueError{parent: nil}
	assert.Equal(t, "failed to marshal value", err.Error())
}

func TestFailedToMarshalValueError_Unwrap(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("yaml marshal error")
	err := FailedToMarshalValueError{parent: parentErr}
	assert.Equal(t, parentErr, err.Unwrap())

	err = FailedToMarshalValueError{parent: nil}
	require.NoError(t, err.Unwrap())
}

func Test_errFailedToMarshalValue(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("yaml marshal error")
	err := errFailedToMarshalValue(parentErr)
	assert.Equal(t, "failed to marshal value: yaml marshal error", err.Error())

	var failedErr FailedToMarshalValueError
	require.ErrorAs(t, err, &failedErr)
	assert.Equal(t, parentErr, failedErr.Unwrap())

	// Test without parent error.
	err = errFailedToMarshalValue(nil)
	assert.Equal(t, "failed to marshal value", err.Error())
}

func TestFailedToComputeHashError_Error(t *testing.T) {
	t.Parallel()

	// Test with parent error.
	parentErr := errors.New("hash computation failed")
	err := FailedToComputeHashError{parent: parentErr}
	assert.Equal(t, "failed to compute hash: hash computation failed", err.Error())

	// Test without parent error.
	err = FailedToComputeHashError{parent: nil}
	assert.Equal(t, "failed to compute hash", err.Error())
}

func TestFailedToComputeHashError_Unwrap(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("hash computation failed")
	err := FailedToComputeHashError{parent: parentErr}
	assert.Equal(t, parentErr, err.Unwrap())

	err = FailedToComputeHashError{parent: nil}
	require.NoError(t, err.Unwrap())
}

func Test_errFailedToComputeHash(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("hash computation failed")
	err := errFailedToComputeHash(parentErr)
	assert.Equal(t, "failed to compute hash: hash computation failed", err.Error())

	var failedErr FailedToComputeHashError
	require.ErrorAs(t, err, &failedErr)
	assert.Equal(t, parentErr, failedErr.Unwrap())

	// Test without parent error.
	err = errFailedToComputeHash(nil)
	assert.Equal(t, "failed to compute hash", err.Error())
}

func TestFailedToGenerateSignatureError_Error(t *testing.T) {
	t.Parallel()

	// Test with parent error.
	parentErr := errors.New("signature generation failed")
	err := FailedToGenerateSignatureError{parent: parentErr}
	assert.Equal(t, "failed to generate signature: signature generation failed", err.Error())

	// Test without parent error.
	err = FailedToGenerateSignatureError{parent: nil}
	assert.Equal(t, "failed to generate signature", err.Error())
}

func TestFailedToGenerateSignatureError_Unwrap(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("signature generation failed")
	err := FailedToGenerateSignatureError{parent: parentErr}
	assert.Equal(t, parentErr, err.Unwrap())

	err = FailedToGenerateSignatureError{parent: nil}
	require.NoError(t, err.Unwrap())
}

func Test_errFailedToGenerateSignature(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("signature generation failed")
	err := errFailedToGenerateSignature(parentErr)
	assert.Equal(t, "failed to generate signature: signature generation failed", err.Error())

	var failedErr FailedToGenerateSignatureError
	require.ErrorAs(t, err, &failedErr)
	assert.Equal(t, parentErr, failedErr.Unwrap())

	// Test without parent error.
	err = errFailedToGenerateSignature(nil)
	assert.Equal(t, "failed to generate signature", err.Error())
}

func Test_errHashNotVerifiedMissing(t *testing.T) {
	t.Parallel()

	err := errHashNotVerifiedMissing("sha1")
	assert.Equal(t, "hash \"sha1\" not verified (missing)", err.Error())

	var validationErr ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func Test_errSignatureNotVerifiedMissing(t *testing.T) {
	t.Parallel()

	err := errSignatureNotVerifiedMissing("ecdsa")
	assert.Equal(t, "signature \"ecdsa\" not verified (missing)", err.Error())

	var validationErr ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func Test_hashMismatchDetailError_Error(t *testing.T) {
	t.Parallel()

	err := hashMismatchDetailError{
		expected: []byte("expected"),
		got:      []byte("got"),
	}
	// Note: hex.Dump adds newlines and formatting.
	require.ErrorContains(t, err, "expected")
	require.ErrorContains(t, err, "got")
}

func Test_errHashMismatch(t *testing.T) {
	t.Parallel()

	err := errHashMismatch("sha256", []byte("expected"), []byte("got"))
	require.ErrorContains(t, err, "hash mismatch for \"sha256\"")
	require.ErrorContains(t, err, "expected")
	require.ErrorContains(t, err, "got")

	var validationErr ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func Test_errFailedToParseKey(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("invalid key")
	err := errFailedToParseKey(parentErr)
	assert.Equal(t, "failed to parse key: invalid key", err.Error())

	var validationErr ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func Test_errFailedToComputeHashWith(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("hash computation failed")
	err := errFailedToComputeHashWith("sha256", parentErr)
	require.ErrorContains(t, err, "failed to calculate hash \"sha256\": hash computation failed")

	var validationErr ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func Test_errSignatureVerificationFailed(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("invalid signature")
	err := errSignatureVerificationFailed("rsa", parentErr)
	assert.Equal(t, "signature verification failed for \"rsa\": invalid signature", err.Error())

	var validationErr ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func Test_errFailedToUnmarshal(t *testing.T) {
	t.Parallel()

	parentErr := errors.New("invalid yaml")
	err := errFailedToUnmarshal(parentErr)
	assert.Equal(t, "failed to unmarshal record: invalid yaml", err.Error())

	var validationErr ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func TestFailedToValidateAggregatedError_Unwrap(t *testing.T) {
	t.Parallel()

	t.Run("nil parent", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: nil}
		assert.Empty(t, aggErr.Unwrap())
	})

	t.Run("multiple parents", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: []error{errors.New("error1"), errors.New("error2")}}

		errs := aggErr.Unwrap()
		assert.Len(t, errs, 2)
		assert.Equal(t, "error1", errs[0].Error())
		assert.Equal(t, "error2", errs[1].Error())
	})
}

func TestFailedToValidateAggregatedError_Append(t *testing.T) {
	t.Parallel()

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: nil}

		aggErr.Append(nil)
		assert.Empty(t, aggErr.Unwrap())
	})

	t.Run("one error", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: nil}

		aggErr.Append(errors.New("first error"))
		assert.Len(t, aggErr.Unwrap(), 1)
		assert.Equal(t, "first error", aggErr.Unwrap()[0].Error())
	})

	t.Run("two errors", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: nil}

		aggErr.Append(errors.New("first error"))
		aggErr.Append(errors.New("second error"))
		assert.Len(t, aggErr.Unwrap(), 2)
		assert.Equal(t, "first error", aggErr.Unwrap()[0].Error())
		assert.Equal(t, "second error", aggErr.Unwrap()[1].Error())
	})
}

func TestFailedToValidateAggregatedError_Error(t *testing.T) {
	t.Parallel()

	t.Run("zero errors", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: nil}
		assert.Empty(t, aggErr.Error())
	})

	t.Run("single error", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: []error{errors.New("single error")}}
		assert.Equal(t, "single error", aggErr.Error())
	})

	t.Run("multiple errors", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: []error{errors.New("error1"), errors.New("error2")}}
		assert.Equal(t, "aggregated error: error1, error2", aggErr.Error())
	})
}

func TestFailedToValidateAggregatedError_Finalize(t *testing.T) {
	t.Parallel()

	t.Run("zero errors", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: nil}
		require.NoError(t, aggErr.Finalize())
	})

	t.Run("single error", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: []error{errors.New("single error")}}

		finalizedErr := aggErr.Finalize()
		assert.Equal(t, "single error", finalizedErr.Error())
	})

	t.Run("multiple errors", func(t *testing.T) {
		t.Parallel()

		aggErr := FailedToValidateAggregatedError{parent: []error{errors.New("error1"), errors.New("error2")}}
		finalizedErr := aggErr.Finalize()
		assert.Equal(t, "aggregated error: error1, error2", finalizedErr.Error())
	})
}

func TestErrorWrapping(t *testing.T) {
	t.Parallel()

	rootErr := errors.New("root cause")

	t.Run("errFailedToGenerateKeys", func(t *testing.T) {
		t.Parallel()

		genErr := errFailedToGenerateKeys(rootErr)
		require.ErrorIs(t, genErr, rootErr)
	})

	t.Run("errFailedToMarshalValue", func(t *testing.T) {
		t.Parallel()

		marshalErr := errFailedToMarshalValue(rootErr)
		require.ErrorIs(t, marshalErr, rootErr)
	})

	t.Run("errFailedToComputeHash", func(t *testing.T) {
		t.Parallel()

		hashErr := errFailedToComputeHash(rootErr)
		require.ErrorIs(t, hashErr, rootErr)
	})

	t.Run("errFailedToGenerateSignature", func(t *testing.T) {
		t.Parallel()

		sigErr := errFailedToGenerateSignature(rootErr)
		require.ErrorIs(t, sigErr, rootErr)
	})
}
