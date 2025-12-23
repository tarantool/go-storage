package integrity_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

type mockVerifier struct {
	name      string
	verifyErr error
}

func (m *mockVerifier) Name() string { return m.name }

func (m *mockVerifier) Verify(_ []byte, _ []byte) error {
	return m.verifyErr
}

type mockTypedMarshaller[T any] struct {
	unmarshalErr error
}

func (m *mockTypedMarshaller[T]) Marshal(_ T) ([]byte, error) {
	return []byte("marshalled"), nil
}

func (m *mockTypedMarshaller[T]) Unmarshal(_ []byte) (T, error) {
	var zero T
	return zero, m.unmarshalErr
}

func TestValidatorValidate_Success(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{"rsa"})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()

	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}
	verifiers := []crypto.Verifier{&mockVerifier{name: "rsa", verifyErr: nil}}
	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		verifiers,
	)

	var expectedModRevision int64 = 0x123

	// Create plain []KeyValue without generator.
	value := SimpleStruct{Name: "test", Value: 42}
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: expectedModRevision,
		},
		{
			Key: []byte("/test/hash/sha256/my-object"),
			Value: []byte{
				0x86, 0x1c, 0xdf, 0xcd, 0x76, 0x2f, 0x0a, 0x8c, 0xc0, 0xc7, 0xfc, 0x44, 0xcb, 0xfa, 0x5d, 0x29, 0xde,
				0xed, 0x36, 0xa2, 0x5c, 0x73, 0xf7, 0xa4, 0xc6, 0x7a, 0xd6, 0x37, 0xf7, 0x1b, 0xab, 0x39,
			},
			ModRevision: expectedModRevision,
		},
		{
			Key:         []byte("/test/sig/rsa/my-object"),
			Value:       []byte("mock-signature-rsa"),
			ModRevision: expectedModRevision,
		},
	}

	// Validate.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())
	assert.Equal(t, expectedModRevision, result.ModRevision)

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)
}
func TestValidatorValidate_MissingHash(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Create KVs with only value key, missing hash key.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		// Missing hash key: /test/hash/sha256/my-object.
	}

	// Should fail because sha256 hash is expected but missing.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "hash \"sha256\" not verified (missing)")
}

func TestValidatorValidate_HashMismatch(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Create KVs with corrupted hash value.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key:         []byte("/test/hash/sha256/my-object"),
			Value:       []byte("corrupted-hash"), // Wrong hash.
			ModRevision: 0,
		},
	}

	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "hash mismatch for \"sha256\"")
}

func TestValidatorValidate_MultipleObjects(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Create KVs for two objects.
	allKVs := []kv.KeyValue{
		// object1.
		{
			Key:         []byte("/test/object1"),
			Value:       []byte("name: test1\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key: []byte("/test/hash/sha256/object1"),
			Value: []byte{
				0xf3, 0x88, 0x82, 0x49, 0x59, 0x8f, 0xbf, 0x4e, 0xcd, 0x8a, 0x47, 0x7a, 0x6b, 0xc3, 0x83, 0xe9, 0xa8,
				0x8f, 0x6c, 0x13, 0xd7, 0x2a, 0x44, 0x86, 0xba, 0x6d, 0xe4, 0xf0, 0xbe, 0x7d, 0x18, 0xa9,
			},
			ModRevision: 0,
		},
		// object2.
		{
			Key:         []byte("/test/object2"),
			Value:       []byte("name: test2\nvalue: 100\n"),
			ModRevision: 0,
		},
		{
			Key: []byte("/test/hash/sha256/object2"),
			Value: []byte{
				0x1c, 0x47, 0x13, 0x01, 0xf9, 0x1b, 0x97, 0x9e, 0xa2, 0x92, 0x3e, 0xd2, 0x95, 0x67, 0x46, 0x6c, 0xad,
				0x09, 0x7d, 0xc6, 0x33, 0xb4, 0x10, 0xac, 0x9d, 0x88, 0xdb, 0xc8, 0xf2, 0xb2, 0x3f, 0x7b,
			},
			ModRevision: 0,
		},
	}

	validatedResults, err := validator.Validate(allKVs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 2)

	// Find results by name.
	var result1, result2 integrity.ValidatedResult[SimpleStruct]

	for _, res := range validatedResults {
		switch res.Name {
		case "object1":
			result1 = res
		case "object2":
			result2 = res
		}
	}

	assert.Equal(t, "object1", result1.Name)
	assert.True(t, result1.Value.IsSome())

	val1, ok := result1.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test1", Value: 42}, val1)
	require.NoError(t, result1.Error)

	assert.Equal(t, "object2", result2.Name)
	assert.True(t, result2.Value.IsSome())

	val2, ok := result2.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test2", Value: 100}, val2)
	require.NoError(t, result2.Error)
}

func TestValidatorValidate_PartialSuccess(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Create KVs for two objects, with object2 having corrupted hash.
	allKVs := []kv.KeyValue{
		// object1 - valid.
		{
			Key:         []byte("/test/object1"),
			Value:       []byte("name: test1\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key: []byte("/test/hash/sha256/object1"),
			Value: []byte{
				0xf3, 0x88, 0x82, 0x49, 0x59, 0x8f, 0xbf, 0x4e, 0xcd, 0x8a, 0x47, 0x7a, 0x6b, 0xc3, 0x83, 0xe9, 0xa8,
				0x8f, 0x6c, 0x13, 0xd7, 0x2a, 0x44, 0x86, 0xba, 0x6d, 0xe4, 0xf0, 0xbe, 0x7d, 0x18, 0xa9,
			},
			ModRevision: 0,
		},
		// object2 - corrupted hash.
		{
			Key:         []byte("/test/object2"),
			Value:       []byte("name: test2\nvalue: 100\n"),
			ModRevision: 0,
		},
		{
			Key:         []byte("/test/hash/sha256/object2"),
			Value:       []byte("corrupted-hash"), // Wrong hash.
			ModRevision: 0,
		},
	}

	validatedResults, err := validator.Validate(allKVs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 2)

	// Find results.
	var result1, result2 integrity.ValidatedResult[SimpleStruct]

	for _, res := range validatedResults {
		switch res.Name {
		case "object1":
			result1 = res
		case "object2":
			result2 = res
		}
	}

	// object1 should be valid.
	assert.Equal(t, "object1", result1.Name)
	assert.True(t, result1.Value.IsSome())

	val1, ok := result1.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test1", Value: 42}, val1)
	require.NoError(t, result1.Error)

	// object2 should have hash mismatch error.
	assert.Equal(t, "object2", result2.Name)
	assert.True(t, result2.Value.IsSome())

	val2, ok := result2.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test2", Value: 100}, val2)
	require.ErrorAs(t, result2.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result2.Error, "hash mismatch for \"sha256\"")
}

func TestValidatorValidate_MissingSignature(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{}, []string{"rsa"})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	verifiers := []crypto.Verifier{&mockVerifier{name: "rsa", verifyErr: nil}}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		nil,
		verifiers,
	)

	// Create KVs with only value key, missing signature key.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		// Missing signature key: /test/sig/rsa/my-object .
	}

	// Should fail because rsa signature is expected but missing.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "signature \"rsa\" not verified (missing)")
}

func TestValidatorValidate_SignatureVerificationError(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{}, []string{"rsa"})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()

	// Create verifier that returns error.
	verifiers := []crypto.Verifier{&mockVerifier{name: "rsa", verifyErr: assert.AnError}}
	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		nil,
		verifiers,
	)

	// Create KVs with value and signature.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key:         []byte("/test/sig/rsa/my-object"),
			Value:       []byte("mock-signature-rsa"),
			ModRevision: 0,
		},
	}

	// Should fail because signature verification returns error.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "signature verification failed for \"rsa\"")
}

func TestValidatorValidate_HashComputationError(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()

	// Validate with failing hasher.
	failingHashers := []hasher.Hasher{newMockHasherWithError("sha256", "hash computation failed")}
	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		failingHashers,
		nil,
	)

	// Create KVs with value and hash.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key: []byte("/test/hash/sha256/my-object"),
			Value: []byte{
				0x86, 0x1c, 0xdf, 0xcd, 0x76, 0x2f, 0x0a, 0x8c, 0xc0, 0xc7, 0xfc, 0x44, 0xcb, 0xfa, 0x5d, 0x29, 0xde,
				0xed, 0x36, 0xa2, 0x5c, 0x73, 0xf7, 0xa4, 0xc6, 0x7a, 0xd6, 0x37, 0xf7, 0x1b, 0xab, 0x39,
			},
			ModRevision: 0,
		},
	}

	// Should fail because hash computation returns error.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "failed to calculate hash \"sha256\"")
}

func TestValidatorValidate_EmptyKVs(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Empty KV list should return empty result.
	validatedResults, err := validator.Validate([]kv.KeyValue{})
	require.NoError(t, err)
	assert.Empty(t, validatedResults)
}

func TestValidatorValidate_HasherNotAvailable(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256", "sha1"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()

	// Validator only has sha256 hasher, not sha1.
	validatorHashers := []hasher.Hasher{hasher.NewSHA256Hasher()}
	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		validatorHashers,
		nil,
	)

	// Create KVs with both sha256 and sha1 hashes.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key: []byte("/test/hash/sha256/my-object"),
			Value: []byte{
				0x86, 0x1c, 0xdf, 0xcd, 0x76, 0x2f, 0x0a, 0x8c, 0xc0, 0xc7, 0xfc, 0x44, 0xcb, 0xfa, 0x5d, 0x29, 0xde,
				0xed, 0x36, 0xa2, 0x5c, 0x73, 0xf7, 0xa4, 0xc6, 0x7a, 0xd6, 0x37, 0xf7, 0x1b, 0xab, 0x39,
			},
			ModRevision: 0,
		},
		{
			Key:         []byte("/test/hash/sha1/my-object"),
			Value:       []byte("mock-sha1-hash"),
			ModRevision: 0,
		},
	}

	// Should fail because sha1 hasher is not available.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.NoError(t, result.Error)
}

func TestValidatorValidate_VerifierNotAvailable(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{}, []string{"rsa", "ecdsa"})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()

	// Validator only has rsa verifier, not ecdsa.
	verifiers := []crypto.Verifier{&mockVerifier{name: "rsa", verifyErr: nil}}
	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		nil,
		verifiers,
	)

	// Create KVs with both rsa and ecdsa signatures.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key:         []byte("/test/sig/rsa/my-object"),
			Value:       []byte("mock-signature-rsa"),
			ModRevision: 0,
		},
		{
			Key:         []byte("/test/sig/ecdsa/my-object"),
			Value:       []byte("mock-signature-ecdsa"),
			ModRevision: 0,
		},
	}

	// Should fail because ecdsa verifier is not available.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.NoError(t, result.Error)
}

func TestValidatorValidate_InvalidKeyParsing(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Create KVs with invalid keys that can't be parsed.
	invalidKVs := []kv.KeyValue{
		{Key: []byte("invalid/key/format"), Value: []byte("some-value"), ModRevision: 0},
	}

	// Should fail with parse error.
	validatedResults, err := validator.Validate(invalidKVs)
	require.ErrorAs(t, err, &integrity.ValidationError{})
	assert.Nil(t, validatedResults)
}

func TestValidatorValidate_MissingValueKey(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Create KVs with only hash key, no value key.
	missingValueKVs := []kv.KeyValue{
		{Key: []byte("/test/hash/sha256/my-object"), Value: []byte("some-hash"), ModRevision: 0},
	}

	// Should fail because value key is missing.
	validatedResults, err := validator.Validate(missingValueKVs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsZero())
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	// When there's no value key, the validator still tries to compute hashes on nil data.
	require.ErrorContains(t, result.Error, "failed to calculate hash")
}

func TestValidatorValidate_UnmarshalError(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})

	// Create a mock marshaller that returns error.
	mockMarshaller := &mockTypedMarshaller[SimpleStruct]{
		unmarshalErr: errors.New("invalid yaml format"),
	}

	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	// Create validator with mock marshaller that fails unmarshal.
	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		mockMarshaller,
		hashers,
		nil,
	)

	// Create KVs with value and hash.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key: []byte("/test/hash/sha256/my-object"),
			Value: []byte{
				0x86, 0x1c, 0xdf, 0xcd, 0x76, 0x2f, 0x0a, 0x8c, 0xc0, 0xc7, 0xfc, 0x44, 0xcb, 0xfa, 0x5d, 0x29, 0xde,
				0xed, 0x36, 0xa2, 0x5c, 0x73, 0xf7, 0xa4, 0xc6, 0x7a, 0xd6, 0x37, 0xf7, 0x1b, 0xab, 0x39,
			},
			ModRevision: 0,
		},
	}

	// Should fail because unmarshal returns error.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsZero())
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "failed to unmarshal record")
}

func TestValidatorValidate_HashKeyNotFound(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{"sha256", "sha1"}, []string{})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher(), hasher.NewSHA1Hasher()}

	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		hashers,
		nil,
	)

	// Create KVs with sha256 hash but missing sha1 hash.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key: []byte("/test/hash/sha256/my-object"),
			Value: []byte{
				0x86, 0x1c, 0xdf, 0xcd, 0x76, 0x2f, 0x0a, 0x8c, 0xc0, 0xc7, 0xfc, 0x44, 0xcb, 0xfa, 0x5d, 0x29, 0xde,
				0xed, 0x36, 0xa2, 0x5c, 0x73, 0xf7, 0xa4, 0xc6, 0x7a, 0xd6, 0x37, 0xf7, 0x1b, 0xab, 0x39,
			},
			ModRevision: 0,
		},
		// Missing sha1 hash key: /test/hash/sha1/my-object .
	}

	// Should fail because sha1 hash key is missing.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "hash \"sha1\" not verified (missing)")
}

func TestValidatorValidate_SignatureKeyNotFound(t *testing.T) {
	t.Parallel()

	namerInstance := namer.NewDefaultNamer("test", []string{}, []string{"rsa", "ecdsa"})
	marshallerInstance := marshaller.NewTypedYamlMarshaller[SimpleStruct]()

	// Validator has both verifiers.
	verifiers := []crypto.Verifier{
		&mockVerifier{name: "rsa", verifyErr: nil},
		&mockVerifier{name: "ecdsa", verifyErr: nil},
	}
	validator := integrity.NewValidator[SimpleStruct](
		namerInstance,
		marshallerInstance,
		nil,
		verifiers,
	)

	// Create KVs with rsa signature but missing ecdsa signature.
	kvs := []kv.KeyValue{
		{
			Key:         []byte("/test/my-object"),
			Value:       []byte("name: test\nvalue: 42\n"),
			ModRevision: 0,
		},
		{
			Key:         []byte("/test/sig/rsa/my-object"),
			Value:       []byte("mock-signature-rsa"),
			ModRevision: 0,
		},
		// Missing ecdsa signature key: /test/sig/ecdsa/my-object .
	}

	// Should fail because ecdsa signature key is missing.
	validatedResults, err := validator.Validate(kvs)
	require.NoError(t, err)
	require.Len(t, validatedResults, 1)

	result := validatedResults[0]
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, SimpleStruct{Name: "test", Value: 42}, val)
	require.ErrorAs(t, result.Error, &integrity.ValidationError{})
	require.ErrorContains(t, result.Error, "signature \"ecdsa\" not verified (missing)")
}
