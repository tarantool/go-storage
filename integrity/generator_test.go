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

// Test structures.
type SimpleStruct struct {
	Name  string `yaml:"name"`
	Value int    `yaml:"value"`
}

// Mock implementations for hashers and signers only.
type mockHasher struct {
	name      string
	hashFunc  func(data []byte) ([]byte, error)
	shouldErr bool
	errMsg    string
}

func (m *mockHasher) Name() string { return m.name }

func (m *mockHasher) Hash(data []byte) ([]byte, error) {
	if m.shouldErr {
		return nil, errors.New(m.errMsg)
	}

	if m.hashFunc != nil {
		return m.hashFunc(data)
	}

	return []byte("mock-hash-" + m.name), nil
}

type mockSigner struct {
	name      string
	signFunc  func(data []byte) ([]byte, error)
	shouldErr bool
	errMsg    string
}

func (m *mockSigner) Name() string { return m.name }

func (m *mockSigner) Sign(data []byte) ([]byte, error) {
	if m.shouldErr {
		return nil, errors.New(m.errMsg)
	}

	if m.signFunc != nil {
		return m.signFunc(data)
	}

	return []byte("mock-signature-" + m.name), nil
}

type mockFailingTypedMarshaller[T any] struct {
	marshalErr   error
	unmarshalErr error
}

func (m *mockFailingTypedMarshaller[T]) Marshal(data T) ([]byte, error) {
	if m.marshalErr != nil {
		return nil, m.marshalErr
	}

	return []byte("marshalled"), nil
}

func (m *mockFailingTypedMarshaller[T]) Unmarshal(data []byte) (T, error) {
	var zero T
	return zero, m.unmarshalErr
}

// Helper functions for creating mock instances.
func newMockHasher(name string) *mockHasher {
	return &mockHasher{
		name:      name,
		hashFunc:  nil,
		shouldErr: false,
		errMsg:    "",
	}
}

func newMockHasherWithError(name, errMsg string) *mockHasher {
	return &mockHasher{
		name:      name,
		hashFunc:  nil,
		shouldErr: true,
		errMsg:    errMsg,
	}
}

func newMockSigner(name string) *mockSigner {
	return &mockSigner{
		name:      name,
		signFunc:  nil,
		shouldErr: false,
		errMsg:    "",
	}
}

func newMockSignerWithError(name, errMsg string) *mockSigner {
	return &mockSigner{
		name:      name,
		signFunc:  nil,
		shouldErr: true,
		errMsg:    errMsg,
	}
}

func TestGeneratorGenerate_SuccessSimpleValue(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{}, []string{}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}
	pairs, err := generator.Generate("my-object", value)

	require.NoError(t, err)
	require.Len(t, pairs, 1)

	assert.Equal(t, "/test/my-object", string(pairs[0].Key))
	assert.Contains(t, string(pairs[0].Value), "name: test")
	assert.Contains(t, string(pairs[0].Value), "value: 42")
}

func TestGeneratorGenerate_SuccessWithHashes(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{"sha256", "md5"}, []string{}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{
			newMockHasher("sha256"),
			newMockHasher("md5"),
		},
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}
	pairs, err := generator.Generate("my-object", value)

	require.NoError(t, err)
	require.Len(t, pairs, 3)

	// Find value pair.
	var valuePair kv.KeyValue

	for _, pair := range pairs {
		if string(pair.Key) == "/test/my-object" {
			valuePair = pair
			break
		}
	}

	require.NotEmpty(t, valuePair.Key)
	assert.Contains(t, string(valuePair.Value), "name: test")
	assert.Contains(t, string(valuePair.Value), "value: 42")

	// Check hash pairs.
	hashPairs := 0

	for _, pair := range pairs {
		if string(pair.Key) == "/test/hash/sha256/my-object" {
			hashPairs++

			assert.Equal(t, "mock-hash-sha256", string(pair.Value))
		}

		if string(pair.Key) == "/test/hash/md5/my-object" {
			hashPairs++

			assert.Equal(t, "mock-hash-md5", string(pair.Value))
		}
	}

	assert.Equal(t, 2, hashPairs)
}

func TestGeneratorGenerate_SuccessWithSignatures(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{}, []string{"rsa", "ecdsa"}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{
			newMockSigner("rsa"),
			newMockSigner("ecdsa"),
		},
	)

	value := SimpleStruct{Name: "test", Value: 42}
	pairs, err := generator.Generate("my-object", value)

	require.NoError(t, err)
	require.Len(t, pairs, 3)

	var valuePair kv.KeyValue

	for _, pair := range pairs {
		if string(pair.Key) == "/test/my-object" {
			valuePair = pair
			break
		}
	}

	require.NotEmpty(t, valuePair.Key)
	assert.Contains(t, string(valuePair.Value), "name: test")
	assert.Contains(t, string(valuePair.Value), "value: 42")

	sigPairs := 0

	for _, pair := range pairs {
		if string(pair.Key) == "/test/sig/rsa/my-object" {
			sigPairs++

			assert.Equal(t, "mock-signature-rsa", string(pair.Value))
		}

		if string(pair.Key) == "/test/sig/ecdsa/my-object" {
			sigPairs++

			assert.Equal(t, "mock-signature-ecdsa", string(pair.Value))
		}
	}

	assert.Equal(t, 2, sigPairs)
}

func TestGeneratorGenerate_SuccessWithHashesAndSignatures(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{"sha256"}, []string{"rsa"}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{
			newMockHasher("sha256"),
		},
		[]crypto.Signer{
			newMockSigner("rsa"),
		},
	)

	value := SimpleStruct{Name: "test", Value: 42}
	pairs, err := generator.Generate("my-object", value)

	require.NoError(t, err)
	require.Len(t, pairs, 3)

	keys := make(map[string]bool)
	for _, pair := range pairs {
		keys[string(pair.Key)] = true
	}

	assert.True(t, keys["/test/my-object"])
	assert.True(t, keys["/test/hash/sha256/my-object"])
	assert.True(t, keys["/test/sig/rsa/my-object"])
}

func TestGeneratorGenerate_ErrorHasherNotFound(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{"sha256", "md5"}, []string{}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{
			newMockHasher("sha256"),
		},
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}
	_, err := generator.Generate("my-object", value)

	assert.ErrorAs(t, err, &integrity.ImpossibleError{})
}

func TestGeneratorGenerate_ErrorSignerNotFound(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{}, []string{"rsa", "ecdsa"}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{
			newMockSigner("rsa"),
		},
	)

	value := SimpleStruct{Name: "test", Value: 42}
	_, err := generator.Generate("my-object", value)

	assert.ErrorAs(t, err, &integrity.ImpossibleError{})
}

func TestGeneratorGenerate_ErrorHasherReturnsError(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{"sha256"}, []string{}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{
			newMockHasherWithError("sha256", "hash error"),
		},
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}
	_, err := generator.Generate("my-object", value)

	assert.ErrorAs(t, err, &integrity.FailedToComputeHashError{})
}

func TestGeneratorGenerate_ErrorSignerReturnsError(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{}, []string{"rsa"}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{
			newMockSignerWithError("rsa", "sign error"),
		},
	)

	value := SimpleStruct{Name: "test", Value: 42}
	_, err := generator.Generate("my-object", value)

	assert.ErrorAs(t, err, &integrity.FailedToGenerateSignatureError{})
}

func TestGeneratorGenerate_ErrorMarshallerReturnsError(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{}, []string{}),
		&mockFailingTypedMarshaller[SimpleStruct]{
			marshalErr:   errors.New("marshal error"),
			unmarshalErr: nil,
		},
		nil,
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}
	_, err := generator.Generate("my-object", value)

	assert.ErrorAs(t, err, &integrity.FailedToMarshalValueError{})
}

func TestGeneratorGenerate_ErrorInvalidName(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("test", []string{}, []string{}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}

	_, err := generator.Generate("", value)
	require.ErrorAs(t, err, &integrity.FailedToGenerateKeysError{})

	_, err = generator.Generate("prefix/", value)
	require.ErrorAs(t, err, &integrity.FailedToGenerateKeysError{})
}

func TestGeneratorGenerate_OutputStructure(t *testing.T) {
	t.Parallel()

	generator := integrity.NewGenerator[SimpleStruct](
		namer.NewDefaultNamer("storage", []string{"sha256"}, []string{"rsa"}),
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{
			newMockHasher("sha256"),
		},
		[]crypto.Signer{
			newMockSigner("rsa"),
		},
	)

	value := SimpleStruct{Name: "test-object", Value: 100}
	pairs, err := generator.Generate("config/app", value)

	require.NoError(t, err)
	require.Len(t, pairs, 3)

	// Verify key patterns.
	expectedKeys := map[string]bool{
		"/storage/config/app":             true,
		"/storage/hash/sha256/config/app": true,
		"/storage/sig/rsa/config/app":     true,
	}

	for _, pair := range pairs {
		assert.True(t, expectedKeys[string(pair.Key)], "unexpected key: %s", pair.Key)
		assert.NotEmpty(t, pair.Value)
	}
}
