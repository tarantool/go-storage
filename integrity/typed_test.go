package integrity_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/internal/mocks"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

type mockNamer struct {
	generateNamesErr error
	prefixVal        string
}

func (m *mockNamer) GenerateNames(name string) ([]namer.Key, error) {
	if m.generateNamesErr != nil {
		return nil, m.generateNamesErr
	}

	return []namer.Key{
		namer.NewDefaultKey(name, namer.KeyTypeValue, "", "/test/"+name),
	}, nil
}

func (m *mockNamer) ParseKey(name string) (namer.DefaultKey, error) {
	return namer.DefaultKey{}, errors.New("not implemented")
}

func (m *mockNamer) ParseKeys(names []string, ignoreError bool) (namer.Results, error) {
	return namer.Results{}, errors.New("not implemented")
}

func (m *mockNamer) Prefix(val string, isPrefix bool) string {
	switch {
	case m.prefixVal != "":
		return m.prefixVal
	case isPrefix:
		return "/test/" + val + "/"
	default:
		return "/test/" + val
	}
}

func TestTypedGet_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	ctx := context.Background()
	_, err := typed.Get(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	_, err = typed.Get(ctx, "/name")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	_, err = typed.Get(ctx, "name/")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestTypedPut_InvalidName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	err := typed.Put(ctx, "", SimpleStruct{Name: "test", Value: 42})
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = typed.Put(ctx, "/name", SimpleStruct{Name: "test", Value: 42})
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = typed.Put(ctx, "name/", SimpleStruct{Name: "test", Value: 42})
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestTypedDelete_InvalidName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	err := typed.Delete(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = typed.Delete(ctx, "/name")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = typed.Delete(ctx, "name/")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestTypedRange_InvalidName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	_, err := typed.Range(ctx, "/invalid")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestTypedWatch_InvalidName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	_, err := typed.Watch(ctx, "/invalid")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestTypedGet_Success(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := []operation.Operation{
		operation.Get([]byte(keys[0].Build())),
	}
	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{expectedKVs[0]},
			},
		},
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	namedValue, err := typed.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, "my-object", namedValue.Name)
	assert.Equal(t, int64(integrity.ModRevisionEmpty), namedValue.ModRevision)
	assert.True(t, namedValue.Value.IsSome())

	val, ok := namedValue.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestTypedGet_ExecutionError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	ctx := context.Background()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, operation.Get([]byte(key.Build())))
	}

	expectedError := errors.New("driver execution failed")

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	_, err = typed.Get(ctx, "my-object")
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to execute")

	driverMock.MinimockFinish()
}

func TestTypedGet_NotFound(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	ctx := context.Background()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, operation.Get([]byte(key.Build())))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	_, err = typed.Get(ctx, "my-object")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrNotFound)

	driverMock.MinimockFinish()
}

func TestTypedGet_VerificationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockHasher := newMockHasher("sha256")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithHasher(mockHasher).
		Build()

	ctx := context.Background()

	namerInstance := namer.NewDefaultNamer("/test", []string{"sha256"}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockHasher},
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	dataKV := expectedKVs[0]

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, operation.Get([]byte(key.Build())))
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{dataKV},
			},
		},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	_, err = typed.Get(ctx, "my-object")
	require.Error(t, err)
	require.ErrorContains(t, err, "hash")

	driverMock.MinimockFinish()
}

func TestTypedGet_WithIgnoreVerificationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockHasher := newMockHasher("sha256")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithHasher(mockHasher).
		Build()

	ctx := context.Background()

	namerInstance := namer.NewDefaultNamer("/test", []string{"sha256"}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockHasher},
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	dataKV := expectedKVs[0]

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, operation.Get([]byte(key.Build())))
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{dataKV},
			},
		},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	result, err := typed.Get(ctx, "my-object", integrity.IgnoreVerificationError())
	require.NoError(t, err)
	assert.Equal(t, "my-object", result.Name)
	require.Error(t, result.Error)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestTypedPut_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, expectedKV := range expectedKVs {
		expectedOps = append(expectedOps, operation.Put(expectedKV.Key, expectedKV.Value))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	err = typed.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestTypedDelete_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, operation.Delete([]byte(key.Build())))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	err = typed.Delete(ctx, "my-object")
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestTypedGet_WithIgnoreMoreThanOneResult(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := []operation.Operation{
		operation.Get([]byte(keys[0].Build())),
	}
	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{expectedKVs[0]},
			},
		},
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	// Test that IgnoreMoreThanOneResult option can be used without error.
	result, err := typed.Get(ctx, "my-object", integrity.IgnoreMoreThanOneResult())
	require.NoError(t, err)
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestTypedBuilder_WithNamer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	strg := storage.NewStorage(driverMock)

	// Create a custom namer constructor that returns a DefaultNamer
	// We'll track if it was called with the right parameters.
	var constructorCalled bool

	var constructorPrefix string

	var constructorHashNames []string

	var constructorSigNames []string

	customNamerConstructor := func(prefix string, hashNames []string, sigNames []string) namer.Namer {
		constructorCalled = true
		constructorPrefix = prefix
		constructorHashNames = hashNames
		constructorSigNames = sigNames

		return namer.NewDefaultNamer(prefix, hashNames, sigNames)
	}

	typed := integrity.NewTypedBuilder[SimpleStruct](strg).
		WithPrefix("/test").
		WithHasher(newMockHasher("sha256")).
		WithSigner(newMockSigner("rsa")).
		WithNamer(customNamerConstructor).
		Build()

	require.True(t, constructorCalled, "namer constructor should have been called")
	require.Equal(t, "/test", constructorPrefix)
	require.Equal(t, []string{"sha256"}, constructorHashNames)
	require.Equal(t, []string{"rsa"}, constructorSigNames)

	// Now test that the typed storage works with the custom namer.
	namerInstance := namer.NewDefaultNamer("/test", []string{"sha256"}, []string{"rsa"})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 3) // value + hash + signature.

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{newMockHasher("sha256")},
		[]crypto.Signer{newMockSigner("rsa")},
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 3)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, operation.Put(kv.Key, kv.Value))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	err = typed.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

type mockSignerVerifier struct {
	name      string
	signErr   error
	verifyErr error
}

func (m *mockSignerVerifier) Name() string { return m.name }

func (m *mockSignerVerifier) Sign(data []byte) ([]byte, error) {
	if m.signErr != nil {
		return nil, m.signErr
	}

	return []byte("mock-signature-" + m.name), nil
}

func (m *mockSignerVerifier) Verify(data []byte, signature []byte) error {
	return m.verifyErr
}

func TestTypedBuilder_WithSignerVerifier(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockSignerVerifier := &mockSignerVerifier{name: "rsa", signErr: nil, verifyErr: nil}
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithSignerVerifier(mockSignerVerifier).
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{"rsa"})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{mockSignerVerifier},
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))

	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, operation.Put(kv.Key, kv.Value))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	err = typed.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestTypedDelete_TransactionExecutionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, operation.Delete([]byte(key.Build())))
	}

	expectedError := errors.New("driver execution failed")
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	err = typed.Delete(ctx, "my-object")
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to execute")

	driverMock.MinimockFinish()
}

func TestTypedDelete_NamerGenerateNamesError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := storage.NewStorage(driverMock)

	mockNamer := &mockNamer{
		generateNamesErr: errors.New("namer error"),
		prefixVal:        "",
	}

	typed := integrity.NewTypedBuilder[SimpleStruct](store).
		WithNamer(func(prefix string, hashNames []string, sigNames []string) namer.Namer {
			return mockNamer
		}).
		Build()

	ctx := context.Background()
	err := typed.Delete(ctx, "my-object")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to generate keys")
}

func TestTypedRange_Success(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	ctx := context.Background()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)

	value1 := SimpleStruct{Name: "obj1", Value: 100}
	value2 := SimpleStruct{Name: "obj2", Value: 200}

	kvs1, err := generator.Generate("object1", value1)
	require.NoError(t, err)
	require.Len(t, kvs1, 1)

	kvs2, err := generator.Generate("object2", value2)
	require.NoError(t, err)
	require.Len(t, kvs2, 1)

	expectedPrefix := namerInstance.Prefix("", true)
	expectedOps := []operation.Operation{
		operation.Get([]byte(expectedPrefix)),
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{kvs1[0], kvs2[0]},
			},
		},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	results, err := typed.Range(ctx, "")
	require.NoError(t, err)
	require.Len(t, results, 2)

	foundObj1 := false
	foundObj2 := false

	for _, result := range results {
		require.NoError(t, result.Error)
		require.Equal(t, int64(integrity.ModRevisionEmpty), result.ModRevision)
		require.True(t, result.Value.IsSome())

		val, ok := result.Value.Get()
		require.True(t, ok)

		if val.Name == "obj1" && val.Value == 100 {
			foundObj1 = true

			assert.Equal(t, "object1", result.Name)
		} else if val.Name == "obj2" && val.Value == 200 {
			foundObj2 = true

			assert.Equal(t, "object2", result.Name)
		}
	}

	assert.True(t, foundObj1, "object1 not found in results")
	assert.True(t, foundObj2, "object2 not found in results")

	driverMock.MinimockFinish()
}

func TestTypedRange_WithValidationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockHasher := newMockHasher("sha256")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithHasher(mockHasher).
		Build()

	ctx := context.Background()

	namerInstance := namer.NewDefaultNamer("/test", []string{"sha256"}, []string{})
	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockHasher},
		nil,
	)

	value := SimpleStruct{Name: "obj1", Value: 100}
	kvs, err := generator.Generate("object1", value)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	dataKV := kvs[0]

	expectedPrefix := namerInstance.Prefix("", true)
	expectedOps := []operation.Operation{
		operation.Get([]byte(expectedPrefix)),
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{dataKV},
			},
		},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	results, err := typed.Range(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, results, 0)

	driverMock.MinimockFinish()
}

func TestTypedRange_WithIgnoreVerificationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockHasher := newMockHasher("sha256")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithHasher(mockHasher).
		Build()

	ctx := context.Background()

	namerInstance := namer.NewDefaultNamer("/test", []string{"sha256"}, []string{})
	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockHasher},
		nil,
	)

	value := SimpleStruct{Name: "obj1", Value: 100}
	kvs, err := generator.Generate("object1", value)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	dataKV := kvs[0]

	expectedPrefix := namerInstance.Prefix("", true)
	expectedOps := []operation.Operation{
		operation.Get([]byte(expectedPrefix)),
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{dataKV},
			},
		},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	results, err := typed.Range(ctx, "", integrity.IgnoreVerificationError())
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, "object1", results[0].Name)
	require.Error(t, results[0].Error)
	assert.True(t, results[0].Value.IsSome())

	val, ok := results[0].Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestTypedRange_ExecutionError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	ctx := context.Background()

	expectedPrefix := namer.NewDefaultNamer("/test", []string{}, []string{}).Prefix("", true)
	expectedOps := []operation.Operation{
		operation.Get([]byte(expectedPrefix)),
	}

	expectedError := errors.New("driver execution failed")

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	results, err := typed.Range(ctx, "")
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to execute range")
	require.Nil(t, results)

	driverMock.MinimockFinish()
}

func TestTypedRange_WithIgnoreVerificationErrorButFailedToDecode(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	ctx := context.Background()

	kvPair := kv.KeyValue{
		Key:         []byte("/test/my-object"),
		Value:       []byte("invalid: yaml: [unclosed"),
		ModRevision: 0,
	}

	expectedPrefix := "/test/"
	expectedOps := []operation.Operation{
		operation.Get([]byte(expectedPrefix)),
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{kvPair},
			},
		},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	results, err := typed.Range(ctx, "", integrity.IgnoreVerificationError())
	require.NoError(t, err)
	require.Empty(t, results)

	driverMock.MinimockFinish()
}

func TestTypedGet_WithHasher(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockHasher := newMockHasher("sha256")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithHasher(mockHasher).
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{"sha256"}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockHasher},
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	kvMap := make(map[string]kv.KeyValue, len(expectedKVs))
	for _, kvPair := range expectedKVs {
		kvMap[string(kvPair.Key)] = kvPair
	}

	var (
		expectedOps = make([]operation.Operation, 0, len(keys))
		results     = make([]tx.RequestResponse, 0, len(keys))
	)

	for _, key := range keys {
		keyStr := key.Build()

		expectedOps = append(expectedOps, operation.Get([]byte(keyStr)))

		kvPair, ok := kvMap[keyStr]
		require.True(t, ok, "missing expected KV for key %s", keyStr)

		results = append(results, tx.RequestResponse{
			Values: []kv.KeyValue{kvPair},
		})
	}

	response := tx.Response{
		Succeeded: true,
		Results:   results,
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	namedValue, err := typed.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, "my-object", namedValue.Name)
	assert.True(t, namedValue.Value.IsSome())

	val, ok := namedValue.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestTypedGet_WithHasherAndNamer(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockHasher := newMockHasher("sha256")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithHasher(mockHasher).
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{"sha256"}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockHasher},
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	kvMap := make(map[string]kv.KeyValue, len(expectedKVs))
	for _, kvPair := range expectedKVs {
		kvMap[string(kvPair.Key)] = kvPair
	}

	var (
		expectedOps = make([]operation.Operation, 0, len(keys))
		results     = make([]tx.RequestResponse, 0, len(keys))
	)

	for _, key := range keys {
		keyStr := key.Build()

		expectedOps = append(expectedOps, operation.Get([]byte(keyStr)))

		kvPair, ok := kvMap[keyStr]
		require.True(t, ok, "missing expected KV for key %s", keyStr)

		results = append(results, tx.RequestResponse{
			Values: []kv.KeyValue{kvPair},
		})
	}

	response := tx.Response{
		Succeeded: true,
		Results:   results,
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	namedValue, err := typed.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, "my-object", namedValue.Name)
	assert.True(t, namedValue.Value.IsSome())

	val, ok := namedValue.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestTypedPut_WithSigner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockSigner := newMockSigner("rsa")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithSigner(mockSigner).
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{"rsa"})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{mockSigner},
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))

	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, operation.Put(kv.Key, kv.Value))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	err = typed.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestTypedGet_WithVerifier(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	mockVerifier := &mockVerifier{name: "rsa", verifyErr: nil}
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithVerifier(mockVerifier).
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{"rsa"})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	mockSigner := newMockSigner("rsa")
	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{mockSigner},
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	kvMap := make(map[string]kv.KeyValue, len(expectedKVs))
	for _, kvPair := range expectedKVs {
		kvMap[string(kvPair.Key)] = kvPair
	}

	var (
		expectedOps = make([]operation.Operation, 0, len(keys))
		results     = make([]tx.RequestResponse, 0, len(keys))
	)

	for _, key := range keys {
		keyStr := key.Build()

		expectedOps = append(expectedOps, operation.Get([]byte(keyStr)))

		kvPair, ok := kvMap[keyStr]
		require.True(t, ok, "missing expected KV for key %s", keyStr)

		results = append(results, tx.RequestResponse{
			Values: []kv.KeyValue{kvPair},
		})
	}

	response := tx.Response{
		Succeeded: true,
		Results:   results,
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	namedValue, err := typed.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, "my-object", namedValue.Name)
	assert.True(t, namedValue.Value.IsSome())

	val, ok := namedValue.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestTypedGet_NamerGenerateNamesError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := storage.NewStorage(driverMock)

	mockNamer := &mockNamer{
		generateNamesErr: errors.New("namer error"),
		prefixVal:        "",
	}

	typed := integrity.NewTypedBuilder[SimpleStruct](store).
		WithNamer(func(prefix string, hashNames []string, sigNames []string) namer.Namer {
			return mockNamer
		}).
		Build()

	ctx := context.Background()
	_, err := typed.Get(ctx, "my-object")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to generate keys")
}

func TestTypedPut_GenerationError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	failingHasher := newMockHasherWithError("sha256", "hash computation failed")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithHasher(failingHasher).
		Build()

	value := SimpleStruct{Name: "test", Value: 42}
	err := typed.Put(ctx, "my-object", value)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to generate")
	require.ErrorContains(t, err, "hash computation failed")

	driverMock.MinimockFinish()
}

func TestTypedPut_TransactionExecutionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, expectedKV := range expectedKVs {
		expectedOps = append(expectedOps, operation.Put(expectedKV.Key, expectedKV.Value))
	}

	expectedError := errors.New("driver execution failed")
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	err = typed.Put(ctx, "my-object", value)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to execute")

	driverMock.MinimockFinish()
}

func TestTypedPut_SignerError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	failingSigner := newMockSignerWithError("rsa", "signature generation failed")
	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithSigner(failingSigner).
		Build()

	value := SimpleStruct{Name: "test", Value: 42}
	err := typed.Put(ctx, "my-object", value)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to generate")
	require.ErrorContains(t, err, "signature generation failed")

	driverMock.MinimockFinish()
}

func TestTypedBuilder_WithMarshaller(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		WithMarshaller(marshaller.NewTypedYamlMarshaller[SimpleStruct]()).
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, expectedKV := range expectedKVs {
		expectedOps = append(expectedOps, operation.Put(expectedKV.Key, expectedKV.Value))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	err = typed.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestTypedWatch(t *testing.T) {
	t.Parallel()

	t.Run("basic event filtering", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		driverMock := mocks.NewDriverMock(t)
		st := storage.NewStorage(driverMock)

		typed := integrity.NewTypedBuilder[SimpleStruct](st).
			WithPrefix("/test").
			Build()

		namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
		key := namerInstance.Prefix("my-object", false)

		rawCh := make(chan watch.Event, 10)
		cleanup := func() {} // no-op, channel will be garbage collected.

		driverMock.WatchMock.Expect(ctx, []byte(key)).Return(rawCh, cleanup, nil)

		eventCh, err := typed.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Send events.
		events := []watch.Event{
			{Prefix: []byte("/test/my-object")},             // value key, name "my-object" passes.
			{Prefix: []byte("/test/my-object2")},            // value key, name "my-object2" passes (prefix match).
			{Prefix: []byte("/test/hash/sha256/my-object")}, // hash key, name "my-object" passes.
			{Prefix: []byte("/test/other-object")},          // name "other-object" filtered out (no prefix match).
		}

		for _, e := range events {
			rawCh <- e
		}

		// Read events from filtered channel.
		var received []watch.Event

	loop:
		for range 3 {
			select {
			case e := <-eventCh:
				received = append(received, e)
			case <-time.After(100 * time.Millisecond):
				break loop
			}
		}

		require.Len(t, received, 3)
		assert.Equal(t, events[0], received[0])
		assert.Equal(t, events[1], received[1])
		assert.Equal(t, events[2], received[2])

		driverMock.MinimockFinish()
	})

	t.Run("raw channel closure", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		driverMock := mocks.NewDriverMock(t)
		st := storage.NewStorage(driverMock)

		typed := integrity.NewTypedBuilder[SimpleStruct](st).
			WithPrefix("/test").
			Build()

		namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
		key := namerInstance.Prefix("my-object", false)

		rawCh := make(chan watch.Event)
		cleanup := func() {} // no-op.

		driverMock.WatchMock.Expect(ctx, []byte(key)).Return(rawCh, cleanup, nil)

		eventCh, err := typed.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Close raw channel immediately.
		close(rawCh)

		// filteredCh should be closed.
		select {
		case _, ok := <-eventCh:
			assert.False(t, ok, "channel should be closed")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected channel to be closed")
		}

		driverMock.MinimockFinish()
	})

	t.Run("ParseKey error skips event", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		driverMock := mocks.NewDriverMock(t)
		st := storage.NewStorage(driverMock)

		typed := integrity.NewTypedBuilder[SimpleStruct](st).
			WithPrefix("/test").
			Build()

		namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
		key := namerInstance.Prefix("my-object", false)

		rawCh := make(chan watch.Event, 1)
		cleanup := func() {} // no-op.

		driverMock.WatchMock.Expect(ctx, []byte(key)).Return(rawCh, cleanup, nil)

		eventCh, err := typed.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Send an event with malformed prefix that will cause ParseKey error.
		rawCh <- watch.Event{Prefix: []byte("/invalid/key")}

		// No event should be forwarded.
		select {
		case <-eventCh:
			t.Fatal("unexpected event forwarded")
		case <-time.After(100 * time.Millisecond):
			// Expected - event filtered out.
		}

		driverMock.MinimockFinish()
	})

	t.Run("context cancellation while waiting", func(t *testing.T) {
		t.Parallel()

		driverMock := mocks.NewDriverMock(t)
		st := storage.NewStorage(driverMock)

		typed := integrity.NewTypedBuilder[SimpleStruct](st).
			WithPrefix("/test").
			Build()

		namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
		key := namerInstance.Prefix("my-object", false)

		rawCh := make(chan watch.Event)
		cleanup := func() {} // no-op.

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		driverMock.WatchMock.Expect(ctx, []byte(key)).Return(rawCh, cleanup, nil)

		eventCh, err := typed.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Cancel context.
		cancel()

		// filteredCh should be closed.
		select {
		case _, ok := <-eventCh:
			assert.False(t, ok, "channel should be closed after context cancellation")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected channel to be closed after context cancellation")
		}

		driverMock.MinimockFinish()
	})

	t.Run("context cancellation while sending", func(t *testing.T) {
		t.Parallel()

		driverMock := mocks.NewDriverMock(t)
		st := storage.NewStorage(driverMock)

		typed := integrity.NewTypedBuilder[SimpleStruct](st).
			WithPrefix("/test").
			Build()

		namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
		key := namerInstance.Prefix("my-object", false)

		rawCh := make(chan watch.Event, 1)
		cleanup := func() {} // no-op.

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		driverMock.WatchMock.Expect(ctx, []byte(key)).Return(rawCh, cleanup, nil)

		eventCh, err := typed.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Send an event that will be filtered (valid key but not matching name).
		rawCh <- watch.Event{Prefix: []byte("/test/other-object")}

		// Cancel context while goroutine is trying to send to filteredCh (which nobody reads).
		// Since filteredCh is unbuffered and we're not reading, the send will block.
		// Cancelling context should cause goroutine to exit.
		cancel()

		// Wait a bit for goroutine to exit.
		time.Sleep(50 * time.Millisecond)

		// filteredCh should be closed.
		select {
		case _, ok := <-eventCh:
			assert.False(t, ok, "channel should be closed after context cancellation")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected channel to be closed after context cancellation")
		}

		driverMock.MinimockFinish()
	})

	t.Run("inner select context cancellation", func(t *testing.T) {
		t.Parallel()

		driverMock := mocks.NewDriverMock(t)
		st := storage.NewStorage(driverMock)

		typed := integrity.NewTypedBuilder[SimpleStruct](st).
			WithPrefix("/test").
			Build()

		namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
		key := namerInstance.Prefix("my-object", false)

		rawCh := make(chan watch.Event, 1)
		cleanup := func() {} // no-op.

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		driverMock.WatchMock.Expect(ctx, []byte(key)).Return(rawCh, cleanup, nil)

		eventCh, err := typed.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Send a valid event that passes all filters.
		rawCh <- watch.Event{Prefix: []byte("/test/my-object")}

		// Give goroutine time to receive event and enter inner select.
		time.Sleep(10 * time.Millisecond)

		// Cancel context while goroutine is blocked trying to send to filteredCh.
		cancel()

		// filteredCh should be closed.
		select {
		case _, ok := <-eventCh:
			assert.False(t, ok, "channel should be closed after context cancellation")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected channel to be closed after context cancellation")
		}

		driverMock.MinimockFinish()
	})
}

func TestTypedGet_PassingModRevision(t *testing.T) {
	t.Parallel()

	var expectedModRevision int64 = 67

	value := SimpleStruct{Name: "test", Value: 42}

	driverMock := mocks.NewDriverMock(t)
	st := storage.NewStorage(driverMock)

	typed := integrity.NewTypedBuilder[SimpleStruct](st).
		WithPrefix("/test").
		Build()

	namerInstance := namer.NewDefaultNamer("/test", []string{}, []string{})
	keys, err := namerInstance.GenerateNames("my-object")
	require.NoError(t, err)

	generator := integrity.NewGenerator[SimpleStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)

	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)

	expectedKVs[0].ModRevision = expectedModRevision

	expectedOps := []operation.Operation{
		operation.Get([]byte(keys[0].Build())),
	}
	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				Values: []kv.KeyValue{expectedKVs[0]},
			},
		},
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(
		ctx,
		nil,
		expectedOps,
		nil,
	).Return(response, nil)

	namedValue, err := typed.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, expectedModRevision, namedValue.ModRevision)

	driverMock.MinimockFinish()
}
