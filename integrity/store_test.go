package integrity_test

// Conventions used by tests in this file:
//
//   codec := integrity.NewCodecBuilder[T]().WithObjectLocation("objects").WithHasher(...).Build()
//   store := codec.Bind(storage.Prefixed("/test", storage.NewStorage(driverMock)))
//
// LayeredNamer key layout (objectLocation = "objects"):
//   /objects/<name>          value
//   /<hasherName>/<name>     hash       (e.g. /sha256/<name>)
//   /<signerName>/<name>     signature  (e.g. /rsa/<name>)
//
// The Prefixed("/test", ...) wrapper prepends "/test" for the driver mock,
// so /objects/<name> is asserted against the driver as /test/objects/<name>.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/internal/mocks"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

const storeTestPrefix = "/test"

func newStoreCodec(t *testing.T) *integrity.Codec[SimpleStruct] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	return codec
}

func newStoreCodecWithHasher(t *testing.T, h hasher.Hasher) *integrity.Codec[SimpleStruct] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").WithHasher(h).Build()
	require.NoError(t, err)

	return codec
}

func newStoreCodecWithSigner(t *testing.T, s crypto.Signer) *integrity.Codec[SimpleStruct] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").WithSigner(s).Build()
	require.NoError(t, err)

	return codec
}

func newStoreCodecWithVerifier(t *testing.T, v crypto.Verifier) *integrity.Codec[SimpleStruct] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").WithVerifier(v).Build()
	require.NoError(t, err)

	return codec
}

func newStoreCodecWithSignerVerifier(t *testing.T, sv crypto.SignerVerifier) *integrity.Codec[SimpleStruct] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").WithSignerVerifier(sv).Build()
	require.NoError(t, err)

	return codec
}

func newMockedStore(
	t *testing.T,
	codec *integrity.Codec[SimpleStruct],
	driverMock *mocks.DriverMock,
) *integrity.Store[SimpleStruct] {
	t.Helper()

	st := storage.NewStorage(driverMock)
	prefixed, err := storage.Prefixed(storeTestPrefix, st)
	require.NoError(t, err)

	return codec.Bind(prefixed)
}

// newLayeredNamer reproduces the codec's internal namer so tests can compute
// the absolute keys the driver mock will see (location = hasher/signer name).
func newLayeredNamer(t *testing.T, hashNames, sigNames []string) namer.Namer {
	t.Helper()

	hashLocs := make([]namer.LayeredHashLocation, 0, len(hashNames))
	for _, n := range hashNames {
		hashLocs = append(hashLocs, namer.LayeredHashLocation{HasherName: n, Location: n})
	}

	sigLocs := make([]namer.LayeredSigLocation, 0, len(sigNames))
	for _, n := range sigNames {
		sigLocs = append(sigLocs, namer.LayeredSigLocation{SignerName: n, Location: n})
	}

	testNamer, err := namer.NewLayeredNamer("objects", hashLocs, sigLocs)
	require.NoError(t, err)

	return testNamer
}

func absKey(relKey []byte) []byte {
	out := make([]byte, len(storeTestPrefix)+len(relKey))
	copy(out, storeTestPrefix)
	copy(out[len(storeTestPrefix):], relKey)

	return out
}

func absKeyStr(relKey string) []byte {
	return absKey([]byte(relKey))
}

func makeAbsoluteGetOp(relKey string) operation.Operation {
	return operation.Get(absKeyStr(relKey))
}

func makeAbsolutePutOp(relKey string, value []byte) operation.Operation {
	return operation.Put(absKeyStr(relKey), value)
}

func makeAbsoluteDeleteOp(relKey string) operation.Operation {
	return operation.Delete(absKeyStr(relKey))
}

func storeValueKeyFromKVs(t *testing.T, testNamer namer.Namer, kvs []kv.KeyValue) []byte {
	t.Helper()

	for _, kvPair := range kvs {
		key, err := testNamer.ParseKey(string(kvPair.Key))
		require.NoError(t, err)

		if key.Type() == namer.KeyTypeValue {
			return absKey(kvPair.Key)
		}
	}

	t.Fatal("value key not found in generated KVs")

	return nil
}

func TestStore_Get_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	_, err := store.Get(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	_, err = store.Get(ctx, "/name")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	_, err = store.Get(ctx, "name/")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestStore_Put_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	err := store.Put(ctx, "", SimpleStruct{Name: "test", Value: 42})
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = store.Put(ctx, "/name", SimpleStruct{Name: "test", Value: 42})
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = store.Put(ctx, "name/", SimpleStruct{Name: "test", Value: 42})
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestStore_Delete_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	err := store.Delete(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = store.Delete(ctx, "/name")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	err = store.Delete(ctx, "name/")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestStore_Range_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	_, err := store.Range(ctx, "/invalid")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestStore_Watch_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	_, err := store.Watch(ctx, "/invalid")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	driverMock.MinimockFinish()
}

func TestStore_Get_Success(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	// Driver receives absolute keys (Prefixed wrapper adds "/test").
	expectedOps := []operation.Operation{
		makeAbsoluteGetOp(keys[0].Build()),
	}
	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{
				// Values returned with absolute keys; Prefixed strips them before returning.
				Values: []kv.KeyValue{{
					Key:         expectedKVs[0].Key, // relative key (Prefixed strips on way up).
					Value:       expectedKVs[0].Value,
					ModRevision: expectedKVs[0].ModRevision,
				}},
			},
		},
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	namedValue, err := store.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, "my-object", namedValue.Name)
	assert.Equal(t, int64(integrity.ModRevisionEmpty), namedValue.ModRevision)
	assert.True(t, namedValue.Value.IsSome())

	val, ok := namedValue.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestStore_Get_ExecutionError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := []operation.Operation{
		makeAbsoluteGetOp(keys[0].Build()),
	}

	expectedError := errors.New("driver execution failed")

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).
		Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	_, err = store.Get(ctx, "my-object")
	require.Error(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Get_NotFound(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := []operation.Operation{
		makeAbsoluteGetOp(keys[0].Build()),
	}

	// Tx.Commit slices Results[span.start:span.end]; must have 1 RR for 1 op.
	// Empty Values inside the RR → ErrNotFound.
	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{{Values: nil}},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	_, err = store.Get(ctx, "my-object")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrNotFound)

	driverMock.MinimockFinish()
}

func TestStore_Get_VerificationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	mockH := newMockHasher("sha256")
	codec := newStoreCodecWithHasher(t, mockH)
	store := newMockedStore(t, codec, driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, []string{"sha256"}, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockH},
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	// TxGet enqueues one Get op per key (value + hash = 2 ops).
	// The response must have 2 RequestResponse items (one per op).
	// Hash KV is absent → verification error.
	dataKV := expectedKVs[0]

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, makeAbsoluteGetOp(key.Build()))
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: []kv.KeyValue{dataKV}}, // value key: data present.
			{Values: nil},                   // hash key: absent.
		},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	_, err = store.Get(ctx, "my-object")
	require.Error(t, err)
	require.ErrorContains(t, err, "hash")

	driverMock.MinimockFinish()
}

func TestStore_Get_WithIgnoreVerificationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	mockH := newMockHasher("sha256")
	codec := newStoreCodecWithHasher(t, mockH)
	store := newMockedStore(t, codec, driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, []string{"sha256"}, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockH},
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	// TxGet enqueues 2 ops (value + hash). Response must have 2 RR items.
	dataKV := expectedKVs[0]

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, makeAbsoluteGetOp(key.Build()))
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: []kv.KeyValue{dataKV}}, // value key: data present.
			{Values: nil},                   // hash key: absent → verification error.
		},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	result, err := store.Get(ctx, "my-object", integrity.IgnoreVerificationError())
	require.NoError(t, err)
	assert.Equal(t, "my-object", result.Name)
	require.Error(t, result.Error)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestStore_Get_WithIgnoreMoreThanOneResult(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := []operation.Operation{
		makeAbsoluteGetOp(keys[0].Build()),
	}
	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: []kv.KeyValue{expectedKVs[0]}},
		},
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	result, err := store.Get(ctx, "my-object", integrity.IgnoreMoreThanOneResult())
	require.NoError(t, err)
	assert.Equal(t, "my-object", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestStore_Get_WithHasher(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	mockH := newMockHasher("sha256")
	codec := newStoreCodecWithHasher(t, mockH)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, []string{"sha256"}, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockH},
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

	expectedOps := make([]operation.Operation, 0, len(keys))
	results := make([]tx.RequestResponse, 0, len(keys))

	for _, key := range keys {
		keyStr := key.Build()

		expectedOps = append(expectedOps, makeAbsoluteGetOp(keyStr))

		kvPair, ok := kvMap[keyStr]
		require.True(t, ok, "missing expected KV for key %s", keyStr)

		results = append(results, tx.RequestResponse{
			Values: []kv.KeyValue{kvPair},
		})
	}

	response := tx.Response{Succeeded: true, Results: results}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	namedValue, err := store.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, "my-object", namedValue.Name)
	assert.True(t, namedValue.Value.IsSome())

	val, ok := namedValue.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestStore_Get_WithVerifier(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	mockV := &mockVerifier{name: "rsa", verifyErr: nil}
	codec := newStoreCodecWithVerifier(t, mockV)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, []string{"rsa"})
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	mockS := newMockSigner("rsa")
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{mockS},
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	kvMap := make(map[string]kv.KeyValue, len(expectedKVs))
	for _, kvPair := range expectedKVs {
		kvMap[string(kvPair.Key)] = kvPair
	}

	expectedOps := make([]operation.Operation, 0, len(keys))
	results := make([]tx.RequestResponse, 0, len(keys))

	for _, key := range keys {
		keyStr := key.Build()

		expectedOps = append(expectedOps, makeAbsoluteGetOp(keyStr))

		kvPair, ok := kvMap[keyStr]
		require.True(t, ok, "missing expected KV for key %s", keyStr)

		results = append(results, tx.RequestResponse{
			Values: []kv.KeyValue{kvPair},
		})
	}

	response := tx.Response{Succeeded: true, Results: results}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	namedValue, err := store.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, "my-object", namedValue.Name)
	assert.True(t, namedValue.Value.IsSome())

	val, ok := namedValue.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	driverMock.MinimockFinish()
}

func TestStore_Get_NamerGenerateNamesError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	generateErr := errors.New("namer error")

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").
		WithNamer(func(
			_ string,
			_ []namer.LayeredHashLocation,
			_ []namer.LayeredSigLocation,
			_ ...namer.LayeredOption,
		) (namer.Namer, error) {
			return &mockNamer{generateNamesErr: generateErr, prefixVal: ""}, nil
		}).
		Build()
	require.NoError(t, err)

	prefixed, errPrefix := storage.Prefixed(storeTestPrefix, storage.NewStorage(driverMock))
	require.NoError(t, errPrefix)

	store := codec.Bind(prefixed)

	ctx := context.Background()

	_, err = store.Get(ctx, "my-object")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "namer error")
}

func TestStore_Get_PassingModRevision(t *testing.T) {
	t.Parallel()

	var expectedModRevision int64 = 67

	value := SimpleStruct{Name: "test", Value: 42}

	driverMock := mocks.NewDriverMock(t)
	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)

	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)

	expectedKVs[0].ModRevision = expectedModRevision

	expectedOps := []operation.Operation{
		makeAbsoluteGetOp(keys[0].Build()),
	}
	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: []kv.KeyValue{expectedKVs[0]}},
		},
	}

	ctx := context.Background()
	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	namedValue, err := store.Get(ctx, "my-object")
	require.NoError(t, err)
	assert.Equal(t, expectedModRevision, namedValue.ModRevision)

	driverMock.MinimockFinish()
}

func TestStore_Put_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	err = store.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Put_WithPutPredicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	// Value key (absolute = storeTestPrefix + relative).
	vKey := storeValueKeyFromKVs(t, testNamer, expectedKVs)

	valueEqualPredicate, err := codec.ValueEqual(value)
	require.NoError(t, err)

	versionGreaterPredicate := codec.VersionGreater(10)

	expectedPredicates := []predicate.Predicate{valueEqualPredicate(vKey), versionGreaterPredicate(vKey)}

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, expectedPredicates, expectedOps, nil).Return(response, nil)

	err = store.Put(ctx, "my-object", value, integrity.WithPutPredicates(valueEqualPredicate, versionGreaterPredicate))
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Put_WithPredicates_Failed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)

	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	vKey := storeValueKeyFromKVs(t, testNamer, expectedKVs)

	valueEqualPredicate, err := codec.ValueEqual(value)
	require.NoError(t, err)

	expectedPredicates := []predicate.Predicate{valueEqualPredicate(vKey)}

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	response := tx.Response{Succeeded: false, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, expectedPredicates, expectedOps, nil).Return(response, nil)

	err = store.Put(ctx, "my-object", value, integrity.WithPutPredicates(valueEqualPredicate))
	require.ErrorIs(t, err, integrity.ErrPredicateFailed)

	driverMock.MinimockFinish()
}

func TestStore_Put_GenerationError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	failingHasher := newMockHasherWithError("sha256", "hash computation failed")
	codec := newStoreCodecWithHasher(t, failingHasher)
	store := newMockedStore(t, codec, driverMock)

	value := SimpleStruct{Name: "test", Value: 42}
	err := store.Put(ctx, "my-object", value)
	require.Error(t, err)
	require.ErrorContains(t, err, "hash computation failed")

	driverMock.MinimockFinish()
}

func TestStore_Put_TransactionExecutionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	expectedError := errors.New("driver execution failed")
	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).
		Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	err = store.Put(ctx, "my-object", value)
	require.Error(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Put_SignerError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	failingSigner := newMockSignerWithError("rsa", "signature generation failed")
	codec := newStoreCodecWithSigner(t, failingSigner)
	store := newMockedStore(t, codec, driverMock)

	value := SimpleStruct{Name: "test", Value: 42}
	err := store.Put(ctx, "my-object", value)
	require.Error(t, err)
	require.ErrorContains(t, err, "signature generation failed")

	driverMock.MinimockFinish()
}

func TestStore_Put_WithSigner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	mockS := newMockSigner("rsa")
	codec := newStoreCodecWithSigner(t, mockS)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, []string{"rsa"})
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{mockS},
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	err = store.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Put_WithSignerVerifier(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	mockSV := &mockSignerVerifier{name: "rsa", signErr: nil, verifyErr: nil}
	codec := newStoreCodecWithSignerVerifier(t, mockSV)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, []string{"rsa"})
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		[]crypto.Signer{mockSV},
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 2)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	err = store.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Put_WithMarshaller(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").
		WithMarshaller(marshaller.NewTypedYamlMarshaller[SimpleStruct]()).
		Build()
	require.NoError(t, err)

	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		nil,
		nil,
	)
	value := SimpleStruct{Name: "test", Value: 42}
	expectedKVs, err := generator.Generate("my-object", value)
	require.NoError(t, err)
	require.Len(t, expectedKVs, 1)

	expectedOps := make([]operation.Operation, 0, len(expectedKVs))
	for _, kv := range expectedKVs {
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	err = store.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Predicates_ValueOps(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	codec := newStoreCodec(t)
	// store not used — just testing predicate factory methods on codec.
	_ = newMockedStore(t, codec, driverMock)

	// The key for the predicate is whatever the caller passes in.
	key := []byte("/objects/my-object")
	value := SimpleStruct{Name: "test", Value: 42}
	expectedValue, err := marshaller.NewTypedYamlMarshaller[SimpleStruct]().Marshal(value)
	require.NoError(t, err)

	tests := []struct {
		name           string
		buildPredicate func() (integrity.Predicate, error)
		expectedOp     predicate.Op
	}{
		{
			name: "ValueEqual",
			buildPredicate: func() (integrity.Predicate, error) {
				return codec.ValueEqual(value)
			},
			expectedOp: predicate.OpEqual,
		},
		{
			name: "ValueNotEqual",
			buildPredicate: func() (integrity.Predicate, error) {
				return codec.ValueNotEqual(value)
			},
			expectedOp: predicate.OpNotEqual,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			predFn, err := testCase.buildPredicate()
			require.NoError(t, err)

			p := predFn(key)
			require.NotNil(t, p)
			assert.Equal(t, key, p.Key())
			assert.Equal(t, predicate.TargetValue, p.Target())
			assert.Equal(t, testCase.expectedOp, p.Operation())
			assert.Equal(t, expectedValue, p.Value())
		})
	}

	driverMock.MinimockFinish()
}

func TestStore_Predicates_VersionOps(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	codec := newStoreCodec(t)

	_ = newMockedStore(t, codec, driverMock)

	key := []byte("/objects/my-object")
	version := int64(67)

	tests := []struct {
		name           string
		buildPredicate func() integrity.Predicate
		expectedOp     predicate.Op
	}{
		{
			name:           "VersionEqual",
			buildPredicate: func() integrity.Predicate { return codec.VersionEqual(version) },
			expectedOp:     predicate.OpEqual,
		},
		{
			name:           "VersionNotEqual",
			buildPredicate: func() integrity.Predicate { return codec.VersionNotEqual(version) },
			expectedOp:     predicate.OpNotEqual,
		},
		{
			name:           "VersionGreater",
			buildPredicate: func() integrity.Predicate { return codec.VersionGreater(version) },
			expectedOp:     predicate.OpGreater,
		},
		{
			name:           "VersionLess",
			buildPredicate: func() integrity.Predicate { return codec.VersionLess(version) },
			expectedOp:     predicate.OpLess,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			predFn := testCase.buildPredicate()
			p := predFn(key)
			require.NotNil(t, p)
			assert.Equal(t, key, p.Key())
			assert.Equal(t, predicate.TargetVersion, p.Target())
			assert.Equal(t, testCase.expectedOp, p.Operation())
			assert.Equal(t, version, p.Value())
		})
	}

	driverMock.MinimockFinish()
}

func TestStore_Delete_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, makeAbsoluteDeleteOp(key.Build()))
	}

	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	err = store.Delete(ctx, "my-object")
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Delete_WithDeletePredicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	// Delete uses value-key built from GenerateNames; absolute key for predicate.
	vKey := absKeyStr(keys[0].Build())

	versionEqualPredicate := codec.VersionEqual(5)
	expectedPredicates := []predicate.Predicate{versionEqualPredicate(vKey)}

	expectedOps := []operation.Operation{makeAbsoluteDeleteOp(keys[0].Build())}
	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, expectedPredicates, expectedOps, nil).Return(response, nil)

	err = store.Delete(ctx, "my-object", integrity.WithDeletePredicates(versionEqualPredicate))
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Delete_WithDeletePredicates_Failed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	vKey := absKeyStr(keys[0].Build())

	versionEqualPredicate := codec.VersionEqual(5)
	expectedPredicates := []predicate.Predicate{versionEqualPredicate(vKey)}

	expectedOps := []operation.Operation{makeAbsoluteDeleteOp(keys[0].Build())}
	response := tx.Response{Succeeded: false, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, expectedPredicates, expectedOps, nil).Return(response, nil)

	err = store.Delete(ctx, "my-object", integrity.WithDeletePredicates(versionEqualPredicate))
	require.ErrorIs(t, err, integrity.ErrPredicateFailed)

	driverMock.MinimockFinish()
}

func TestStore_Delete_TransactionExecutionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	expectedOps := make([]operation.Operation, 0, len(keys))
	for _, key := range keys {
		expectedOps = append(expectedOps, makeAbsoluteDeleteOp(key.Build()))
	}

	expectedError := errors.New("driver execution failed")
	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).
		Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	err = store.Delete(ctx, "my-object")
	require.Error(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Delete_NamerGenerateNamesError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	generateErr := errors.New("namer error")

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").
		WithNamer(func(
			_ string,
			_ []namer.LayeredHashLocation,
			_ []namer.LayeredSigLocation,
			_ ...namer.LayeredOption,
		) (namer.Namer, error) {
			return &mockNamer{generateNamesErr: generateErr, prefixVal: ""}, nil
		}).
		Build()
	require.NoError(t, err)

	prefixed, errPrefix := storage.Prefixed(storeTestPrefix, storage.NewStorage(driverMock))
	require.NoError(t, errPrefix)

	store := codec.Bind(prefixed)

	ctx := context.Background()

	err = store.Delete(ctx, "my-object")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "namer error")
}

func TestStore_Delete_Prefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	testNamer := newLayeredNamer(t, nil, nil)
	keys, err := testNamer.GenerateNames("test/")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	// TxDelete with withPrefix=true applies: strings.TrimSuffix(key.Build(), "/") + "/"
	// keys[0].Build() = "/objects/test/" (GenerateNames keeps trailing slash in name)
	// TrimSuffix("/objects/test/", "/") + "/" → "/objects/test/".
	keyStr := keys[0].Build()
	relKeyWithSlash := strings.TrimSuffix(keyStr, "/") + "/"
	expectedOps := []operation.Operation{makeAbsoluteDeleteOp(relKeyWithSlash)}

	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	err = store.Delete(ctx, "test/", integrity.WithPrefix())
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Delete_PrefixEmptyKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	store := newMockedStore(t, newStoreCodec(t), driverMock)

	err := store.Delete(ctx, "", integrity.WithPrefix())
	require.Error(t, err)
}

func TestStore_Delete_PrefixHasPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	store := newMockedStore(t, newStoreCodec(t), driverMock)

	err := store.Delete(ctx, "/objs/", integrity.WithPrefix())
	require.Error(t, err)
}

func TestStore_Delete_PrefixNoSuffix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	store := newMockedStore(t, newStoreCodec(t), driverMock)

	err := store.Delete(ctx, "/objs", integrity.WithPrefix())
	require.Error(t, err)
}

func TestStore_Range_Success(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	codec := newStoreCodec(t)
	store := newMockedStore(t, codec, driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, nil, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
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

	// Range with name="" uses the Prefix("", true) = "/objects/".
	expectedPrefix := testNamer.Prefix("", true) // "/objects/".
	expectedOps := []operation.Operation{
		makeAbsoluteGetOp(expectedPrefix),
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: []kv.KeyValue{kvs1[0], kvs2[0]}},
		},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	results, err := store.Range(ctx, "")
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

func TestStore_Range_WithValidationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	mockH := newMockHasher("sha256")
	codec := newStoreCodecWithHasher(t, mockH)
	store := newMockedStore(t, codec, driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, []string{"sha256"}, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockH},
		nil,
	)

	value := SimpleStruct{Name: "obj1", Value: 100}
	kvs, err := generator.Generate("object1", value)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	// Only data KV — hash missing → validation error (object skipped).
	// Empty Range fans out one Get per category prefix (value + hash); the
	// hash-prefix Get returns nothing, which is the missing-hash signal.
	dataKV := kvs[0]

	expectedPrefixes := testNamer.Prefixes("", true)
	require.Len(t, expectedPrefixes, 2)

	expectedOps := make([]operation.Operation, 0, len(expectedPrefixes))
	for _, prefix := range expectedPrefixes {
		expectedOps = append(expectedOps, makeAbsoluteGetOp(prefix))
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: []kv.KeyValue{dataKV}}, // value-prefix Get: data present.
			{Values: nil},                   // hash-prefix Get: empty.
		},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	results, err := store.Range(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, results)

	driverMock.MinimockFinish()
}

func TestStore_Range_WithIgnoreVerificationError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)

	mockH := newMockHasher("sha256")
	codec := newStoreCodecWithHasher(t, mockH)
	store := newMockedStore(t, codec, driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, []string{"sha256"}, nil)
	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
		marshaller.NewTypedYamlMarshaller[SimpleStruct](),
		[]hasher.Hasher{mockH},
		nil,
	)

	value := SimpleStruct{Name: "obj1", Value: 100}
	kvs, err := generator.Generate("object1", value)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	dataKV := kvs[0]

	expectedPrefixes := testNamer.Prefixes("", true)
	require.Len(t, expectedPrefixes, 2)

	expectedOps := make([]operation.Operation, 0, len(expectedPrefixes))
	for _, prefix := range expectedPrefixes {
		expectedOps = append(expectedOps, makeAbsoluteGetOp(prefix))
	}

	response := tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: []kv.KeyValue{dataKV}}, // value-prefix Get: data present.
			{Values: nil},                   // hash-prefix Get: empty.
		},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	results, err := store.Range(ctx, "", integrity.IgnoreVerificationError())
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

func TestStore_Range_ExecutionError(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	testNamer := newLayeredNamer(t, nil, nil)
	expectedPrefix := testNamer.Prefix("", true)
	expectedOps := []operation.Operation{makeAbsoluteGetOp(expectedPrefix)}

	expectedError := errors.New("driver execution failed")

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).
		Return(tx.Response{Succeeded: false, Results: nil}, expectedError)

	results, err := store.Range(ctx, "")
	require.Error(t, err)
	require.Nil(t, results)

	driverMock.MinimockFinish()
}

func TestStore_Range_WithIgnoreVerificationErrorButFailedToDecode(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	store := newMockedStore(t, newStoreCodec(t), driverMock)

	ctx := context.Background()

	// A key that is valid for the new namer: /objects/my-object (relative).
	// The validator will try to decode the malformed YAML and fail.
	kvPair := kv.KeyValue{
		Key:         []byte("/objects/my-object"),
		Value:       []byte("invalid: yaml: [unclosed"),
		ModRevision: 0,
	}

	testNamer := newLayeredNamer(t, nil, nil)
	expectedPrefix := testNamer.Prefix("", true) // "/objects/".
	expectedOps := []operation.Operation{makeAbsoluteGetOp(expectedPrefix)}

	response := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{{Values: []kv.KeyValue{kvPair}}},
	}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	results, err := store.Range(ctx, "", integrity.IgnoreVerificationError())
	require.NoError(t, err)
	require.Empty(t, results)

	driverMock.MinimockFinish()
}

func TestStore_WithNamer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverMock := mocks.NewDriverMock(t)

	var constructorCalled bool

	var capturedObjectLoc string

	var capturedHashLocs []namer.LayeredHashLocation

	var capturedSigLocs []namer.LayeredSigLocation

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().WithObjectLocation("objects").
		WithHasher(newMockHasher("sha256")).
		WithSigner(newMockSigner("rsa")).
		WithNamer(func(
			objectLocation string,
			hashLocations []namer.LayeredHashLocation,
			sigLocations []namer.LayeredSigLocation,
			opts ...namer.LayeredOption,
		) (namer.Namer, error) {
			constructorCalled = true
			capturedObjectLoc = objectLocation
			capturedHashLocs = hashLocations
			capturedSigLocs = sigLocations

			return namer.NewLayeredNamer(objectLocation, hashLocations, sigLocations, opts...)
		}).
		Build()
	require.NoError(t, err)

	prefixed, errPrefix := storage.Prefixed(storeTestPrefix, storage.NewStorage(driverMock))
	require.NoError(t, errPrefix)

	store := codec.Bind(prefixed)

	require.True(t, constructorCalled, "namer constructor should have been called")
	require.Equal(t, "objects", capturedObjectLoc)
	require.Len(t, capturedHashLocs, 1)
	assert.Equal(t, "sha256", capturedHashLocs[0].HasherName)
	require.Len(t, capturedSigLocs, 1)
	assert.Equal(t, "rsa", capturedSigLocs[0].SignerName)

	testNamer := newLayeredNamer(t, []string{"sha256"}, []string{"rsa"})
	keys, err := testNamer.GenerateNames("my-object")
	require.NoError(t, err)
	require.Len(t, keys, 3) // value + hash + signature.

	generator := integrity.NewGenerator[SimpleStruct](
		testNamer,
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
		expectedOps = append(expectedOps, makeAbsolutePutOp(string(kv.Key), kv.Value))
	}

	response := tx.Response{Succeeded: true, Results: []tx.RequestResponse{}}

	driverMock.ExecuteMock.Expect(ctx, []predicate.Predicate{}, expectedOps, nil).Return(response, nil)

	err = store.Put(ctx, "my-object", value)
	require.NoError(t, err)

	driverMock.MinimockFinish()
}

func TestStore_Watch(t *testing.T) {
	t.Parallel()

	t.Run("basic event filtering", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		driverMock := mocks.NewDriverMock(t)

		// Use sha256 hasher so the namer knows the "sha256" location.
		mockH := newMockHasher("sha256")
		codec := newStoreCodecWithHasher(t, mockH)
		store := newMockedStore(t, codec, driverMock)

		// Codec namer (LayeredNamer with sha256): Prefix("my-object", false) = "/objects/my-object"
		// Prefixed wrapper adds "/test" → driver Watch called with "/test/objects/my-object".
		absWatchKey := absKeyStr("/objects/my-object")
		rawCh := make(chan watch.Event, 10)
		cleanup := func() {}

		driverMock.WatchMock.Expect(ctx, absWatchKey).Return(rawCh, cleanup, nil)

		eventCh, err := store.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Prefixed wrapper strips "/test" from event.Prefix before forwarding.
		// Events on rawCh carry absolute keys; Store.Watch goroutine sees stripped keys.
		events := []watch.Event{
			// value key for "my-object" → stripped: /objects/my-object → name "my-object" → passes.
			{Prefix: absKeyStr("/objects/my-object")},
			// value key for "my-object2" → stripped: /objects/my-object2 → name "my-object2" → passes (prefix match).
			{Prefix: absKeyStr("/objects/my-object2")},
			// hash key for "my-object" (sha256) → stripped: /hash/sha256/objects/my-object → name "my-object" → passes.
			{Prefix: absKeyStr("/hash/sha256/objects/my-object")},
			// value key for "other-object" → stripped: /objects/other-object → filtered.
			{Prefix: absKeyStr("/objects/other-object")},
		}

		for _, e := range events {
			rawCh <- e
		}

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
		// Events are forwarded with prefix already stripped (Prefixed wrapper does it).
		assert.Equal(t, []byte("/objects/my-object"), received[0].Prefix)
		assert.Equal(t, []byte("/objects/my-object2"), received[1].Prefix)
		assert.Equal(t, []byte("/hash/sha256/objects/my-object"), received[2].Prefix)

		driverMock.MinimockFinish()
	})

	t.Run("raw channel closure", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		driverMock := mocks.NewDriverMock(t)

		codec := newStoreCodec(t)
		store := newMockedStore(t, codec, driverMock)

		absWatchKey := absKeyStr("/objects/my-object")
		rawCh := make(chan watch.Event)
		cleanup := func() {}

		driverMock.WatchMock.Expect(ctx, absWatchKey).Return(rawCh, cleanup, nil)

		eventCh, err := store.Watch(ctx, "my-object")
		require.NoError(t, err)

		close(rawCh)

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

		codec := newStoreCodec(t)
		store := newMockedStore(t, codec, driverMock)

		absWatchKey := absKeyStr("/objects/my-object")
		rawCh := make(chan watch.Event, 1)
		cleanup := func() {}

		driverMock.WatchMock.Expect(ctx, absWatchKey).Return(rawCh, cleanup, nil)

		eventCh, err := store.Watch(ctx, "my-object")
		require.NoError(t, err)

		// After stripping "/test", this becomes "/invalid/key" which ParseKey rejects
		// (no "invalid" location in namer).
		rawCh <- watch.Event{Prefix: absKeyStr("/invalid/key")}

		select {
		case <-eventCh:
			t.Fatal("unexpected event forwarded")
		case <-time.After(100 * time.Millisecond):
		}

		driverMock.MinimockFinish()
	})

	t.Run("context cancellation while waiting", func(t *testing.T) {
		t.Parallel()

		driverMock := mocks.NewDriverMock(t)
		codec := newStoreCodec(t)
		store := newMockedStore(t, codec, driverMock)

		absWatchKey := absKeyStr("/objects/my-object")
		rawCh := make(chan watch.Event)
		cleanup := func() {}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		driverMock.WatchMock.Expect(ctx, absWatchKey).Return(rawCh, cleanup, nil)

		eventCh, err := store.Watch(ctx, "my-object")
		require.NoError(t, err)

		cancel()

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
		codec := newStoreCodec(t)
		store := newMockedStore(t, codec, driverMock)

		absWatchKey := absKeyStr("/objects/my-object")
		rawCh := make(chan watch.Event, 1)
		cleanup := func() {}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		driverMock.WatchMock.Expect(ctx, absWatchKey).Return(rawCh, cleanup, nil)

		eventCh, err := store.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Valid key but not matching "my-object" — filtered. Goroutine loops back.
		rawCh <- watch.Event{Prefix: absKeyStr("/objects/other-object")}

		cancel()
		time.Sleep(50 * time.Millisecond)

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
		codec := newStoreCodec(t)
		store := newMockedStore(t, codec, driverMock)

		absWatchKey := absKeyStr("/objects/my-object")
		rawCh := make(chan watch.Event, 1)
		cleanup := func() {}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		driverMock.WatchMock.Expect(ctx, absWatchKey).Return(rawCh, cleanup, nil)

		eventCh, err := store.Watch(ctx, "my-object")
		require.NoError(t, err)

		// Valid matching event → goroutine enters inner select, blocks on unbuffered filteredCh.
		rawCh <- watch.Event{Prefix: absKeyStr("/objects/my-object")}

		time.Sleep(10 * time.Millisecond)

		cancel()

		select {
		case _, ok := <-eventCh:
			assert.False(t, ok, "channel should be closed after context cancellation")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected channel to be closed after context cancellation")
		}

		driverMock.MinimockFinish()
	})
}

// Two codecs of different types share one *Tx. The inner storage must see
// exactly one Execute call so the multi-type write is genuinely atomic — a
// guarantee Store[T] (single-codec) cannot offer on its own.
func TestStore_CrossCodec_AtomicCommit(t *testing.T) {
	t.Parallel()

	type UserVal struct {
		ID   int    `yaml:"id"`
		Name string `yaml:"name"`
	}

	type OrderVal struct {
		ID     int    `yaml:"id"`
		UserID int    `yaml:"userid"`
		Item   string `yaml:"item"`
	}

	callCount := 0

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Set(func(
		_ context.Context,
		_ []predicate.Predicate,
		thenOps []operation.Operation,
		_ []operation.Operation,
	) (tx.Response, error) {
		callCount++

		return tx.Response{
			Succeeded: true,
			Results:   make([]tx.RequestResponse, len(thenOps)),
		}, nil
	})

	base := storage.NewStorage(driverMock)

	userCodec, err := integrity.NewCodecBuilder[UserVal]().WithObjectLocation("objects").
		WithObjectLocation("users").
		Build()
	require.NoError(t, err)

	orderCodec, err := integrity.NewCodecBuilder[OrderVal]().WithObjectLocation("objects").
		WithObjectLocation("orders").
		Build()
	require.NoError(t, err)

	userStore := userCodec.Bind(base)
	orderStore := orderCodec.Bind(base)

	sharedTx := integrity.NewTx(base)

	err = userCodec.TxPut(sharedTx, "user-1", UserVal{ID: 1, Name: "Alice"})
	require.NoError(t, err)

	err = orderCodec.TxPut(sharedTx, "order-1", OrderVal{ID: 1, UserID: 1, Item: "widget"})
	require.NoError(t, err)

	rsp, err := sharedTx.Commit(context.Background())
	require.NoError(t, err)
	assert.True(t, rsp.Succeeded)

	assert.Equal(t, 1, callCount, "storage Execute must be called exactly once for a multi-codec tx")

	_ = userStore
	_ = orderStore

	driverMock.MinimockFinish()
}

// Compile-time check: dummy.New() used in newTestStorage (tx_test.go) is available.
var _ = dummy.New
