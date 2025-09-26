package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/internal/mocks"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
)

func TestStorage_Tx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)

	txInstance := strg.Tx(ctx)

	assert.NotNil(t, txInstance)
}

func TestTxBuilder_If(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	pred1 := predicate.ValueEqual([]byte("key1"), "value1")
	pred2 := predicate.VersionEqual([]byte("key2"), int64(42))

	result := txInstance.If(pred1, pred2)

	assert.Equal(t, txInstance, result, "If should return the same tx instance")
}

func TestTxBuilder_If_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	result := txInstance.If()

	assert.Equal(t, txInstance, result, "If with empty predicates should return the same tx instance")
}

func TestTxBuilder_Then(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	op1 := operation.Put([]byte("key1"), []byte("value1"))
	op2 := operation.Delete([]byte("key2"))

	result := txInstance.Then(op1, op2)

	assert.Equal(t, txInstance, result, "Then should return the same tx instance")
}

func TestTxBuilder_Else(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	op1 := operation.Put([]byte("key1"), []byte("value1"))
	op2 := operation.Delete([]byte("key2"))

	result := txInstance.Else(op1, op2)

	assert.Equal(t, txInstance, result, "Else should return the same tx instance")
}

func TestTxBuilder_Commit_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	pred := predicate.ValueEqual([]byte("key"), "value")
	thenOp := operation.Put([]byte("key"), []byte("new-value"))
	elseOp := operation.Delete([]byte("key"))

	txInstance.If(pred).Then(thenOp).Else(elseOp)

	expectedResponse := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	mockDriver.ExecuteMock.Expect(ctx, []predicate.Predicate{pred},
		[]operation.Operation{thenOp}, []operation.Operation{elseOp}).
		Return(expectedResponse, nil)

	resp, err := txInstance.Commit()

	require.NoError(t, err)
	assert.Equal(t, expectedResponse, resp)
	mockDriver.MinimockFinish()
}

func TestTxBuilder_Commit_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	pred := predicate.ValueEqual([]byte("key"), "value")
	thenOp := operation.Put([]byte("key"), []byte("new-value"))

	txInstance.If(pred).Then(thenOp)

	expectedError := errors.New("driver execution failed") //nolint:err113 // Test error is fine

	mockDriver.ExecuteMock.Expect(ctx, []predicate.Predicate{pred},
		[]operation.Operation{thenOp}, []operation.Operation{}).
		Return(tx.Response{Succeeded: false, Results: []tx.RequestResponse{}}, expectedError)

	resp, err := txInstance.Commit()

	require.Error(t, err)
	require.EqualError(t, err, "tx execute failed: driver execution failed")

	assert.False(t, resp.Succeeded)
	assert.Nil(t, resp.Results)
	mockDriver.MinimockFinish()
}

func TestTxBuilder_Chaining(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	pred1 := predicate.ValueEqual([]byte("key1"), "value1")
	pred2 := predicate.VersionEqual([]byte("key2"), int64(42))
	thenOp1 := operation.Put([]byte("key1"), []byte("new-value1"))
	thenOp2 := operation.Put([]byte("key2"), []byte("new-value2"))
	elseOp := operation.Delete([]byte("key1"))

	result := txInstance.
		If(pred1).
		Then(thenOp1).
		Else(elseOp).
		If(pred2).
		Then(thenOp2)

	assert.Equal(t, txInstance, result, "Chaining should return the same tx instance")
}

func TestTxBuilder_Commit_NoOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	txInstance.If(predicate.ValueEqual([]byte("key"), "value"))

	expectedResponse := tx.Response{
		Succeeded: true,
		Results:   []tx.RequestResponse{},
	}

	mockDriver.ExecuteMock.Expect(ctx,
		[]predicate.Predicate{predicate.ValueEqual([]byte("key"), "value")},
		[]operation.Operation{}, []operation.Operation{}).
		Return(expectedResponse, nil)

	resp, err := txInstance.Commit()

	require.NoError(t, err)
	assert.Equal(t, expectedResponse, resp)
	mockDriver.MinimockFinish()
}
