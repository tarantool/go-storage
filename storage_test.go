package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/internal/mocks"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

func TestStorage_Tx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)

	txInstance := strg.Tx(ctx)

	assert.NotNil(t, txInstance)
}

func TestTx_If(t *testing.T) {
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

func TestTx_If_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	result := txInstance.If()

	assert.Equal(t, txInstance, result, "If with empty predicates should return the same tx instance")
}

func TestTx_Then(t *testing.T) {
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

func TestTx_Else(t *testing.T) {
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

func TestTx_Commit_Success(t *testing.T) {
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

func TestTx_Commit_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)
	txInstance := strg.Tx(ctx)

	pred := predicate.ValueEqual([]byte("key"), "value")
	thenOp := operation.Put([]byte("key"), []byte("new-value"))

	txInstance.If(pred).Then(thenOp)

	expectedError := errors.New("driver execution failed")

	mockDriver.ExecuteMock.Expect(ctx,
		[]predicate.Predicate{pred},
		[]operation.Operation{thenOp},
		nil,
	).Return(tx.Response{Succeeded: false, Results: []tx.RequestResponse{}}, expectedError)

	resp, err := txInstance.Commit()

	require.Error(t, err)
	require.EqualError(t, err, "tx execute failed: driver execution failed")

	assert.False(t, resp.Succeeded)
	assert.Nil(t, resp.Results)
	mockDriver.MinimockFinish()
}

func TestTx_DoubleCall(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)

	t.Run("if", func(t *testing.T) {
		t.Parallel()

		pred1 := predicate.ValueEqual([]byte("key1"), "value1")

		txInstance := strg.Tx(ctx)

		require.Panics(t, func() {
			_ = txInstance.
				If(pred1).
				If(predicate.ValueEqual([]byte("key2"), "value2"))
		})
	})

	t.Run("then", func(t *testing.T) {
		t.Parallel()

		thenOp1 := operation.Put([]byte("key1"), []byte("new-value1"))

		txInstance := strg.Tx(ctx)

		require.Panics(t, func() {
			_ = txInstance.
				Then(thenOp1).
				Then(operation.Put([]byte("key2"), []byte("new-value2")))
		})
	})

	t.Run("else", func(t *testing.T) {
		t.Parallel()

		elseOp := operation.Delete([]byte("key1"))

		txInstance := strg.Tx(ctx)

		require.Panics(t, func() {
			_ = txInstance.
				Else(elseOp).
				Else(operation.Delete([]byte("key2")))
		})
	})
}

func TestTx_OrderValidation_Invalid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)

	t.Run("if after then", func(t *testing.T) {
		t.Parallel()

		pred := predicate.ValueEqual([]byte("key"), "value")
		thenOp := operation.Put([]byte("key"), []byte("new-value"))

		txInstance := strg.Tx(ctx)

		require.Panics(t, func() {
			_ = txInstance.
				Then(thenOp).
				If(pred)
		})
	})

	t.Run("if after else", func(t *testing.T) {
		t.Parallel()

		pred := predicate.ValueEqual([]byte("key"), "value")
		elseOp := operation.Delete([]byte("key"))

		txInstance := strg.Tx(ctx)

		require.Panics(t, func() {
			_ = txInstance.
				Else(elseOp).
				If(pred)
		})
	})

	t.Run("then after else", func(t *testing.T) {
		t.Parallel()

		thenOp := operation.Put([]byte("key"), []byte("new-value"))
		elseOp := operation.Delete([]byte("key"))

		txInstance := strg.Tx(ctx)

		require.Panics(t, func() {
			_ = txInstance.
				Else(elseOp).
				Then(thenOp)
		})
	})
}

func TestTx_OrderValidation_Valid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDriver := mocks.NewDriverMock(t)
	strg := storage.NewStorage(mockDriver)

	t.Run("valid order if-then-else", func(t *testing.T) {
		t.Parallel()

		pred := predicate.ValueEqual([]byte("key"), "value")
		thenOp := operation.Put([]byte("key"), []byte("new-value"))
		elseOp := operation.Delete([]byte("key"))

		txInstance := strg.Tx(ctx)

		require.NotPanics(t, func() {
			_ = txInstance.
				If(pred).
				Then(thenOp).
				Else(elseOp)
		})
	})

	t.Run("valid order if-then", func(t *testing.T) {
		t.Parallel()

		pred := predicate.ValueEqual([]byte("key"), "value")
		thenOp := operation.Put([]byte("key"), []byte("new-value"))

		txInstance := strg.Tx(ctx)

		require.NotPanics(t, func() {
			_ = txInstance.
				If(pred).
				Then(thenOp)
		})
	})

	t.Run("valid order then-else", func(t *testing.T) {
		t.Parallel()

		thenOp := operation.Put([]byte("key"), []byte("new-value"))
		elseOp := operation.Delete([]byte("key"))

		txInstance := strg.Tx(ctx)

		require.NotPanics(t, func() {
			_ = txInstance.
				Then(thenOp).
				Else(elseOp)
		})
	})
}

func TestTx_Commit_NoOperations(t *testing.T) {
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
		nil,
		nil,
	).Return(expectedResponse, nil)

	resp, err := txInstance.Commit()

	require.NoError(t, err)
	assert.Equal(t, expectedResponse, resp)
	mockDriver.MinimockFinish()
}

func TestStorage_Watch(t *testing.T) {
	t.Parallel()

	t.Run("driver returns error", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		expectedErr := errors.New("watch failed")
		mockDriver.WatchMock.Expect(ctx, []byte("key")).Return((<-chan watch.Event)(nil), nil, expectedErr)

		eventCh := strg.Watch(ctx, []byte("key"))
		require.NotNil(t, eventCh)

		// Channel should be closed immediately.
		select {
		case _, ok := <-eventCh:
			assert.False(t, ok, "channel should be closed")
		case <-time.After(200 * time.Millisecond):
			t.Fatal("expected closed channel")
		}

		mockDriver.MinimockFinish()
	})

	t.Run("driver returns channel without cleanup", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		rawCh := make(chan watch.Event, 1)
		defer close(rawCh)

		mockDriver.WatchMock.Expect(ctx, []byte("key")).Return(rawCh, nil, nil)

		eventCh := strg.Watch(ctx, []byte("key"))
		require.NotNil(t, eventCh)

		// Give forwarding goroutine a moment to start.
		time.Sleep(20 * time.Millisecond)

		// Send an event.
		event := watch.Event{Prefix: []byte("key")}
		rawCh <- event

		select {
		case received := <-eventCh:
			assert.Equal(t, event, received)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected event")
		}

		mockDriver.MinimockFinish()
	})

	t.Run("driver returns channel with cleanup", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		rawCh := make(chan watch.Event, 1)
		cleanupCh := make(chan struct{})
		cleanup := func() { close(cleanupCh) }

		mockDriver.WatchMock.Expect(ctx, []byte("key")).Return(rawCh, cleanup, nil)

		eventCh := strg.Watch(ctx, []byte("key"))
		require.NotNil(t, eventCh)

		// Give forwarding goroutine a moment to start.
		time.Sleep(10 * time.Millisecond)

		// Send an event.
		event := watch.Event{Prefix: []byte("key")}
		rawCh <- event

		select {
		case received := <-eventCh:
			assert.Equal(t, event, received)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("expected event")
		}

		// Close raw channel to trigger cleanup in forwarding goroutine.
		close(rawCh)

		// Wait for cleanup to be called.
		select {
		case <-cleanupCh:
			// Cleanup called successfully.
		case <-time.After(200 * time.Millisecond):
			t.Fatal("cleanup should have been called")
		}

		mockDriver.MinimockFinish()
	})

	t.Run("context cancellation triggers cleanup", func(t *testing.T) {
		t.Parallel()

		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		rawCh := make(chan watch.Event, 1)
		cleanupCh := make(chan struct{})
		cleanup := func() { close(cleanupCh) }

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockDriver.WatchMock.Expect(ctx, []byte("key")).Return(rawCh, cleanup, nil)

		eventCh := strg.Watch(ctx, []byte("key"))
		require.NotNil(t, eventCh)

		// Give cleanup goroutine a moment to start.
		time.Sleep(1 * time.Millisecond)

		// Cancel context.
		cancel()

		// Wait for cleanup to be called.
		select {
		case <-cleanupCh:
			// Cleanup called successfully.
		case <-time.After(200 * time.Millisecond):
			t.Fatal("cleanup should have been called on context cancellation")
		}

		mockDriver.MinimockFinish()
	})
}

func TestStorage_Range(t *testing.T) {
	t.Parallel()

	t.Run("empty prefix returns nil", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		kvs, err := strg.Range(ctx)
		require.NoError(t, err)
		assert.Nil(t, kvs)

		mockDriver.MinimockFinish()
	})

	t.Run("with prefix", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		expectedKVs := []kv.KeyValue{
			{Key: []byte("/test/key1"), Value: []byte("value1"), ModRevision: 0},
			{Key: []byte("/test/key2"), Value: []byte("value2"), ModRevision: 0},
		}

		ops := []operation.Operation{operation.Get([]byte("/test/"))}
		response := tx.Response{
			Succeeded: true,
			Results: []tx.RequestResponse{
				{Values: expectedKVs},
			},
		}

		mockDriver.ExecuteMock.Expect(ctx, nil, ops, nil).Return(response, nil)

		kvs, err := strg.Range(ctx, storage.WithPrefix("/test"))
		require.NoError(t, err)
		assert.Equal(t, expectedKVs, kvs)

		mockDriver.MinimockFinish()
	})

	t.Run("with prefix without trailing slash", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		expectedKVs := []kv.KeyValue{
			{Key: []byte("/test/key1"), Value: []byte("value1"), ModRevision: 0},
		}

		ops := []operation.Operation{operation.Get([]byte("/test/"))}
		response := tx.Response{
			Succeeded: true,
			Results: []tx.RequestResponse{
				{Values: expectedKVs},
			},
		}

		mockDriver.ExecuteMock.Expect(ctx, nil, ops, nil).Return(response, nil)

		kvs, err := strg.Range(ctx, storage.WithPrefix("/test"))
		require.NoError(t, err)
		assert.Equal(t, expectedKVs, kvs)

		mockDriver.MinimockFinish()
	})

	t.Run("driver execution error", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		mockDriver := mocks.NewDriverMock(t)
		strg := storage.NewStorage(mockDriver)

		ops := []operation.Operation{operation.Get([]byte("/test/"))}
		expectedErr := errors.New("driver error")
		mockDriver.ExecuteMock.Expect(ctx, nil, ops, nil).
			Return(tx.Response{Succeeded: false, Results: nil}, expectedErr)

		kvs, err := strg.Range(ctx, storage.WithPrefix("/test"))
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to execute ops")
		assert.Nil(t, kvs)

		mockDriver.MinimockFinish()
	})
}
