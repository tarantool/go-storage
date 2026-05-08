package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/internal/mocks"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	txPkg "github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

type recordedTxCall struct {
	predicates []predicate.Predicate
	thenOps    []operation.Operation
	elseOps    []operation.Operation
}

type recordingStorage struct {
	lastTx recordedTxCall
	txResp txPkg.Response
	txErr  error

	lastWatchKey []byte
	watchEvents  []watch.Event

	// Range is exercised through DriverMock (see TestPrefixed_Range), so this
	// stub does not implement it.
}

func (s *recordingStorage) Tx(_ context.Context) txPkg.Tx {
	return &recordingTx{rec: s, call: recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil}}
}

func (s *recordingStorage) Range(_ context.Context, _ ...storage.RangeOption) ([]kv.KeyValue, error) {
	panic("recordingStorage.Range: use DriverMock-based tests for Range")
}

func (s *recordingStorage) Watch(_ context.Context, key []byte, _ ...watch.Option) <-chan watch.Event {
	s.lastWatchKey = key

	watchCh := make(chan watch.Event, len(s.watchEvents)+1)
	for _, e := range s.watchEvents {
		watchCh <- e
	}

	// Closed so the Prefixed forwarder goroutine exits after draining events.
	close(watchCh)

	return watchCh
}

type recordingTx struct {
	rec  *recordingStorage
	call recordedTxCall
}

func (t *recordingTx) If(preds ...predicate.Predicate) txPkg.Tx {
	t.call.predicates = append(t.call.predicates, preds...)
	return t
}

func (t *recordingTx) Then(ops ...operation.Operation) txPkg.Tx {
	t.call.thenOps = append(t.call.thenOps, ops...)
	return t
}

func (t *recordingTx) Else(ops ...operation.Operation) txPkg.Tx {
	t.call.elseOps = append(t.call.elseOps, ops...)
	return t
}

func (t *recordingTx) Commit() (txPkg.Response, error) {
	t.rec.lastTx = t.call
	return t.rec.txResp, t.rec.txErr
}

func mustPrefix(t *testing.T, prefix string, inner storage.Storage) storage.Storage {
	t.Helper()

	s, err := storage.Prefixed(prefix, inner)
	require.NoError(t, err)

	return s
}

func TestPrefixed_RejectsTrailingSlash(t *testing.T) {
	t.Parallel()

	inner := &recordingStorage{
		lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
		txResp:       txPkg.Response{Succeeded: true, Results: nil},
		txErr:        nil,
		lastWatchKey: nil,
		watchEvents:  nil,
	}

	cases := []string{"/", "/ns/", "/a/b/"}
	for _, prefix := range cases {
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()

			s, err := storage.Prefixed(prefix, inner)
			assert.Nil(t, s)
			require.ErrorIs(t, err, storage.ErrPrefixTrailingSlash)
		})
	}
}

func TestPrefixed_OperationKeyPrefixing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const namespace = "/ns"

	t.Run("Put in Then", func(t *testing.T) {
		t.Parallel()

		inner := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		_, err := prefixedStore.Tx(ctx).
			Then(operation.Put([]byte("key"), []byte("val"))).
			Commit()
		require.NoError(t, err)

		require.Len(t, inner.lastTx.thenOps, 1)

		op := inner.lastTx.thenOps[0]
		assert.Equal(t, operation.TypePut, op.Type())
		assert.Equal(t, []byte(namespace+"key"), op.Key())
		assert.Equal(t, []byte("val"), op.Value())
	})

	t.Run("Get in Then", func(t *testing.T) {
		t.Parallel()

		inner := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		_, err := prefixedStore.Tx(ctx).
			Then(operation.Get([]byte("foo"))).
			Commit()
		require.NoError(t, err)

		require.Len(t, inner.lastTx.thenOps, 1)

		op := inner.lastTx.thenOps[0]
		assert.Equal(t, operation.TypeGet, op.Type())
		assert.Equal(t, []byte(namespace+"foo"), op.Key())
	})

	t.Run("Delete in Else", func(t *testing.T) {
		t.Parallel()

		inner := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		_, err := prefixedStore.Tx(ctx).
			Else(operation.Delete([]byte("bar"))).
			Commit()
		require.NoError(t, err)

		require.Len(t, inner.lastTx.elseOps, 1)

		op := inner.lastTx.elseOps[0]
		assert.Equal(t, operation.TypeDelete, op.Type())
		assert.Equal(t, []byte(namespace+"bar"), op.Key())
	})

	t.Run("empty prefix is transparent", func(t *testing.T) {
		t.Parallel()

		inner := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, "", inner)

		_, err := prefixedStore.Tx(ctx).
			Then(operation.Put([]byte("key"), []byte("val"))).
			Commit()
		require.NoError(t, err)

		require.Len(t, inner.lastTx.thenOps, 1)
		assert.Equal(t, []byte("key"), inner.lastTx.thenOps[0].Key())
	})

	t.Run("multiple ops in Then and Else", func(t *testing.T) {
		t.Parallel()

		inner := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		_, err := prefixedStore.Tx(ctx).
			Then(
				operation.Put([]byte("a"), []byte("1")),
				operation.Get([]byte("b")),
			).
			Else(
				operation.Delete([]byte("c")),
			).
			Commit()
		require.NoError(t, err)

		require.Len(t, inner.lastTx.thenOps, 2)
		assert.Equal(t, []byte(namespace+"a"), inner.lastTx.thenOps[0].Key())
		assert.Equal(t, []byte(namespace+"b"), inner.lastTx.thenOps[1].Key())

		require.Len(t, inner.lastTx.elseOps, 1)
		assert.Equal(t, []byte(namespace+"c"), inner.lastTx.elseOps[0].Key())
	})
}

func TestPrefixed_PredicateKeyPrefixing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const namespace = "/pfx"

	tests := []struct {
		name   string
		build  func(key []byte) predicate.Predicate
		op     predicate.Op
		target predicate.Target
		val    any
	}{
		{
			name:   "ValueEqual",
			build:  func(k []byte) predicate.Predicate { return predicate.ValueEqual(k, []byte("v")) },
			op:     predicate.OpEqual,
			target: predicate.TargetValue,
			val:    []byte("v"),
		},
		{
			name:   "ValueNotEqual",
			build:  func(k []byte) predicate.Predicate { return predicate.ValueNotEqual(k, []byte("v")) },
			op:     predicate.OpNotEqual,
			target: predicate.TargetValue,
			val:    []byte("v"),
		},
		{
			name:   "VersionEqual",
			build:  func(k []byte) predicate.Predicate { return predicate.VersionEqual(k, int64(42)) },
			op:     predicate.OpEqual,
			target: predicate.TargetVersion,
			val:    int64(42),
		},
		{
			name:   "VersionNotEqual",
			build:  func(k []byte) predicate.Predicate { return predicate.VersionNotEqual(k, int64(7)) },
			op:     predicate.OpNotEqual,
			target: predicate.TargetVersion,
			val:    int64(7),
		},
		{
			name:   "VersionGreater",
			build:  func(k []byte) predicate.Predicate { return predicate.VersionGreater(k, int64(10)) },
			op:     predicate.OpGreater,
			target: predicate.TargetVersion,
			val:    int64(10),
		},
		{
			name:   "VersionLess",
			build:  func(k []byte) predicate.Predicate { return predicate.VersionLess(k, int64(5)) },
			op:     predicate.OpLess,
			target: predicate.TargetVersion,
			val:    int64(5),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			inner := &recordingStorage{
				lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
				txResp:       txPkg.Response{Succeeded: true, Results: nil},
				txErr:        nil,
				lastWatchKey: nil,
				watchEvents:  nil,
			}
			prefixedStore := mustPrefix(t, namespace, inner)

			callerKey := []byte("mykey")
			pred := testCase.build(callerKey)

			_, err := prefixedStore.Tx(ctx).
				If(pred).
				Then(operation.Put(callerKey, []byte("val"))).
				Commit()
			require.NoError(t, err)

			require.Len(t, inner.lastTx.predicates, 1)

			got := inner.lastTx.predicates[0]

			assert.Equal(t, []byte(namespace+"mykey"), got.Key(),
				"predicate key must have namespace prefix prepended")
			assert.Equal(t, testCase.op, got.Operation(), "Op must be preserved")
			assert.Equal(t, testCase.target, got.Target(), "Target must be preserved")
			assert.Equal(t, testCase.val, got.Value(), "Value must be preserved")
		})
	}
}

func TestPrefixed_Range(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Range uses DriverMock because storage.go translates WithPrefix(p) into
	// operation.Get([]byte(p+"/")), and we want to assert the combined key the
	// driver actually sees.

	t.Run("WithPrefix concatenates namespace prefix", func(t *testing.T) {
		t.Parallel()

		mockDriver := mocks.NewDriverMock(t)
		inner := storage.NewStorage(mockDriver)
		prefixedStore := mustPrefix(t, "/test", inner)

		// "/test" + "/foo" + trailing "/" added by storage.go.
		combinedKey := []byte("/test/foo/")
		absKVs := []kv.KeyValue{
			{Key: []byte("/test/foo/a"), Value: []byte("1"), ModRevision: 0},
			{Key: []byte("/test/foo/b"), Value: []byte("2"), ModRevision: 0},
		}

		mockDriver.ExecuteMock.
			Expect(ctx, nil, []operation.Operation{operation.Get(combinedKey)}, nil).
			Return(txPkg.Response{
				Succeeded: true,
				Results:   []txPkg.RequestResponse{{Values: absKVs}},
			}, nil)

		kvs, err := prefixedStore.Range(ctx, storage.WithPrefix("/foo"))
		require.NoError(t, err)

		require.Len(t, kvs, 2)
		assert.Equal(t, []byte("/foo/a"), kvs[0].Key)
		assert.Equal(t, []byte("/foo/b"), kvs[1].Key)

		mockDriver.MinimockFinish()
	})

	t.Run("no WithPrefix passes nothing to inner", func(t *testing.T) {
		t.Parallel()

		// Without WithPrefix the wrapper has nothing to rewrite, so it must
		// not invoke the driver at all — hence no ExecuteMock expectation.
		mockDriver := mocks.NewDriverMock(t)
		inner := storage.NewStorage(mockDriver)
		prefixedStore := mustPrefix(t, "/test", inner)

		kvs, err := prefixedStore.Range(ctx)
		require.NoError(t, err)
		assert.Nil(t, kvs)

		mockDriver.MinimockFinish()
	})

	t.Run("result keys have namespace prefix stripped", func(t *testing.T) {
		t.Parallel()

		mockDriver := mocks.NewDriverMock(t)
		inner := storage.NewStorage(mockDriver)
		prefixedStore := mustPrefix(t, "/ns", inner)

		absKVs := []kv.KeyValue{
			{Key: []byte("/ns/obj/x"), Value: []byte("val-x"), ModRevision: 1},
		}

		mockDriver.ExecuteMock.
			Expect(ctx, nil, []operation.Operation{operation.Get([]byte("/ns/obj/"))}, nil).
			Return(txPkg.Response{
				Succeeded: true,
				Results:   []txPkg.RequestResponse{{Values: absKVs}},
			}, nil)

		kvs, err := prefixedStore.Range(ctx, storage.WithPrefix("/obj"))
		require.NoError(t, err)
		require.Len(t, kvs, 1)
		assert.Equal(t, []byte("/obj/x"), kvs[0].Key)
		assert.Equal(t, []byte("val-x"), kvs[0].Value)
		assert.Equal(t, int64(1), kvs[0].ModRevision)

		mockDriver.MinimockFinish()
	})
}

func TestPrefixed_Watch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const namespace = "/test"

	t.Run("Watch key is prefixed and event Prefix is stripped", func(t *testing.T) {
		t.Parallel()

		// concatKey concatenates raw bytes with no separator: "/test"+"foo"
		// = "/testfoo". The inner emits an event keyed at the absolute path,
		// which the wrapper strips back down to "foo" for the caller.
		inner := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: false, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents: []watch.Event{
				{Prefix: []byte("/testfoo")},
			},
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		watchCh := prefixedStore.Watch(ctx, []byte("foo"))

		assert.Equal(t, []byte("/testfoo"), inner.lastWatchKey)

		select {
		case event, ok := <-watchCh:
			require.True(t, ok)
			assert.Equal(t, []byte("foo"), event.Prefix,
				"event Prefix must have namespace prefix stripped")
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected event from Watch channel")
		}
	})

	t.Run("Watch channel closes when inner channel closes", func(t *testing.T) {
		t.Parallel()

		inner := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: false, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		watchCh := prefixedStore.Watch(ctx, []byte("k"))

		// The recording stub buffers an empty event slice and immediately
		// closes the inner channel; the wrapper goroutine should propagate
		// the close to the caller-facing channel.
		select {
		case _, ok := <-watchCh:
			assert.False(t, ok, "channel should be closed when inner closes")
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected channel to close")
		}
	})

	t.Run("Watch channel closes on context cancellation", func(t *testing.T) {
		t.Parallel()

		// blockCh stays open until the deferred close below, so the wrapper
		// goroutine has nothing to drain and can only exit via ctx.Done().
		blockCh := make(chan watch.Event)
		blockInner := &blockingWatchStorage{ch: blockCh}
		prefixedStore := mustPrefix(t, namespace, blockInner)

		ctx2, cancel := context.WithCancel(ctx)

		watchCh := prefixedStore.Watch(ctx2, []byte("k"))

		// Yield so the wrapper goroutine reaches its select before cancel.
		time.Sleep(10 * time.Millisecond)
		cancel()

		select {
		case _, ok := <-watchCh:
			assert.False(t, ok, "channel should be closed on context cancellation")
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected channel to close after context cancellation")
		}

		close(blockCh)
	})
}

type blockingWatchStorage struct {
	ch <-chan watch.Event
}

func (b *blockingWatchStorage) Tx(_ context.Context) txPkg.Tx {
	return &recordingTx{
		rec: &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: false, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		},
		call: recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
	}
}

func (b *blockingWatchStorage) Range(_ context.Context, _ ...storage.RangeOption) ([]kv.KeyValue, error) {
	return nil, nil
}

func (b *blockingWatchStorage) Watch(_ context.Context, _ []byte, _ ...watch.Option) <-chan watch.Event {
	return b.ch
}

func TestPrefixed_Composition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("flattened at construction: Put lands at combined prefix", func(t *testing.T) {
		t.Parallel()

		base := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		composed := mustPrefix(t, "/a", mustPrefix(t, "/b", base))

		_, err := composed.Tx(ctx).
			Then(operation.Put([]byte("k"), []byte("v"))).
			Commit()
		require.NoError(t, err)

		require.Len(t, base.lastTx.thenOps, 1)
		assert.Equal(t, []byte("/a/bk"), base.lastTx.thenOps[0].Key(),
			"composed Put key must equal /a/b concatenated with the caller key")
	})

	t.Run("equivalent to single Prefixed with concatenated prefix", func(t *testing.T) {
		t.Parallel()

		base1 := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		base2 := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}

		composed := mustPrefix(t, "/a", mustPrefix(t, "/b", base1))
		flat := mustPrefix(t, "/a/b", base2)

		callerKey := []byte("/obj/name")
		callerVal := []byte("data")

		_, err1 := composed.Tx(ctx).Then(operation.Put(callerKey, callerVal)).Commit()
		_, err2 := flat.Tx(ctx).Then(operation.Put(callerKey, callerVal)).Commit()

		require.NoError(t, err1)
		require.NoError(t, err2)

		require.Len(t, base1.lastTx.thenOps, 1)
		require.Len(t, base2.lastTx.thenOps, 1)

		assert.Equal(t, base1.lastTx.thenOps[0].Key(), base2.lastTx.thenOps[0].Key(),
			"composed and flat prefixes must produce identical inner keys")
	})

	t.Run("three levels", func(t *testing.T) {
		t.Parallel()

		base := &recordingStorage{
			lastTx:       recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp:       txPkg.Response{Succeeded: true, Results: nil},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		composed := mustPrefix(t, "/a", mustPrefix(t, "/b", mustPrefix(t, "/c", base)))

		_, err := composed.Tx(ctx).
			Then(operation.Put([]byte("x"), []byte("v"))).
			Commit()
		require.NoError(t, err)

		require.Len(t, base.lastTx.thenOps, 1)
		assert.Equal(t, []byte("/a/b/cx"), base.lastTx.thenOps[0].Key())
	})
}

func TestPrefixed_ResultKeyStripping(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const namespace = "/ns"

	t.Run("Tx.Commit Values keys are stripped", func(t *testing.T) {
		t.Parallel()

		inner := &recordingStorage{
			lastTx: recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp: txPkg.Response{
				Succeeded: true,
				Results: []txPkg.RequestResponse{
					{Values: []kv.KeyValue{
						{Key: []byte("/ns/a"), Value: []byte("1"), ModRevision: 10},
						{Key: []byte("/ns/b"), Value: []byte("2"), ModRevision: 20},
					}},
					{Values: []kv.KeyValue{
						{Key: []byte("/ns/c"), Value: []byte("3"), ModRevision: 30},
					}},
				},
			},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		resp, err := prefixedStore.Tx(ctx).
			Then(operation.Get([]byte("a")), operation.Get([]byte("c"))).
			Commit()
		require.NoError(t, err)

		require.Len(t, resp.Results, 2)
		require.Len(t, resp.Results[0].Values, 2)
		assert.Equal(t, []byte("/a"), resp.Results[0].Values[0].Key)
		assert.Equal(t, []byte("/b"), resp.Results[0].Values[1].Key)

		require.Len(t, resp.Results[1].Values, 1)
		assert.Equal(t, []byte("/c"), resp.Results[1].Values[0].Key)
	})

	t.Run("non-prefixed keys in results are returned unchanged", func(t *testing.T) {
		t.Parallel()

		// If the inner storage returns a key that does not carry the expected
		// prefix (e.g. a driver bug), the wrapper must not corrupt it.
		inner := &recordingStorage{
			lastTx: recordedTxCall{predicates: nil, thenOps: nil, elseOps: nil},
			txResp: txPkg.Response{
				Succeeded: true,
				Results: []txPkg.RequestResponse{
					{Values: []kv.KeyValue{
						{Key: []byte("/other/x"), Value: []byte("v"), ModRevision: 0},
					}},
				},
			},
			txErr:        nil,
			lastWatchKey: nil,
			watchEvents:  nil,
		}
		prefixedStore := mustPrefix(t, namespace, inner)

		resp, err := prefixedStore.Tx(ctx).Then(operation.Get([]byte("x"))).Commit()
		require.NoError(t, err)

		require.Len(t, resp.Results[0].Values, 1)
		assert.Equal(t, []byte("/other/x"), resp.Results[0].Values[0].Key)
	})

	t.Run("Range result keys are stripped (via DriverMock)", func(t *testing.T) {
		t.Parallel()

		mockDriver := mocks.NewDriverMock(t)
		inner := storage.NewStorage(mockDriver)
		prefixedStore := mustPrefix(t, "/ns", inner)

		absKVs := []kv.KeyValue{
			{Key: []byte("/ns/objects/myname"), Value: []byte("data"), ModRevision: 1},
		}

		mockDriver.ExecuteMock.
			Expect(ctx, nil, []operation.Operation{operation.Get([]byte("/ns/objects/"))}, nil).
			Return(txPkg.Response{
				Succeeded: true,
				Results:   []txPkg.RequestResponse{{Values: absKVs}},
			}, nil)

		kvs, err := prefixedStore.Range(ctx, storage.WithPrefix("/objects"))
		require.NoError(t, err)
		require.Len(t, kvs, 1)
		assert.Equal(t, []byte("/objects/myname"), kvs[0].Key)

		mockDriver.MinimockFinish()
	})
}
