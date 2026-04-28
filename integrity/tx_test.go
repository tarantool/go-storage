package integrity_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/internal/mocks"
	"github.com/tarantool/go-storage/namer"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	storagetx "github.com/tarantool/go-storage/tx"
)

type txTestValue struct {
	Field string `yaml:"field"`
}

func newTestStorage() storage.Storage {
	return storage.NewStorage(dummy.New())
}

func newTestCodec(t *testing.T) *integrity.Codec[txTestValue] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[txTestValue]().Build()
	require.NoError(t, err)

	return codec
}

func newTestCodecWithHasher(t *testing.T) *integrity.Codec[txTestValue] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[txTestValue]().
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	require.NoError(t, err)

	return codec
}

// putDirect bypasses the codec so a test can seed a key/value pair the codec
// would otherwise refuse to write (e.g. a value with no companion hash).
func putDirect(t *testing.T, store storage.Storage, key, value string) {
	t.Helper()

	rsp, err := store.Tx(context.Background()).
		Then(operation.Put([]byte(key), []byte(value))).
		Commit()
	require.NoError(t, err)
	require.True(t, rsp.Succeeded)
}

func TestNewTx_ThenIdempotent(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	txn := integrity.NewTx(store)

	b1 := txn.Then()
	b2 := txn.Then()

	assert.Same(t, b1, b2, "txn.Then() must return the same *Branch on every call")
}

func TestNewTx_ElseIdempotent(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	txn := integrity.NewTx(store)

	b1 := txn.Else()
	b2 := txn.Else()

	assert.Same(t, b1, b2, "txn.Else() must return the same *Branch on every call")
}

func TestNewTx_ThenAndElseAreDistinct(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	txn := integrity.NewTx(store)

	assert.NotSame(t, txn.Then(), txn.Else(),
		"txn.Then() and txn.Else() must be distinct *Branch values")
}

func TestTx_IfAppends(t *testing.T) {
	t.Parallel()

	executeCount := 0

	var capturedPreds []predicate.Predicate

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Set(func(
		_ context.Context,
		preds []predicate.Predicate,
		thenOps []operation.Operation,
		_ []operation.Operation,
	) (storagetx.Response, error) {
		executeCount++

		capturedPreds = preds

		return storagetx.Response{Succeeded: true, Results: make([]storagetx.RequestResponse, len(thenOps))}, nil
	})

	store := storage.NewStorage(driverMock)

	pred1 := predicate.VersionEqual([]byte("/objects/x"), 1)
	pred2 := predicate.VersionEqual([]byte("/objects/y"), 2)

	txn := integrity.NewTx(store)
	txn.If(pred1).If(pred2)

	_, err := txn.Commit(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, executeCount, "storage Execute must be called exactly once")
	require.Len(t, capturedPreds, 2, "both predicates must reach the driver")
	assert.Equal(t, pred1.Key(), capturedPreds[0].Key())
	assert.Equal(t, pred2.Key(), capturedPreds[1].Key())
}

func TestBranchable_TxRoutesToThen(t *testing.T) {
	t.Parallel()

	codec := newTestCodec(t)
	store := newTestStorage()

	txn := integrity.NewTx(store)
	// Passing txn (not txn.Then()) must route ops onto the Then branch.
	_ = codec.TxPut(txn, "item", txTestValue{Field: "hello"})

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	assert.True(t, rsp.Succeeded)
}

func TestBranchable_BranchRoutesToItself(t *testing.T) {
	t.Parallel()

	codec := newTestCodec(t)
	store := newTestStorage()

	txn := integrity.NewTx(store)

	elseBranch := txn.Else()
	err := codec.TxPut(elseBranch, "item", txTestValue{Field: "on-else"})
	require.NoError(t, err)

	// Predicate must fail so the Else branch is the one that fires.
	pred := predicate.VersionGreater([]byte("/objects/nonexistent"), 999)
	txn.If(pred)

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	assert.False(t, rsp.Succeeded, "else branch should fire when predicate fails")
}

func TestTx_CommitCallsStorageOnce(t *testing.T) {
	t.Parallel()

	callCount := 0

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Set(func(
		_ context.Context,
		_ []predicate.Predicate,
		thenOps []operation.Operation,
		_ []operation.Operation,
	) (storagetx.Response, error) {
		callCount++

		return storagetx.Response{Succeeded: true, Results: make([]storagetx.RequestResponse, len(thenOps))}, nil
	})

	store := storage.NewStorage(driverMock)
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)
	err := codec.TxPut(txn, "a", txTestValue{Field: "1"})
	require.NoError(t, err)

	err = codec.TxPut(txn, "b", txTestValue{Field: "2"})
	require.NoError(t, err)

	_, err = txn.Commit(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, callCount, "storage Execute must be called exactly once")
}

func TestTx_CommitSingleCall(t *testing.T) {
	t.Parallel()

	callCount := 0

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Set(func(
		_ context.Context,
		_ []predicate.Predicate,
		thenOps []operation.Operation,
		_ []operation.Operation,
	) (storagetx.Response, error) {
		callCount++

		return storagetx.Response{Succeeded: true, Results: make([]storagetx.RequestResponse, len(thenOps))}, nil
	})

	store := storage.NewStorage(driverMock)
	txn := integrity.NewTx(store)

	_, err := txn.Commit(context.Background())
	require.NoError(t, err)

	_, err2 := txn.Commit(context.Background())
	require.ErrorIs(t, err2, integrity.ErrTxAlreadyCommitted)
	assert.Equal(t, 1, callCount, "storage must not be called on a second Commit")
}

func TestTx_BuildErrorOnEnqueue_StorageNotCalled(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	// No ExecuteMock expectation: minimock will fail the test if Commit
	// reaches the driver despite the build error.
	driverMock.ExecuteMock.Optional()

	store := storage.NewStorage(driverMock)
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)
	future := codec.TxGet(txn, "")

	_, err := txn.Commit(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	_, futureErr := future.Result()
	require.Error(t, futureErr)
	require.ErrorIs(t, futureErr, integrity.ErrInvalidName)
}

func TestTx_FirstErrorWins(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Optional()

	store := storage.NewStorage(driverMock)
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)

	_ = codec.TxGet(txn, "")
	// Second enqueue would normally surface a different invalid-name reason,
	// but first-error-wins means it stays a no-op.
	_ = codec.TxGet(txn, "/leading-slash")

	_, err := txn.Commit(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrInvalidName,
		"first error (ErrInvalidName) must be the one surfaced by Commit")
}

func TestFuture_BeforeCommit_ReturnsErrTxNotCommitted(t *testing.T) {
	t.Parallel()

	codec := newTestCodec(t)
	store := newTestStorage()
	txn := integrity.NewTx(store)

	future := codec.TxGet(txn, "something")

	_, err := future.Result()
	require.ErrorIs(t, err, integrity.ErrTxNotCommitted,
		"reading a future before Commit must return ErrTxNotCommitted")
}

func TestFuture_ThenBranchNotFired(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)

	thenFuture := codec.TxGet(txn, "item")

	// VersionGreater on a key that doesn't exist is guaranteed to fail.
	txn.If(predicate.VersionGreater([]byte("/objects/nope"), 999))

	_ = codec.TxPut(txn.Else(), "else-item", txTestValue{Field: "e"})

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	assert.False(t, rsp.Succeeded, "predicate must fail")

	_, futureErr := thenFuture.Result()
	require.ErrorIs(t, futureErr, integrity.ErrBranchNotFired,
		"Then future must return ErrBranchNotFired when Else fires")
}

func TestFuture_ElseBranchNotFired(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)

	// No predicates, so Commit always succeeds and Then fires.
	err := codec.TxPut(txn, "item", txTestValue{Field: "v"})
	require.NoError(t, err)

	elseFuture := codec.TxGet(txn.Else(), "else-item")

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	assert.True(t, rsp.Succeeded, "no predicates → Succeeded must be true")

	_, futureErr := elseFuture.Result()
	require.ErrorIs(t, futureErr, integrity.ErrBranchNotFired,
		"Else future must return ErrBranchNotFired when Then fires")
}

func TestTx_ThenElseFiring(t *testing.T) {
	t.Parallel()

	t.Run("then_fires_on_success", func(t *testing.T) {
		t.Parallel()

		store := newTestStorage()
		codec := newTestCodec(t)

		seedTx := integrity.NewTx(store)
		err := codec.TxPut(seedTx, "mykey", txTestValue{Field: "hello"})
		require.NoError(t, err)

		_, err = seedTx.Commit(context.Background())
		require.NoError(t, err)

		readTx := integrity.NewTx(store)
		fut := codec.TxGet(readTx, "mykey")

		_ = codec.TxPut(readTx.Else(), "else-key", txTestValue{Field: "else"})

		rsp, err := readTx.Commit(context.Background())
		require.NoError(t, err)
		assert.True(t, rsp.Succeeded)

		result, err := fut.Result()
		require.NoError(t, err)
		require.True(t, result.Value.IsSome())

		got := result.Value.Unwrap()
		assert.Equal(t, "hello", got.Field)
	})

	t.Run("else_fires_on_failure", func(t *testing.T) {
		t.Parallel()

		store := newTestStorage()
		codec := newTestCodec(t)

		seedTx := integrity.NewTx(store)
		err := codec.TxPut(seedTx, "else-key", txTestValue{Field: "else-value"})
		require.NoError(t, err)

		_, err = seedTx.Commit(context.Background())
		require.NoError(t, err)

		mainTx := integrity.NewTx(store)
		thenFut := codec.TxGet(mainTx, "mykey")
		elseFut := codec.TxGet(mainTx.Else(), "else-key")

		mainTx.If(predicate.VersionGreater([]byte("/objects/nope"), 999))

		rsp, err := mainTx.Commit(context.Background())
		require.NoError(t, err)
		assert.False(t, rsp.Succeeded)

		_, thenErr := thenFut.Result()
		require.ErrorIs(t, thenErr, integrity.ErrBranchNotFired)

		elseResult, err := elseFut.Result()
		require.NoError(t, err)
		require.True(t, elseResult.Value.IsSome())

		got := elseResult.Value.Unwrap()
		assert.Equal(t, "else-value", got.Field)
	})
}

func TestTxPut_RoundTrip(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	putTx := integrity.NewTx(store)
	err := codec.TxPut(putTx, "roundtrip", txTestValue{Field: "rtr"})
	require.NoError(t, err)

	rsp, err := putTx.Commit(context.Background())
	require.NoError(t, err)
	assert.True(t, rsp.Succeeded)

	getTx := integrity.NewTx(store)
	fut := codec.TxGet(getTx, "roundtrip")

	_, err = getTx.Commit(context.Background())
	require.NoError(t, err)

	result, err := fut.Result()
	require.NoError(t, err)
	require.True(t, result.Value.IsSome())

	got := result.Value.Unwrap()
	assert.Equal(t, "rtr", got.Field)
}

func TestTxDelete_WithPrefix(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	seedTx := integrity.NewTx(store)
	require.NoError(t, codec.TxPut(seedTx, "prefix/a", txTestValue{Field: "a"}))
	require.NoError(t, codec.TxPut(seedTx, "prefix/b", txTestValue{Field: "b"}))

	_, err := seedTx.Commit(context.Background())
	require.NoError(t, err)

	delTx := integrity.NewTx(store)

	err = codec.TxDelete(delTx, "prefix/", integrity.WithPrefix())
	require.NoError(t, err)

	_, err = delTx.Commit(context.Background())
	require.NoError(t, err)

	getTx := integrity.NewTx(store)
	futA := codec.TxGet(getTx, "prefix/a")
	futB := codec.TxGet(getTx, "prefix/b")

	_, err = getTx.Commit(context.Background())
	require.NoError(t, err)

	_, errA := futA.Result()
	require.ErrorIs(t, errA, integrity.ErrNotFound, "prefix/a should have been deleted")

	_, errB := futB.Result()
	require.ErrorIs(t, errB, integrity.ErrNotFound, "prefix/b should have been deleted")
}

func TestTxRange_EmptyName(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	seedTx := integrity.NewTx(store)
	require.NoError(t, codec.TxPut(seedTx, "alpha", txTestValue{Field: "a"}))
	require.NoError(t, codec.TxPut(seedTx, "beta", txTestValue{Field: "b"}))

	_, err := seedTx.Commit(context.Background())
	require.NoError(t, err)

	rangeTx := integrity.NewTx(store)
	fut := codec.TxRange(rangeTx, "")

	_, err = rangeTx.Commit(context.Background())
	require.NoError(t, err)

	results, err := fut.Result()
	require.NoError(t, err)
	assert.Len(t, results, 2, "empty name Range should return all items")
}

func TestTxRange_Named(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	seedTx := integrity.NewTx(store)
	require.NoError(t, codec.TxPut(seedTx, "group/x", txTestValue{Field: "x"}))
	require.NoError(t, codec.TxPut(seedTx, "group/y", txTestValue{Field: "y"}))
	require.NoError(t, codec.TxPut(seedTx, "other/z", txTestValue{Field: "z"}))

	_, err := seedTx.Commit(context.Background())
	require.NoError(t, err)

	rangeTx := integrity.NewTx(store)
	fut := codec.TxRange(rangeTx, "group")

	_, err = rangeTx.Commit(context.Background())
	require.NoError(t, err)

	results, err := fut.Result()
	require.NoError(t, err)
	assert.Len(t, results, 2, "named Range should return only items under 'group'")
}

func TestTxGet_IgnoreVerificationError(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodecWithHasher(t)

	// Seed the value at /objects/item without writing its companion hash so
	// validation will fail with "hash not verified (missing)" — the codec's
	// TxPut would refuse to produce this state on its own.
	putDirect(t, store, "/objects/item", "field: corrupted\n")

	getTx1 := integrity.NewTx(store)
	futStrict := codec.TxGet(getTx1, "item")
	_, err := getTx1.Commit(context.Background())
	require.NoError(t, err)

	_, errStrict := futStrict.Result()
	require.Error(t, errStrict, "missing hash should cause verification error without IgnoreVerificationError")

	getTx2 := integrity.NewTx(store)
	futLenient := codec.TxGet(getTx2, "item", integrity.IgnoreVerificationError())

	_, err = getTx2.Commit(context.Background())
	require.NoError(t, err)

	result, errLenient := futLenient.Result()
	require.NoError(t, errLenient, "IgnoreVerificationError should suppress the verification error")
	require.Error(t, result.Error, "ValidatedResult.Error should still be set even when ignored")
	require.True(t, result.Value.IsSome(), "value should be present despite integrity error")
}

func TestBindPredicate_Success(t *testing.T) {
	t.Parallel()

	codec := newTestCodec(t)

	pred, err := codec.BindPredicate("mykey", func(key []byte) predicate.Predicate {
		return predicate.VersionEqual(key, 42)
	})
	require.NoError(t, err)
	require.NotNil(t, pred)

	assert.Equal(t, []byte("/objects/mykey"), pred.Key())
	assert.Equal(t, int64(42), pred.Value())
}

func TestBindPredicate_NoValueKey(t *testing.T) {
	t.Parallel()

	// hashOnlyNamer never emits a value-type key, so BindPredicate has no
	// key to anchor the predicate to.
	noValueNamer := &hashOnlyNamer{}

	codec, err := integrity.NewCodecBuilder[txTestValue]().
		WithNamer(func(
			_ string,
			_ []namer.LayeredHashLocation,
			_ []namer.LayeredSigLocation,
			_ ...namer.LayeredOption,
		) (namer.Namer, error) {
			return noValueNamer, nil
		}).
		Build()
	require.NoError(t, err)

	_, err = codec.BindPredicate("anything", func(key []byte) predicate.Predicate {
		return predicate.VersionEqual(key, 1)
	})
	require.ErrorIs(t, err, integrity.ErrNoValueKey)
}

type hashOnlyNamer struct{}

func (n *hashOnlyNamer) GenerateNames(name string) ([]namer.Key, error) {
	return []namer.Key{
		namer.NewDefaultKey(name, namer.KeyTypeHash, "sha256", "/sha256/"+name),
	}, nil
}

func (n *hashOnlyNamer) ParseKey(raw string) (namer.DefaultKey, error) {
	return namer.DefaultKey{}, errors.New("not implemented")
}

func (n *hashOnlyNamer) ParseKeys(_ []string, _ bool) (namer.Results, error) {
	return namer.Results{}, errors.New("not implemented")
}

func (n *hashOnlyNamer) Prefix(_ string, _ bool) string { return "/sha256/" }

func TestTx_ResultDispatchIsolatesCodecs(t *testing.T) {
	t.Parallel()

	store := newTestStorage()

	// Two codecs sharing a type but living under distinct object locations,
	// so each codec only sees its own keys when results are dispatched.
	codecA, err := integrity.NewCodecBuilder[txTestValue]().
		WithObjectLocation("typeA").
		Build()
	require.NoError(t, err)

	codecB, err := integrity.NewCodecBuilder[txTestValue]().
		WithObjectLocation("typeB").
		Build()
	require.NoError(t, err)

	seedTx := integrity.NewTx(store)
	require.NoError(t, codecA.TxPut(seedTx, "item", txTestValue{Field: "A-value"}))
	require.NoError(t, codecB.TxPut(seedTx, "item", txTestValue{Field: "B-value"}))

	_, err = seedTx.Commit(context.Background())
	require.NoError(t, err)

	readTx := integrity.NewTx(store)
	futA := codecA.TxGet(readTx, "item")
	futB := codecB.TxGet(readTx, "item")

	_, err = readTx.Commit(context.Background())
	require.NoError(t, err)

	resultA, err := futA.Result()
	require.NoError(t, err, "codecA future should succeed")
	require.True(t, resultA.Value.IsSome())

	gotA := resultA.Value.Unwrap()
	assert.Equal(t, "A-value", gotA.Field, "codecA must receive its own result")

	resultB, err := futB.Result()
	require.NoError(t, err, "codecB future should succeed")
	require.True(t, resultB.Value.IsSome())

	gotB := resultB.Value.Unwrap()
	assert.Equal(t, "B-value", gotB.Field, "codecB must receive its own result")
}

func TestTx_BuildError_SecondCommitReturnsAlreadyCommitted(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Optional()

	store := storage.NewStorage(driverMock)
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)

	_ = codec.TxGet(txn, "")

	_, err1 := txn.Commit(context.Background())
	require.ErrorIs(t, err1, integrity.ErrInvalidName)

	// Once a Commit has run (even one that surfaced a build error), the next
	// Commit must return ErrTxAlreadyCommitted, not the original build error.
	_, err2 := txn.Commit(context.Background())
	require.ErrorIs(t, err2, integrity.ErrTxAlreadyCommitted)
}

func TestTxDelete_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Optional()

	store := storage.NewStorage(driverMock)
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)
	err := codec.TxDelete(txn, "")
	require.ErrorIs(t, err, integrity.ErrInvalidName)

	_, commitErr := txn.Commit(context.Background())
	require.ErrorIs(t, commitErr, integrity.ErrInvalidName)
}

func TestTxRange_InvalidName(t *testing.T) {
	t.Parallel()

	driverMock := mocks.NewDriverMock(t)
	driverMock.ExecuteMock.Optional()

	store := storage.NewStorage(driverMock)
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)

	_ = codec.TxRange(txn, "/invalid")

	_, err := txn.Commit(context.Background())
	require.ErrorIs(t, err, integrity.ErrInvalidName)
}

func TestRangeFuture_BeforeCommit(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)
	txn := integrity.NewTx(store)

	fut := codec.TxRange(txn, "")

	_, err := fut.Result()
	require.ErrorIs(t, err, integrity.ErrTxNotCommitted)
}

func TestRangeFuture_NonFiredBranch(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	txn := integrity.NewTx(store)

	elseFut := codec.TxRange(txn.Else(), "")

	// No predicates → Then fires unconditionally → Else cannot fire.
	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	assert.True(t, rsp.Succeeded)

	_, futErr := elseFut.Result()
	require.ErrorIs(t, futErr, integrity.ErrBranchNotFired)
}

// Two TxGet calls on the same Tx must produce independent spans so each
// future receives only its own RequestResponse slice.
func TestTx_MultipleGetsFlattenCorrectly(t *testing.T) {
	t.Parallel()

	store := newTestStorage()
	codec := newTestCodec(t)

	seedTx := integrity.NewTx(store)
	require.NoError(t, codec.TxPut(seedTx, "key1", txTestValue{Field: "v1"}))
	require.NoError(t, codec.TxPut(seedTx, "key2", txTestValue{Field: "v2"}))

	_, err := seedTx.Commit(context.Background())
	require.NoError(t, err)

	readTx := integrity.NewTx(store)
	fut1 := codec.TxGet(readTx, "key1")
	fut2 := codec.TxGet(readTx, "key2")

	_, err = readTx.Commit(context.Background())
	require.NoError(t, err)

	r1, err := fut1.Result()
	require.NoError(t, err)

	got1 := r1.Value.Unwrap()
	assert.Equal(t, "v1", got1.Field)

	r2, err := fut2.Result()
	require.NoError(t, err)

	got2 := r2.Value.Unwrap()
	assert.Equal(t, "v2", got2.Field)
}

// Note: compile-time Branchable assertions cannot be made from the external
// _test package because branch() is unexported. Satisfaction is verified
// indirectly by every test that passes *Tx or *Branch to TxGet/TxPut/etc.
