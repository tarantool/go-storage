package integrity_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/operation"
)

func newPrefixedCodec(t *testing.T, prefix string) *integrity.Codec[txTestValue] {
	t.Helper()

	codec, err := integrity.NewCodecBuilder[txTestValue]().
		WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithKeyPrefix(prefix).
		Build()
	require.NoError(t, err)

	return codec
}

// TestTx_TwoCodecsDifferentPrefixes_Atomic verifies the motivating use case:
// two codecs with different WithKeyPrefix values share one storage handle and
// are committed in the same atomic Tx.
func TestTx_TwoCodecsDifferentPrefixes_Atomic(t *testing.T) {
	t.Parallel()

	store := newTestStorage()

	codecA := newPrefixedCodec(t, "/a")
	codecB := newPrefixedCodec(t, "/b")

	txn := integrity.NewTx(store)

	err := codecA.TxPut(txn, "alice", txTestValue{Field: "from-A"})
	require.NoError(t, err)

	err = codecB.TxPut(txn, "alice", txTestValue{Field: "from-B"})
	require.NoError(t, err)

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	require.True(t, rsp.Succeeded)

	readTx := integrity.NewTx(store)
	futureA := codecA.TxGet(readTx, "alice")
	futureB := codecB.TxGet(readTx, "alice")

	_, err = readTx.Commit(context.Background())
	require.NoError(t, err)

	resultA, err := futureA.Result()
	require.NoError(t, err)

	gotA, ok := resultA.Value.Get()
	require.True(t, ok)
	assert.Equal(t, "from-A", gotA.Field)

	resultB, err := futureB.Result()
	require.NoError(t, err)

	gotB, ok := resultB.Value.Get()
	require.True(t, ok)
	assert.Equal(t, "from-B", gotB.Field)
}

// TestTx_TwoCodecsDifferentPrefixes_RolledBackTogether verifies that when a
// predicate fails, neither prefix sees a partial write.
func TestTx_TwoCodecsDifferentPrefixes_RolledBackTogether(t *testing.T) {
	t.Parallel()

	store := newTestStorage()

	codecA := newPrefixedCodec(t, "/a")
	codecB := newPrefixedCodec(t, "/b")

	txn := integrity.NewTx(store)

	pred, err := codecA.BindPredicate("alice", codecA.VersionGreater(999))
	require.NoError(t, err)

	txn.If(pred)
	require.NoError(t, codecA.TxPut(txn, "alice", txTestValue{Field: "from-A"}))
	require.NoError(t, codecB.TxPut(txn, "alice", txTestValue{Field: "from-B"}))

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	assert.False(t, rsp.Succeeded, "predicate must have failed")

	readTx := integrity.NewTx(store)
	futureA := codecA.TxGet(readTx, "alice")
	futureB := codecB.TxGet(readTx, "alice")

	_, err = readTx.Commit(context.Background())
	require.NoError(t, err)

	_, errA := futureA.Result()
	_, errB := futureB.Result()

	require.ErrorIs(t, errA, integrity.ErrNotFound, "A side must not be written")
	require.ErrorIs(t, errB, integrity.ErrNotFound, "B side must not be written")
}

// TestTx_TwoCodecsDifferentPrefixes_RangeIsolated verifies that TxRange on
// codec A only returns entries written under its own prefix — codec B's
// entries in the same storage must not leak into A's range result.
func TestTx_TwoCodecsDifferentPrefixes_RangeIsolated(t *testing.T) {
	t.Parallel()

	store := newTestStorage()

	codecA := newPrefixedCodec(t, "/a")
	codecB := newPrefixedCodec(t, "/b")

	seedTx := integrity.NewTx(store)
	require.NoError(t, codecA.TxPut(seedTx, "alice", txTestValue{Field: "A-alice"}))
	require.NoError(t, codecA.TxPut(seedTx, "bob", txTestValue{Field: "A-bob"}))
	require.NoError(t, codecB.TxPut(seedTx, "carol", txTestValue{Field: "B-carol"}))

	rsp, err := seedTx.Commit(context.Background())
	require.NoError(t, err)
	require.True(t, rsp.Succeeded)

	rangeTx := integrity.NewTx(store)
	futA := codecA.TxRange(rangeTx, "")
	futB := codecB.TxRange(rangeTx, "")

	_, err = rangeTx.Commit(context.Background())
	require.NoError(t, err)

	resultsA, err := futA.Result()
	require.NoError(t, err)
	assert.Len(t, resultsA, 2, "codec A range must see only its own two entries")

	resultsB, err := futB.Result()
	require.NoError(t, err)
	assert.Len(t, resultsB, 1, "codec B range must see only its own one entry")
}

// TestTx_KeyPrefix_OnTopOfStoragePrefixed_DoublePrefixes guards the docstring
// warning: stacking WithKeyPrefix on top of storage.Prefixed for the same
// logical prefix doubles it. A value written via storage.Prefixed("/p") +
// codec.WithKeyPrefix("/p") lands at "/p/p/objects/alice" in the underlying
// driver, not "/p/objects/alice". This test pins that behaviour so a future
// refactor that silently "helps" by deduping prefixes shows up as a failure.
func TestTx_KeyPrefix_OnTopOfStoragePrefixed_DoublePrefixes(t *testing.T) {
	t.Parallel()

	inner := newTestStorage()

	scoped, err := storage.Prefixed("/p", inner)
	require.NoError(t, err)

	codec, err := integrity.NewCodecBuilder[txTestValue]().
		WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithKeyPrefix("/p").
		Build()
	require.NoError(t, err)

	txn := integrity.NewTx(scoped)
	require.NoError(t, codec.TxPut(txn, "alice", txTestValue{Field: "v"}))

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	require.True(t, rsp.Succeeded)

	// The codec sees its own "/p/objects/alice" key through the Prefixed
	// scope, which prepends another "/p" — final physical key is "/p/p/...".
	// Scan the inner (un-prefixed) storage to observe the doubled prefix.
	innerTx := inner.Tx(context.Background()).
		Then(operation.Get([]byte("/")))

	results, err := innerTx.Commit()
	require.NoError(t, err)
	require.True(t, results.Succeeded)
	require.NotEmpty(t, results.Results)

	keys := make([]string, 0, len(results.Results[0].Values))
	for _, kv := range results.Results[0].Values {
		keys = append(keys, string(kv.Key))
	}

	require.NotEmpty(t, keys, "expected at least one physical key in inner storage")

	for _, k := range keys {
		assert.True(t, strings.HasPrefix(k, "/p/p/"),
			"stacked prefix should double to /p/p/...; got %q", k)
	}
}

// TestTx_TwoCodecsDifferentPrefixes_KeySpacesAreDisjoint verifies that
// /a/objects/alice and /b/objects/alice live in different sub-trees.
func TestTx_TwoCodecsDifferentPrefixes_KeySpacesAreDisjoint(t *testing.T) {
	t.Parallel()

	store := newTestStorage()

	codecA := newPrefixedCodec(t, "/a")
	codecB := newPrefixedCodec(t, "/b")

	txn := integrity.NewTx(store)
	require.NoError(t, codecA.TxPut(txn, "alice", txTestValue{Field: "A"}))

	rsp, err := txn.Commit(context.Background())
	require.NoError(t, err)
	require.True(t, rsp.Succeeded)

	readTx := integrity.NewTx(store)
	futureB := codecB.TxGet(readTx, "alice")

	_, err = readTx.Commit(context.Background())
	require.NoError(t, err)

	_, errB := futureB.Result()
	require.ErrorIs(t, errB, integrity.ErrNotFound)
}
