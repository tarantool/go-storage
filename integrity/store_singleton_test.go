package integrity_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storage "github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/driver/dummy"
	"github.com/tarantool/go-storage/v2/hasher"
	"github.com/tarantool/go-storage/v2/integrity"
)

// newSingletonCodec builds a Codec[SimpleStruct] with objectLocation="settings",
// one hasher, and one signer/verifier — enough to exercise all three key
// layers (value/hashes/sig).
func newSingletonCodec(t *testing.T) (*integrity.Codec[SimpleStruct], string) {
	t.Helper()

	signerVerifier := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[SimpleStruct]().
		WithObjectLocation("settings").
		WithHasher(hasher.NewSHA256Hasher()).
		WithSignerVerifier(signerVerifier).
		Build()
	require.NoError(t, err)

	return codec, signerVerifier.Name()
}

func TestSingletonStore_PutGet(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	auth, err := codec.BindSingleton(storage.NewStorage(dummy.New()), "auth")
	require.NoError(t, err)

	ctx := context.Background()
	want := SimpleStruct{Name: "primary", Value: 42}

	require.NoError(t, auth.Put(ctx, want))

	got, err := auth.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "auth", got.Name)
	assert.Equal(t, want, got.Value.Unwrap())
}

func TestSingletonStore_WireLayout(t *testing.T) {
	t.Parallel()

	codec, sigName := newSingletonCodec(t)
	backend := storage.NewStorage(dummy.New())

	auth, err := codec.BindSingleton(backend, "auth")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, auth.Put(ctx, SimpleStruct{Name: "x", Value: 1}))

	// Snapshot every key the dummy driver holds. With objectLocation="settings"
	// and name="auth", we expect exactly the value + hash + sig layers below.
	all, err := backend.Range(ctx, storage.WithPrefix("/"))
	require.NoError(t, err)

	keys := make([]string, 0, len(all))
	for _, kv := range all {
		keys = append(keys, string(kv.Key))
	}

	sort.Strings(keys)

	want := []string{
		"/hashes/sha256/settings/auth",
		"/settings/auth",
		"/sig/" + sigName + "/settings/auth",
	}
	sort.Strings(want)

	assert.Equal(t, want, keys)
}

func TestCodec_BindSingleton_RejectsInvalidName(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)

	for _, bad := range []string{"", "/auth", "auth/", "/auth/"} {
		t.Run("name="+bad, func(t *testing.T) {
			t.Parallel()

			_, err := codec.BindSingleton(storage.NewStorage(dummy.New()), bad)
			require.ErrorIs(t, err, integrity.ErrInvalidName)
		})
	}
}

func TestSingletonStore_Delete(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	backend := storage.NewStorage(dummy.New())

	auth, err := codec.BindSingleton(backend, "auth")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, auth.Put(ctx, SimpleStruct{Name: "x", Value: 1}))

	pre, err := backend.Range(ctx, storage.WithPrefix("/"))
	require.NoError(t, err)
	require.Len(t, pre, 3, "expected value+hash+sig keys present before delete")

	require.NoError(t, auth.Delete(ctx))

	post, err := backend.Range(ctx, storage.WithPrefix("/"))
	require.NoError(t, err)
	assert.Empty(t, post, "Delete should remove value+hash+sig keys")
}

func TestSingletonStore_Put_PredicateFails(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)

	auth, err := codec.BindSingleton(storage.NewStorage(dummy.New()), "auth")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, auth.Put(ctx, SimpleStruct{Name: "v1", Value: 1}))

	// Predicate matches a value we never wrote — Put must fail.
	pred, err := codec.ValueEqual(SimpleStruct{Name: "never-written", Value: 0})
	require.NoError(t, err)

	err = auth.Put(ctx, SimpleStruct{Name: "v2", Value: 2}, integrity.WithPutPredicates(pred))
	require.ErrorIs(t, err, integrity.ErrPredicateFailed)

	// Sanity: the original value is still there.
	got, err := auth.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, SimpleStruct{Name: "v1", Value: 1}, got.Value.Unwrap())
}

func TestSingletonStore_TxRoundTrip(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	backend := storage.NewStorage(dummy.New())

	auth, err := codec.BindSingleton(backend, "auth")
	require.NoError(t, err)

	ctx := context.Background()

	// TxPut and TxGet inside the same transaction-like sequence: write, commit,
	// then a separate read transaction returns the future result.
	writeTx := integrity.NewTx(backend)
	require.NoError(t, auth.TxPut(writeTx, SimpleStruct{Name: "x", Value: 7}))

	_, err = writeTx.Commit(ctx)
	require.NoError(t, err)

	readTx := integrity.NewTx(backend)
	fut := auth.TxGet(readTx)

	_, err = readTx.Commit(ctx)
	require.NoError(t, err)

	got, err := fut.Result()
	require.NoError(t, err)
	assert.Equal(t, "auth", got.Name)
	assert.Equal(t, SimpleStruct{Name: "x", Value: 7}, got.Value.Unwrap())
}

func TestSingletonStore_TxDelete(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	backend := storage.NewStorage(dummy.New())

	auth, err := codec.BindSingleton(backend, "auth")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, auth.Put(ctx, SimpleStruct{Name: "x", Value: 1}))

	delTx := integrity.NewTx(backend)
	require.NoError(t, auth.TxDelete(delTx))

	_, err = delTx.Commit(ctx)
	require.NoError(t, err)

	post, err := backend.Range(ctx, storage.WithPrefix("/"))
	require.NoError(t, err)
	assert.Empty(t, post)
}

func TestSingletonStore_TxIfBindPredicate(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	backend := storage.NewStorage(dummy.New())

	auth, err := codec.BindSingleton(backend, "auth")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, auth.Put(ctx, SimpleStruct{Name: "v1", Value: 1}))

	// Build a predicate that will not hold (compares against an unwritten value)
	// and gate a TxPut on it. The transaction must report Succeeded=false and
	// the value must remain v1.
	pred, err := codec.ValueEqual(SimpleStruct{Name: "never", Value: 0})
	require.NoError(t, err)

	bound, err := auth.BindPredicate(pred)
	require.NoError(t, err)

	txn := integrity.NewTx(backend)
	txn.If(bound)

	require.NoError(t, auth.TxPut(txn, SimpleStruct{Name: "v2", Value: 2}))

	resp, err := txn.Commit(ctx)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "predicate should fail and block the put")

	got, err := auth.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, SimpleStruct{Name: "v1", Value: 1}, got.Value.Unwrap())
}

func TestSingletonStore_Watch(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)

	auth, err := codec.BindSingleton(storage.NewStorage(dummy.New()), "auth")
	require.NoError(t, err)

	ctx := t.Context()

	events, err := auth.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, auth.Put(ctx, SimpleStruct{Name: "first", Value: 1}))

	select {
	case ev, ok := <-events:
		require.True(t, ok, "watch channel closed before delivering an event")
		assert.NotEmpty(t, ev.Prefix)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}

func TestSingletonStore_ValueKey(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	auth, err := codec.BindSingleton(storage.NewStorage(dummy.New()), "auth")
	require.NoError(t, err)

	key, err := auth.ValueKey()
	require.NoError(t, err)
	assert.Equal(t, "/settings/auth", key)
}

func TestSingletonStore_ValueKey_IncludesStoragePrefix(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	wrapped, err := storage.Prefixed("/scope", storage.NewStorage(dummy.New()))
	require.NoError(t, err)

	auth, err := codec.BindSingleton(wrapped, "auth")
	require.NoError(t, err)

	key, err := auth.ValueKey()
	require.NoError(t, err)
	assert.Equal(t, "/scope/settings/auth", key,
		"SingletonStore.ValueKey must include the bound storage's prefix")
}

func TestSingletonStore_FullKeys(t *testing.T) {
	t.Parallel()

	codec, _ := newSingletonCodec(t)
	wrapped, err := storage.Prefixed("/scope", storage.NewStorage(dummy.New()))
	require.NoError(t, err)

	auth, err := codec.BindSingleton(wrapped, "auth")
	require.NoError(t, err)

	keys, err := auth.FullKeys()
	require.NoError(t, err)
	require.Len(t, keys, 3, "value + sha256 hash + rsa-pss signature")

	for _, k := range keys {
		assert.True(t, len(k) > len("/scope") && k[:len("/scope")] == "/scope",
			"every SingletonStore.FullKeys entry must start with /scope, got %q", k)
	}
}
