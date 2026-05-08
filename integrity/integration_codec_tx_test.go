//nolint:paralleltest
package integrity_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-option"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/driver"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
)

// integrationPrefix returns a per-test storage namespace so concurrent driver
// subtests (and reused TCS instances) do not collide.
func integrationPrefix(t *testing.T) string {
	t.Helper()

	return "/test/" + strings.ReplaceAll(t.Name(), "/", "-")
}

// scopedStorage wraps the driver in a per-test Prefixed namespace.
func scopedStorage(t *testing.T, driverInstance driver.Driver) storage.Storage {
	t.Helper()

	st, err := storage.Prefixed(integrationPrefix(t), storage.NewStorage(driverInstance))
	require.NoError(t, err)

	return st
}

// newIntegrationCodecStore builds a Codec[IntegrationStruct] and binds it to
// a per-test storage namespace. Both are returned because some tests need the
// codec directly (predicates, raw Tx) while others only need the store.
func newIntegrationCodecStore(
	t *testing.T,
	driverInstance driver.Driver,
	configure func(integrity.CodecBuilder[IntegrationStruct]) integrity.CodecBuilder[IntegrationStruct],
) (*integrity.Codec[IntegrationStruct], *integrity.Store[IntegrationStruct]) {
	t.Helper()

	builder := integrity.NewCodecBuilder[IntegrationStruct]()
	if configure != nil {
		builder = configure(builder)
	}

	codec, err := builder.Build()
	require.NoError(t, err)

	base := scopedStorage(t, driverInstance)

	return codec, codec.Bind(base)
}

func cleanupStore[T any](t *testing.T, store *integrity.Store[T], names ...string) {
	t.Helper()

	ctx := t.Context()

	for _, name := range names {
		_ = store.Delete(ctx, name)
	}
}

// TestStoreIntegration_GetPut is the round-trip smoke test for Store[T] across
// every registered backend.
func TestStoreIntegration_GetPut(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		_, store := newIntegrationCodecStore(t, driverInstance, nil)

		value := IntegrationStruct{Name: "test", Value: 42}
		require.NoError(t, store.Put(ctx, "my-object", value))

		got, err := store.Get(ctx, "my-object")
		require.NoError(t, err)
		assert.Equal(t, "my-object", got.Name)
		require.True(t, got.Value.IsSome())

		val, _ := got.Value.Get()
		assert.Equal(t, value, val)

		cleanupStore(t, store, "my-object")
	})
}

// TestStoreIntegration_Put_WithPredicates exercises both the
// "predicate matches → write" and "predicate fails → ErrPredicateFailed" paths.
func TestStoreIntegration_Put_WithPredicates(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		codec, store := newIntegrationCodecStore(t, driverInstance, nil)

		initial := IntegrationStruct{Name: "init", Value: 1}
		updated := IntegrationStruct{Name: "updated", Value: 2}

		// Allow case: ValueEqual matches the seeded record, write proceeds.
		require.NoError(t, store.Put(ctx, "allow", initial))

		pAllow, err := codec.ValueEqual(initial)
		require.NoError(t, err)

		err = store.Put(ctx, "allow", updated, integrity.WithPutPredicates(pAllow))
		require.NoError(t, err)

		got, err := store.Get(ctx, "allow")
		require.NoError(t, err)

		gotVal, _ := got.Value.Get()
		assert.Equal(t, updated, gotVal)

		// Deny case: ValueEqual on the post-update value is still bound to
		// `initial`, so the next write must report ErrPredicateFailed.
		err = store.Put(ctx, "allow", IntegrationStruct{Name: "third", Value: 3},
			integrity.WithPutPredicates(pAllow))
		require.ErrorIs(t, err, integrity.ErrPredicateFailed)

		// Verify the update did not land.
		got, err = store.Get(ctx, "allow")
		require.NoError(t, err)

		gotVal, _ = got.Value.Get()
		assert.Equal(t, updated, gotVal)

		cleanupStore(t, store, "allow")
	})
}

// TestStoreIntegration_Delete deletes a freshly written value.
func TestStoreIntegration_Delete(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		_, store := newIntegrationCodecStore(t, driverInstance, nil)

		require.NoError(t, store.Put(ctx, "victim", IntegrationStruct{Name: "x", Value: 1}))
		require.NoError(t, store.Delete(ctx, "victim"))

		_, err := store.Get(ctx, "victim")
		require.ErrorIs(t, err, integrity.ErrNotFound)
	})
}

// TestStoreIntegration_Delete_Prefix removes every value under the given name
// prefix in a single round-trip.
func TestStoreIntegration_Delete_Prefix(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		_, store := newIntegrationCodecStore(t, driverInstance, nil)

		for i, name := range []string{"tmp/a", "tmp/b", "keep/me"} {
			err := store.Put(ctx, name, IntegrationStruct{Name: name, Value: i})
			require.NoError(t, err)
		}

		require.NoError(t, store.Delete(ctx, "tmp/", integrity.WithPrefix()))

		_, err := store.Get(ctx, "tmp/a")
		require.ErrorIs(t, err, integrity.ErrNotFound)

		_, err = store.Get(ctx, "tmp/b")
		require.ErrorIs(t, err, integrity.ErrNotFound)

		got, err := store.Get(ctx, "keep/me")
		require.NoError(t, err)
		require.True(t, got.Value.IsSome())

		cleanupStore(t, store, "keep/me")
	})
}

// TestStoreIntegration_Range ensures Range returns every codec-owned value
// under the supplied prefix and decodes them correctly.
func TestStoreIntegration_Range(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		_, store := newIntegrationCodecStore(t, driverInstance, nil)

		seed := map[string]IntegrationStruct{
			"records/1": {Name: "obj1", Value: 100},
			"records/2": {Name: "obj2", Value: 200},
		}
		for name, value := range seed {
			require.NoError(t, store.Put(ctx, name, value))
		}

		results, err := store.Range(ctx, "records/")
		require.NoError(t, err)
		require.Len(t, results, len(seed))

		// ModRevision differs between drivers; normalize before comparing.
		for i := range results {
			results[i].ModRevision = 0
		}

		expected := []integrity.ValidatedResult[IntegrationStruct]{
			{
				Name:        "records/1",
				Value:       option.Some(seed["records/1"]),
				ModRevision: 0,
				Error:       nil,
			},
			{
				Name:        "records/2",
				Value:       option.Some(seed["records/2"]),
				ModRevision: 0,
				Error:       nil,
			},
		}
		require.ElementsMatch(t, expected, results)

		cleanupStore(t, store, "records/1", "records/2")
	})
}

// TestStoreIntegration_WithSignerVerifier round-trips a value through a codec
// with both a hasher and an RSA-PSS signer/verifier; Get must verify cleanly.
func TestStoreIntegration_WithSignerVerifier(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		shaHash := hasher.NewSHA256Hasher()
		rsaSV := crypto.NewRSAPSSSignerVerifier(*rsaPK2048)

		_, store := newIntegrationCodecStore(t, driverInstance,
			func(b integrity.CodecBuilder[IntegrationStruct]) integrity.CodecBuilder[IntegrationStruct] {
				return b.WithHasher(shaHash).WithSignerVerifier(rsaSV)
			})

		value := IntegrationStruct{Name: "secured", Value: 7}
		require.NoError(t, store.Put(ctx, "alice", value))

		got, err := store.Get(ctx, "alice")
		require.NoError(t, err)

		gotVal, _ := got.Value.Get()
		assert.Equal(t, value, gotVal)

		cleanupStore(t, store, "alice")
	})
}

// TestStoreIntegration_Watch verifies Watch sees an event for a delete after
// the channel is established.
func TestStoreIntegration_Watch(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		_, store := newIntegrationCodecStore(t, driverInstance, nil)

		require.NoError(t, store.Put(ctx, "object1", IntegrationStruct{Name: "obj1", Value: 100}))

		eventCh, err := store.Watch(ctx, "object1")
		require.NoError(t, err)

		require.NoError(t, store.Delete(ctx, "object1"))

		select {
		case ev := <-eventCh:
			require.Equal(t, []byte("/objects/object1"), ev.Prefix)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for watch event")
		}
	})
}

// TestTxIntegration_MultiOpAtomic enqueues multiple TxPut calls onto a single
// Tx and verifies they all land in one commit.
func TestTxIntegration_MultiOpAtomic(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		base := scopedStorage(t, driverInstance)

		codec, err := integrity.NewCodecBuilder[IntegrationStruct]().Build()
		require.NoError(t, err)

		txn := integrity.NewTx(base)
		require.NoError(t, codec.TxPut(txn, "alice", IntegrationStruct{Name: "alice", Value: 1}))
		require.NoError(t, codec.TxPut(txn, "bob", IntegrationStruct{Name: "bob", Value: 2}))

		resp, err := txn.Commit(ctx)
		require.NoError(t, err)
		require.True(t, resp.Succeeded)

		store := codec.Bind(base)
		defer cleanupStore(t, store, "alice", "bob")

		gotA, err := store.Get(ctx, "alice")
		require.NoError(t, err)

		valA, _ := gotA.Value.Get()
		assert.Equal(t, IntegrationStruct{Name: "alice", Value: 1}, valA)

		gotB, err := store.Get(ctx, "bob")
		require.NoError(t, err)

		valB, _ := gotB.Value.Get()
		assert.Equal(t, IntegrationStruct{Name: "bob", Value: 2}, valB)
	})
}

// TestTxIntegration_ThenElse_ThenFires checks that with a satisfied predicate
// the Then branch executes and the Else-branch future returns ErrBranchNotFired.
func TestTxIntegration_ThenElse_ThenFires(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		base := scopedStorage(t, driverInstance)

		codec, err := integrity.NewCodecBuilder[IntegrationStruct]().Build()
		require.NoError(t, err)

		store := codec.Bind(base)
		require.NoError(t, store.Put(ctx, "anchor", IntegrationStruct{Name: "anchor", Value: 1}))

		defer cleanupStore(t, store, "anchor")

		// VersionGreater(0) is always true once the value exists.
		bound, err := codec.BindPredicate("anchor", codec.VersionGreater(0))
		require.NoError(t, err)

		txn := integrity.NewTx(base).If(bound)

		thenFut := codec.TxGet(txn.Then(), "anchor")
		elseFut := codec.TxGet(txn.Else(), "anchor")

		resp, err := txn.Commit(ctx)
		require.NoError(t, err)
		require.True(t, resp.Succeeded)

		thenRes, err := thenFut.Result()
		require.NoError(t, err)

		thenVal, _ := thenRes.Value.Get()
		assert.Equal(t, "anchor", thenVal.Name)

		_, elseErr := elseFut.Result()
		require.ErrorIs(t, elseErr, integrity.ErrBranchNotFired)
	})
}

// TestTxIntegration_ThenElse_ElseFires inverts the previous test: the
// predicate fails, Else fires, Then-branch futures return ErrBranchNotFired.
func TestTxIntegration_ThenElse_ElseFires(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		base := scopedStorage(t, driverInstance)

		codec, err := integrity.NewCodecBuilder[IntegrationStruct]().Build()
		require.NoError(t, err)

		store := codec.Bind(base)
		require.NoError(t, store.Put(ctx, "fallback", IntegrationStruct{Name: "fallback", Value: 1}))

		defer cleanupStore(t, store, "fallback")

		// Predicate that cannot hold: ValueEqual against a value the seeded
		// record cannot match.
		mismatch, err := codec.ValueEqual(IntegrationStruct{Name: "nope", Value: 99})
		require.NoError(t, err)

		bound, err := codec.BindPredicate("fallback", mismatch)
		require.NoError(t, err)

		txn := integrity.NewTx(base).If(bound)

		thenFut := codec.TxGet(txn.Then(), "fallback")
		elseFut := codec.TxGet(txn.Else(), "fallback")

		resp, err := txn.Commit(ctx)
		require.NoError(t, err)
		require.False(t, resp.Succeeded)

		_, thenErr := thenFut.Result()
		require.ErrorIs(t, thenErr, integrity.ErrBranchNotFired)

		elseRes, err := elseFut.Result()
		require.NoError(t, err)

		elseVal, _ := elseRes.Value.Get()
		assert.Equal(t, "fallback", elseVal.Name)
	})
}

// TestTxIntegration_TxRange exercises TxRange against every backend.
func TestTxIntegration_TxRange(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		base := scopedStorage(t, driverInstance)

		codec, err := integrity.NewCodecBuilder[IntegrationStruct]().Build()
		require.NoError(t, err)

		store := codec.Bind(base)
		for i, name := range []string{"records/1", "records/2"} {
			require.NoError(t, store.Put(ctx, name,
				IntegrationStruct{Name: name, Value: 100 + i}))
		}

		defer cleanupStore(t, store, "records/1", "records/2")

		readTx := integrity.NewTx(base)
		fut := codec.TxRange(readTx, "records/")

		_, err = readTx.Commit(ctx)
		require.NoError(t, err)

		results, err := fut.Result()
		require.NoError(t, err)
		require.Len(t, results, 2)

		gotNames := []string{results[0].Name, results[1].Name}
		assert.ElementsMatch(t, []string{"records/1", "records/2"}, gotNames)
	})
}

// TestTxIntegration_TxDelete checks that TxDelete enqueued onto a Tx removes
// every key that backs the named value.
func TestTxIntegration_TxDelete(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		base := scopedStorage(t, driverInstance)

		codec, err := integrity.NewCodecBuilder[IntegrationStruct]().
			WithHasher(hasher.NewSHA256Hasher()).
			Build()
		require.NoError(t, err)

		store := codec.Bind(base)
		require.NoError(t, store.Put(ctx, "victim", IntegrationStruct{Name: "victim", Value: 1}))

		txn := integrity.NewTx(base)
		require.NoError(t, codec.TxDelete(txn, "victim"))

		resp, err := txn.Commit(ctx)
		require.NoError(t, err)
		require.True(t, resp.Succeeded)

		_, err = store.Get(ctx, "victim")
		require.ErrorIs(t, err, integrity.ErrNotFound)
	})
}

// TestTxIntegration_AlreadyCommitted ensures a second Commit call returns
// ErrTxAlreadyCommitted without re-issuing storage work.
func TestTxIntegration_AlreadyCommitted(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		base := scopedStorage(t, driverInstance)

		codec, err := integrity.NewCodecBuilder[IntegrationStruct]().Build()
		require.NoError(t, err)

		txn := integrity.NewTx(base)
		require.NoError(t, codec.TxPut(txn, "once", IntegrationStruct{Name: "once", Value: 1}))

		resp, err := txn.Commit(ctx)
		require.NoError(t, err)
		require.True(t, resp.Succeeded)

		_, err = txn.Commit(ctx)
		require.ErrorIs(t, err, integrity.ErrTxAlreadyCommitted)

		store := codec.Bind(base)
		defer cleanupStore(t, store, "once")
	})
}
