package integrity_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
)

type txExampleValue struct {
	Field string `yaml:"field"`
}

func newTxExampleCodec() *integrity.Codec[txExampleValue] {
	codec, err := integrity.NewCodecBuilder[txExampleValue]().WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	if err != nil {
		log.Fatalf("build codec: %v", err)
	}

	return codec
}

// ExampleNewTx demonstrates a multi-codec, multi-op transaction. Two TxPut
// calls are accumulated, then committed atomically with a single storage call.
func ExampleNewTx() {
	ctx := context.Background()
	store := storage.NewStorage(dummy.New())
	codec := newTxExampleCodec()

	txn := integrity.NewTx(store)

	err := codec.TxPut(txn, "alice", txExampleValue{Field: "a"})
	if err != nil {
		log.Fatalf("put alice: %v", err)
	}

	err = codec.TxPut(txn, "bob", txExampleValue{Field: "b"})
	if err != nil {
		log.Fatalf("put bob: %v", err)
	}

	resp, err := txn.Commit(ctx)
	if err != nil {
		log.Fatalf("commit: %v", err)
	}

	fmt.Println("succeeded:", resp.Succeeded)

	// Output:
	// succeeded: true
}

// ExampleGetFuture shows how TxGet hands back a *GetFuture[T] whose
// Result() is populated only after Commit.
func ExampleGetFuture() {
	ctx := context.Background()
	store := storage.NewStorage(dummy.New())
	codec := newTxExampleCodec()

	seedTx := integrity.NewTx(store)

	err := codec.TxPut(seedTx, "alice", txExampleValue{Field: "hello"})
	if err != nil {
		log.Fatalf("seed: %v", err)
	}

	_, err = seedTx.Commit(ctx)
	if err != nil {
		log.Fatalf("seed commit: %v", err)
	}

	readTx := integrity.NewTx(store)
	fut := codec.TxGet(readTx, "alice")

	// Calling Result() before Commit returns ErrTxNotCommitted.
	_, err = fut.Result()
	fmt.Println("before commit:", errors.Is(err, integrity.ErrTxNotCommitted))

	_, err = readTx.Commit(ctx)
	if err != nil {
		log.Fatalf("read commit: %v", err)
	}

	res, err := fut.Result()
	if err != nil {
		log.Fatalf("result: %v", err)
	}

	fmt.Println("field:", res.Value.Unwrap().Field)

	// Output:
	// before commit: true
	// field: hello
}

// ExampleRangeFuture shows TxRange returning every value under a name
// prefix in a single transaction. Use "" to fetch every value the codec
// owns; pass a non-empty prefix (must end with "/") to scope.
func ExampleRangeFuture() {
	ctx := context.Background()
	store := storage.NewStorage(dummy.New())
	codec := newTxExampleCodec()

	seedTx := integrity.NewTx(store)
	for _, name := range []string{"users/alice", "users/bob"} {
		err := codec.TxPut(seedTx, name, txExampleValue{Field: name})
		if err != nil {
			log.Fatalf("seed %s: %v", name, err)
		}
	}

	_, err := seedTx.Commit(ctx)
	if err != nil {
		log.Fatalf("seed commit: %v", err)
	}

	readTx := integrity.NewTx(store)
	fut := codec.TxRange(readTx, "users/")

	_, err = readTx.Commit(ctx)
	if err != nil {
		log.Fatalf("read commit: %v", err)
	}

	results, err := fut.Result()
	if err != nil {
		log.Fatalf("result: %v", err)
	}

	names := make([]string, 0, len(results))
	for _, r := range results {
		names = append(names, r.Name)
	}

	sort.Strings(names)

	for _, n := range names {
		fmt.Println(n)
	}

	// Output:
	// users/alice
	// users/bob
}

// ExampleCodec_TxDelete enqueues a Delete onto the Then branch and removes
// every key that belongs to the named value (value plus all hash/sig keys).
func ExampleCodec_TxDelete() {
	ctx := context.Background()
	store := storage.NewStorage(dummy.New())
	codec := newTxExampleCodec()

	seedTx := integrity.NewTx(store)

	err := codec.TxPut(seedTx, "victim", txExampleValue{Field: "x"})
	if err != nil {
		log.Fatalf("seed: %v", err)
	}

	_, err = seedTx.Commit(ctx)
	if err != nil {
		log.Fatalf("seed commit: %v", err)
	}

	delTx := integrity.NewTx(store)

	err = codec.TxDelete(delTx, "victim")
	if err != nil {
		log.Fatalf("delete: %v", err)
	}

	_, err = delTx.Commit(ctx)
	if err != nil {
		log.Fatalf("delete commit: %v", err)
	}

	getTx := integrity.NewTx(store)
	fut := codec.TxGet(getTx, "victim")

	_, err = getTx.Commit(ctx)
	if err != nil {
		log.Fatalf("get commit: %v", err)
	}

	_, err = fut.Result()
	fmt.Println("not found:", errors.Is(err, integrity.ErrNotFound))

	// Output:
	// not found: true
}

// ExampleTx_thenElse routes operations to the Then or Else branch, depending
// on which side fires after If predicates are evaluated. Futures attached to
// a branch that did not fire return ErrBranchNotFired.
func ExampleTx_thenElse() {
	ctx := context.Background()
	store := storage.NewStorage(dummy.New())
	codec := newTxExampleCodec()

	// Seed a value the Else branch will read.
	seedTx := integrity.NewTx(store)

	err := codec.TxPut(seedTx, "fallback", txExampleValue{Field: "fallback-value"})
	if err != nil {
		log.Fatalf("seed: %v", err)
	}

	_, err = seedTx.Commit(ctx)
	if err != nil {
		log.Fatalf("seed commit: %v", err)
	}

	// Build a tx whose If can never hold.
	mainTx := integrity.NewTx(store)

	pred, err := codec.BindPredicate("missing", codec.VersionGreater(999))
	if err != nil {
		log.Fatalf("bind: %v", err)
	}

	mainTx.If(pred)

	thenFut := codec.TxGet(mainTx.Then(), "missing")
	elseFut := codec.TxGet(mainTx.Else(), "fallback")

	resp, err := mainTx.Commit(ctx)
	if err != nil {
		log.Fatalf("commit: %v", err)
	}

	fmt.Println("succeeded:", resp.Succeeded)

	_, thenErr := thenFut.Result()
	if errors.Is(thenErr, integrity.ErrBranchNotFired) {
		fmt.Println("then branch did not fire")
	}

	res, err := elseFut.Result()
	if err != nil {
		log.Fatalf("else result: %v", err)
	}

	fmt.Println("else field:", res.Value.Unwrap().Field)

	// Output:
	// succeeded: false
	// then branch did not fire
	// else field: fallback-value
}
