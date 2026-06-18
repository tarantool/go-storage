package integrity_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

	storage "github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/driver/dummy"
	"github.com/tarantool/go-storage/v2/hasher"
	"github.com/tarantool/go-storage/v2/integrity"
)

type storeExampleValue struct {
	Field string `yaml:"field"`
}

func newExampleCodec() *integrity.Codec[storeExampleValue] {
	codec, err := integrity.NewCodecBuilder[storeExampleValue]().WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	if err != nil {
		log.Fatalf("build codec: %v", err)
	}

	return codec
}

func newExampleStore() *integrity.Store[storeExampleValue] {
	return newExampleCodec().Bind(storage.NewStorage(dummy.New()))
}

// ExampleStore demonstrates a Put/Get round-trip via Store[T]. Each method
// is a thin single-op wrapper around a Tx — observably indistinguishable
// from a single-op Tx.Commit.
func ExampleStore() {
	ctx := context.Background()
	store := newExampleStore()

	err := store.Put(ctx, "alice", storeExampleValue{Field: "hello"})
	if err != nil {
		log.Fatalf("put: %v", err)
	}

	res, err := store.Get(ctx, "alice")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Println("name:", res.Name)
	fmt.Println("field:", res.Value.Unwrap().Field)

	// Output:
	// name: alice
	// field: hello
}

// ExampleStore_Put_predicate uses WithPutPredicates to make the write
// conditional. ErrPredicateFailed is returned when the predicate does not hold.
func ExampleStore_Put_predicate() {
	ctx := context.Background()
	codec := newExampleCodec()
	store := codec.Bind(storage.NewStorage(dummy.New()))

	err := store.Put(ctx, "k", storeExampleValue{Field: "v1"})
	if err != nil {
		log.Fatalf("seed: %v", err)
	}

	pred, err := codec.ValueEqual(storeExampleValue{Field: "v1"})
	if err != nil {
		log.Fatalf("predicate: %v", err)
	}

	// Predicate matches: this update succeeds.
	err = store.Put(ctx, "k", storeExampleValue{Field: "v2"},
		integrity.WithPutPredicates(pred))
	fmt.Println("first update err:", err)

	// Predicate no longer matches (current value is "v2"): ErrPredicateFailed.
	err = store.Put(ctx, "k", storeExampleValue{Field: "v3"},
		integrity.WithPutPredicates(pred))
	fmt.Println("second update is ErrPredicateFailed:", errors.Is(err, integrity.ErrPredicateFailed))

	// Output:
	// first update err: <nil>
	// second update is ErrPredicateFailed: true
}

// ExampleStore_Range fetches and validates every value under a name prefix.
// Pass "" to fetch everything under the codec's object location.
func ExampleStore_Range() {
	ctx := context.Background()
	store := newExampleStore()

	for _, name := range []string{"users/alice", "users/bob", "groups/admins"} {
		err := store.Put(ctx, name, storeExampleValue{Field: name})
		if err != nil {
			log.Fatalf("put %s: %v", name, err)
		}
	}

	results, err := store.Range(ctx, "users/")
	if err != nil {
		log.Fatalf("range: %v", err)
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

// ExampleStore_Watch streams events for changes to a specific name. The
// emitted event Prefix is the codec-internal absolute key; Watch filters out
// events whose key does not belong to this codec's namespace.
func ExampleStore_Watch() {
	ctx, cancel := context.WithCancel(context.Background())

	store := newExampleStore()

	events, err := store.Watch(ctx, "alice")
	if err != nil {
		cancel()
		log.Fatalf("watch: %v", err)
	}

	go func() {
		_ = store.Put(ctx, "alice", storeExampleValue{Field: "v"})
	}()

	ev := <-events

	cancel()

	fmt.Println("event:", string(ev.Key))

	// Output:
	// event: /objects/alice
}

// ExampleStore_Delete_withPrefix removes every value (and its hashes) whose
// name starts with the given prefix. The prefix must end with '/'.
func ExampleStore_Delete_withPrefix() {
	ctx := context.Background()
	store := newExampleStore()

	for _, name := range []string{"tmp/a", "tmp/b"} {
		err := store.Put(ctx, name, storeExampleValue{Field: name})
		if err != nil {
			log.Fatalf("put %s: %v", name, err)
		}
	}

	before, err := store.Range(ctx, "tmp/")
	if err != nil {
		log.Fatalf("range before: %v", err)
	}

	fmt.Println("before:", len(before))

	err = store.Delete(ctx, "tmp/", integrity.WithPrefix())
	if err != nil {
		log.Fatalf("delete: %v", err)
	}

	after, err := store.Range(ctx, "tmp/")
	if err != nil {
		log.Fatalf("range after: %v", err)
	}

	fmt.Println("after:", len(after))

	// Output:
	// before: 2
	// after: 0
}
