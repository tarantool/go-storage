package storage_test

import (
	"context"
	"fmt"
	"log"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
)

// ExamplePrefixed shows how Prefixed scopes every Tx operation under a
// namespace. Callers work with logical keys ("/cfg/version"); the underlying
// driver sees absolute keys ("/ns/cfg/version"), and the namespace is stripped
// from any keys returned to the caller.
func ExamplePrefixed() {
	ctx := context.Background()
	base := storage.NewStorage(dummy.New())

	scoped, err := storage.Prefixed("/ns", base)
	if err != nil {
		log.Fatalf("prefix: %v", err)
	}

	_, err = scoped.Tx(ctx).Then(
		operation.Put([]byte("/cfg/version"), []byte("1.0.0")),
	).Commit()
	if err != nil {
		log.Fatalf("put: %v", err)
	}

	resp, err := scoped.Tx(ctx).Then(
		operation.Get([]byte("/cfg/version")),
	).Commit()
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	got := resp.Results[0].Values[0]
	fmt.Printf("logical key: %s, value: %s\n", got.Key, got.Value)

	// Output:
	// logical key: /cfg/version, value: 1.0.0
}

// ExamplePrefixed_nested shows that nested wrappers are flattened at
// construction time: Prefixed("/a", Prefixed("/b", base)) is equivalent to
// Prefixed("/a/b", base). This is observable by reading the same key through
// the un-wrapped base storage.
func ExamplePrefixed_nested() {
	ctx := context.Background()
	base := storage.NewStorage(dummy.New())

	inner, err := storage.Prefixed("/b", base)
	if err != nil {
		log.Fatalf("prefix /b: %v", err)
	}

	outer, err := storage.Prefixed("/a", inner)
	if err != nil {
		log.Fatalf("prefix /a: %v", err)
	}

	_, err = outer.Tx(ctx).Then(
		operation.Put([]byte("/k"), []byte("v")),
	).Commit()
	if err != nil {
		log.Fatalf("put: %v", err)
	}

	// Read directly from base to see the absolute key the driver actually stored.
	resp, err := base.Tx(ctx).Then(
		operation.Get([]byte("/a/b/k")),
	).Commit()
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	got := resp.Results[0].Values[0]
	fmt.Printf("absolute: %s = %s\n", got.Key, got.Value)

	// Output:
	// absolute: /a/b/k = v
}

// ExamplePrefixed_predicates shows that predicates are also rewritten under
// the configured prefix, so conditional transactions stay scoped.
func ExamplePrefixed_predicates() {
	ctx := context.Background()

	scoped, err := storage.Prefixed("/ns", storage.NewStorage(dummy.New()))
	if err != nil {
		log.Fatalf("prefix: %v", err)
	}

	_, err = scoped.Tx(ctx).Then(
		operation.Put([]byte("/feature"), []byte("on")),
	).Commit()
	if err != nil {
		log.Fatalf("seed: %v", err)
	}

	resp, err := scoped.Tx(ctx).
		If(predicate.ValueEqual([]byte("/feature"), "on")).
		Then(operation.Put([]byte("/feature"), []byte("off"))).
		Else(operation.Put([]byte("/feature"), []byte("unchanged"))).
		Commit()
	if err != nil {
		log.Fatalf("commit: %v", err)
	}

	fmt.Println("succeeded:", resp.Succeeded)

	// Output:
	// succeeded: true
}
