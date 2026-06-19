package integrity_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"log"

	storage "github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/crypto"
	"github.com/tarantool/go-storage/v2/driver/dummy"
	"github.com/tarantool/go-storage/v2/hasher"
	"github.com/tarantool/go-storage/v2/integrity"
)

type codecExampleConfig struct {
	Name string `yaml:"name"`
	N    int    `yaml:"n"`
}

// ExampleNewCodecBuilder builds a Codec[T] using the fluent builder.
// Each WithX call returns a copy, so the original builder is never mutated.
func ExampleNewCodecBuilder() {
	codec, err := integrity.NewCodecBuilder[codecExampleConfig]().WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	if err != nil {
		log.Fatalf("build codec: %v", err)
	}

	store := codec.Bind(storage.NewStorage(dummy.New()))

	ctx := context.Background()

	err = store.Put(ctx, "alice", codecExampleConfig{Name: "alice", N: 42})
	if err != nil {
		log.Fatalf("put: %v", err)
	}

	res, err := store.Get(ctx, "alice")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	got := res.Value.Unwrap()
	fmt.Printf("Name=%s N=%d\n", got.Name, got.N)

	// Output:
	// Name=alice N=42
}

// ExampleCodecBuilder_WithObjectLocation shows overriding the value-layer
// location segment. The default is "objects".
func ExampleCodecBuilder_WithObjectLocation() {
	codec, err := integrity.NewCodecBuilder[codecExampleConfig]().WithObjectLocation("objects").
		WithObjectLocation("users").
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	if err != nil {
		log.Fatalf("build codec: %v", err)
	}

	store := codec.Bind(storage.NewStorage(dummy.New()))

	ctx := context.Background()

	err = store.Put(ctx, "alice", codecExampleConfig{Name: "alice", N: 1})
	if err != nil {
		log.Fatalf("put: %v", err)
	}

	// The value lands under /users/alice instead of /objects/alice.
	res, err := store.Get(ctx, "alice")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Println("name:", res.Value.Unwrap().Name)

	// Output:
	// name: alice
}

// ExampleNewCodecBuilder_immutability demonstrates copy-on-write: the
// original builder is unchanged after a setter call, so two divergent codecs
// can be built from the same base.
func ExampleNewCodecBuilder_immutability() {
	base := integrity.NewCodecBuilder[codecExampleConfig]().WithObjectLocation("objects")

	withHasher := base.WithHasher(hasher.NewSHA256Hasher())
	withoutHasher := base // unchanged.

	codecHashed, err := withHasher.Build()
	if err != nil {
		log.Fatalf("codecHashed: %v", err)
	}

	codecPlain, err := withoutHasher.Build()
	if err != nil {
		log.Fatalf("codecPlain: %v", err)
	}

	fmt.Println("two distinct codecs:", codecHashed != codecPlain)

	// Output:
	// two distinct codecs: true
}

// ExampleCodecBuilder_WithSignerVerifier configures a codec with both a
// hasher and an RSA-PSS signer/verifier. Put writes value, hash, and
// signature keys; Get verifies all of them on read.
func ExampleCodecBuilder_WithSignerVerifier() {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("genkey: %v", err)
	}

	codec, err := integrity.NewCodecBuilder[codecExampleConfig]().WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithSignerVerifier(crypto.NewRSAPSS(*priv)).
		Build()
	if err != nil {
		log.Fatalf("build codec: %v", err)
	}

	store := codec.Bind(storage.NewStorage(dummy.New()))

	ctx := context.Background()

	err = store.Put(ctx, "alice", codecExampleConfig{Name: "alice", N: 7})
	if err != nil {
		log.Fatalf("put: %v", err)
	}

	res, err := store.Get(ctx, "alice")
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Printf("verified Name=%s N=%d\n", res.Value.Unwrap().Name, res.Value.Unwrap().N)

	// Output:
	// verified Name=alice N=7
}

// ExampleCodec_ValueEqual produces a Codec-bound predicate that can be passed
// to Tx.If via Codec.BindPredicate, or used through Store helpers like
// WithPutPredicates.
func ExampleCodec_ValueEqual() {
	codec, err := integrity.NewCodecBuilder[codecExampleConfig]().WithObjectLocation("objects").Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}

	pred, err := codec.ValueEqual(codecExampleConfig{Name: "alice", N: 1})
	if err != nil {
		log.Fatalf("predicate: %v", err)
	}

	bound, err := codec.BindPredicate("alice", pred)
	if err != nil {
		log.Fatalf("bind: %v", err)
	}

	// The bound predicate targets the value-layer key for "alice".
	fmt.Printf("predicate key: %s\n", bound.Key())

	// Output:
	// predicate key: /objects/alice
}
