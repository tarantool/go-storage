package namer_test

import (
	"fmt"
	"log"

	"github.com/tarantool/go-storage/namer"
)

// ExampleNewLayeredNamer demonstrates the default layered key layout:
//
//	/<obj>/<name>                       (value)
//	/hash/<hashLoc>/<obj>/<name>        (one per hasher)
//	/sig/<sigLoc>/<obj>/<name>          (one per signer)
func ExampleNewLayeredNamer() {
	layered, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "rsa", Location: "rsa"}},
	)
	if err != nil {
		log.Fatalf("new namer: %v", err)
	}

	keys, err := layered.GenerateNames("alice")
	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	for _, k := range keys {
		fmt.Printf("%s -> %s\n", k.Type(), k.Build())
	}

	// Output:
	// KeyTypeValue -> /objects/alice
	// KeyTypeHash -> /hash/sha256/objects/alice
	// KeyTypeSignature -> /sig/rsa/objects/alice
}

// ExampleCompactSingleHash shows the compact hash layout, which drops the
// per-hasher segment when exactly one hasher is configured.
func ExampleCompactSingleHash() {
	layered, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
		namer.CompactSingleHash(),
	)
	if err != nil {
		log.Fatalf("new namer: %v", err)
	}

	keys, err := layered.GenerateNames("alice")
	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	for _, k := range keys {
		fmt.Printf("%s -> %s\n", k.Type(), k.Build())
	}

	// Output:
	// KeyTypeValue -> /objects/alice
	// KeyTypeHash -> /hash/objects/alice
}

// ExampleCompactSingleSig shows the compact sig layout, which drops the
// per-signer segment when exactly one signer is configured.
func ExampleCompactSingleSig() {
	layered, err := namer.NewLayeredNamer(
		"objects",
		nil,
		[]namer.LayeredSigLocation{{SignerName: "rsapss", Location: "rsapss"}},
		namer.CompactSingleSig(),
	)
	if err != nil {
		log.Fatalf("new namer: %v", err)
	}

	keys, err := layered.GenerateNames("alice")
	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	for _, k := range keys {
		fmt.Printf("%s -> %s\n", k.Type(), k.Build())
	}

	// Output:
	// KeyTypeValue -> /objects/alice
	// KeyTypeSignature -> /sig/objects/alice
}

// ExampleNewLayeredNamer_parse shows how ParseKey resolves a raw key path
// back into its category and object name.
func ExampleNewLayeredNamer_parse() {
	layered, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
	)
	if err != nil {
		log.Fatalf("new namer: %v", err)
	}

	for _, raw := range []string{
		"/objects/alice",
		"/hash/sha256/objects/alice",
	} {
		k, err := layered.ParseKey(raw)
		if err != nil {
			log.Fatalf("parse %q: %v", raw, err)
		}

		fmt.Printf("%s name=%s property=%q\n", k.Type(), k.Name(), k.Property())
	}

	// Output:
	// KeyTypeValue name=alice property=""
	// KeyTypeHash name=alice property="sha256"
}

// ExampleNewLayeredNamer_prefix shows the prefix used to walk all values
// under the value layer (hashes/sigs are separate sub-trees).
func ExampleNewLayeredNamer_prefix() {
	layered, err := namer.NewLayeredNamer("objects", nil, nil)
	if err != nil {
		log.Fatalf("new namer: %v", err)
	}

	fmt.Println(layered.Prefix("", false))
	fmt.Println(layered.Prefix("alice", false))
	fmt.Println(layered.Prefix("users", true))

	// Output:
	// /objects/
	// /objects/alice
	// /objects/users/
}
