package namer_test

import (
	"fmt"
	"log"

	"github.com/tarantool/go-storage/v2/namer"
)

// ExampleNew demonstrates the default layered key layout:
//
//	/<obj>/<name>                       (value)
//	/hashes/<hashLoc>/<obj>/<name>        (one per hasher)
//	/sig/<sigLoc>/<obj>/<name>          (one per signer)
func ExampleNew() {
	layered, err := namer.New(
		"objects",
		[]namer.HashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.SigLocation{{SignerName: "rsa", Location: "rsa"}},
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
	// value -> /objects/alice
	// hash -> /hashes/sha256/objects/alice
	// signature -> /sig/rsa/objects/alice
}

// ExampleCompactSingleHash shows the compact hash layout, which drops the
// per-hasher segment when exactly one hasher is configured.
func ExampleCompactSingleHash() {
	layered, err := namer.New(
		"objects",
		[]namer.HashLocation{{HasherName: "sha256", Location: "sha256"}},
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
	// value -> /objects/alice
	// hash -> /hashes/objects/alice
}

// ExampleCompactSingleSig shows the compact sig layout, which drops the
// per-signer segment when exactly one signer is configured.
func ExampleCompactSingleSig() {
	layered, err := namer.New(
		"objects",
		nil,
		[]namer.SigLocation{{SignerName: "rsapss", Location: "rsapss"}},
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
	// value -> /objects/alice
	// signature -> /sig/objects/alice
}

// ExampleLegacyHashSigLayout shows the legacy product layout: hash and sig
// keys drop the objectLocation segment while value keys keep it.
func ExampleLegacyHashSigLayout() {
	layered, err := namer.New(
		"config",
		[]namer.HashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.SigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		namer.LegacyHashSigLayout(),
	)
	if err != nil {
		log.Fatalf("new namer: %v", err)
	}

	keys, err := layered.GenerateNames("all")
	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	for _, k := range keys {
		fmt.Printf("%s -> %s\n", k.Type(), k.Build())
	}

	// Output:
	// value -> /config/all
	// hash -> /hashes/sha256/all
	// signature -> /sig/ed25519/all
}

// ExampleNew_parse shows how ParseKey resolves a raw key path
// back into its category and object name.
func ExampleNew_parse() {
	layered, err := namer.New(
		"objects",
		[]namer.HashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
	)
	if err != nil {
		log.Fatalf("new namer: %v", err)
	}

	for _, raw := range []string{
		"/objects/alice",
		"/hashes/sha256/objects/alice",
	} {
		k, err := layered.ParseKey(raw)
		if err != nil {
			log.Fatalf("parse %q: %v", raw, err)
		}

		fmt.Printf("%s name=%s property=%q\n", k.Type(), k.Name(), k.Property())
	}

	// Output:
	// value name=alice property=""
	// hash name=alice property="sha256"
}

// ExampleNew_prefix shows the prefix used to walk all values
// under the value layer (hashes/sigs are separate sub-trees).
func ExampleNew_prefix() {
	layered, err := namer.New("objects", nil, nil)
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
