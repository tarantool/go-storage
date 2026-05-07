package integrity_test

import (
	"context"
	"fmt"
	"log"

	storage "github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
)

type authConfig struct {
	Issuer string `yaml:"issuer"`
}

// ExampleCodec_BindSingleton shows the canonical fixed-key configuration
// pattern: a single object at "/settings/auth" instead of a directory of
// "/settings/auth/<id>" items. The bound name is supplied once; operations
// take no name parameter.
func ExampleCodec_BindSingleton() {
	ctx := context.Background()

	codec, err := integrity.NewCodecBuilder[authConfig]().
		WithObjectLocation("settings").
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	if err != nil {
		log.Fatalf("build codec: %v", err)
	}

	auth, err := codec.BindSingleton(storage.NewStorage(dummy.New()), "auth")
	if err != nil {
		log.Fatalf("bind singleton: %v", err)
	}

	err = auth.Put(ctx, authConfig{Issuer: "tarantool"})
	if err != nil {
		log.Fatalf("put: %v", err)
	}

	res, err := auth.Get(ctx)
	if err != nil {
		log.Fatalf("get: %v", err)
	}

	fmt.Println("issuer:", res.Value.Unwrap().Issuer)

	// Output:
	// issuer: tarantool
}
