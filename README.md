[![Go Reference][godoc-badge]][godoc-url]
[![Code Coverage][coverage-badge]][coverage-url]
[![Telegram EN][telegram-badge]][telegram-en-url]
[![Telegram RU][telegram-badge]][telegram-ru-url]

# go-storage: library to manage centralized configuration storages

### About

<a href="http://tarantool.org">
    <img align="right" src="assets/logo.png" width="250" alt="Tarantool Logo">
</a>

**go-storage** is a Go library that provides a uniform interface for managing
centralized configuration storages, supporting multiple backends like etcd and
Tarantool Config Storage (TCS). It offers transactional operations, conditional
predicates, real-time watch, and data integrity features.


### Overview

The library abstracts the complexities of different storage backends, providing
a consistent API for configuration management. It is designed for distributed
systems where configuration consistency, real-time updates, and transactional
safety are critical.

### Features

- Unified Storage Interface: Single API for multiple backend drivers (etcd,
  TCS)
- Transactional Operations: Atomic transactions with conditional predicates
- Real-time Watch: Monitor changes to keys and prefixes
- Conditional Execution: Value and version-based predicates for safe updates
- Data Integrity: Built-in signing and verification of stored data
- Key‑Value Operations: Get, Put, Delete with prefix support
- Range Queries: Efficient scanning of keys with filters
- Extensible Drivers: Easy to add new storage backends

### Installation

```bash
go get github.com/tarantool/go-storage
```

### Quick Start

#### Using etcd Driver

```go
package main

import (
    "context"
    "log"

    "go.etcd.io/etcd/client/v3"
    "github.com/tarantool/go-storage/driver/etcd"
    "github.com/tarantool/go-storage/operation"
)

func main() {
    // Connect to etcd.
    cli, err := clientv3.New(clientv3.Config{
        Endpoints: []string{"localhost:2379"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    // Create etcd driver.
    driver := etcd.New(cli)

    // Execute a simple Put operation.
    ctx := context.Background()
    _, err = driver.Execute(ctx, nil, []operation.Operation{
        operation.Put([]byte("/config/app/version"), []byte("1.0.0")),
    }, nil)
    if err != nil {
        log.Fatal(err)
    }
}
```

#### Using TCS Driver

```go
package main

import (
    "context"
    "log"

    "github.com/tarantool/go-tarantool/v2"
    "github.com/tarantool/go-storage/driver/tcs"
    "github.com/tarantool/go-storage/operation"
)

func main() {
    // Connect to Tarantool.
    conn, err := tarantool.Connect("localhost:3301", tarantool.Opts{})
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Create TCS driver.
    driver := tcs.New(conn)

    // Execute a transaction.
    ctx := context.Background()
    resp, err := driver.Execute(ctx, nil, []operation.Operation{
        operation.Put([]byte("/config/app/name"), []byte("MyApp")),
    }, nil)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Transaction succeeded: %v", resp.Succeeded)
}
```

### Drivers

#### etcd Driver
The `driver/etcd` package implements the storage driver interface for etcd. It
supports all etcd features including conditional transactions, leases, and
watch.

#### TCS Driver
The `driver/tcs` package provides a driver for Tarantool Config Storage (TCS),
a distributed key‑value storage built on Tarantool. It offers high performance
and strong consistency.

### API Overview

#### Storage Interface
The core `Storage` interface (`storage.Storage`) provides high‑level methods:

- `Watch(ctx, key, opts) <-chan watch.Event` – watch for changes
- `Tx(ctx) tx.Tx` – create a transaction builder
- `Range(ctx, opts) ([]kv.KeyValue, error)` – range query with prefix/limit

#### Transaction Builder
The `tx.Tx` interface enables conditional transactions:

```go
resp, err := storage.Tx(ctx).
    If(predicate.ValueEqual(key, "old")).
    Then(operation.Put(key, "new")).
    Else(operation.Delete(key)).
    Commit()
```

#### Operations
The `operation` package defines `Get`, `Put`, `Delete` operations. Each
operation can be configured with options.

#### Predicates
The `predicate` package provides value and version comparisons:

- `ValueEqual`, `ValueNotEqual`
- `VersionEqual`, `VersionNotEqual`, `VersionGreater`, `VersionLess`

#### Watch
The `watch` package delivers real‑time change events. Watch can be set on a
single key or a prefix.

#### Data Integrity with Typed Storage
The [`integrity`](https://pkg.go.dev/github.com/tarantool/go-storage/integrity)
 package provides a high‑level `Typed` interface for storing and retrieving
  values with built‑in integrity protection. It automatically computes hashes
   and signatures (using configurable algorithms) and verifies them on
   retrieval.

##### Creating a Typed Storage Instance

```go
package main

import (
    "context"
    "crypto/rand"
    "crypto/rsa"
    "log"

    clientv3 "go.etcd.io/etcd/client/v3"
    "github.com/tarantool/go-storage"
    "github.com/tarantool/go-storage/driver/etcd"
    "github.com/tarantool/go-storage/hasher"
    "github.com/tarantool/go-storage/crypto"
    "github.com/tarantool/go-storage/integrity"
)

func main() {
    // 1. Create a base storage (e.g., etcd driver).
    cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    driver := etcd.New(cli)
    baseStorage := storage.NewStorage(driver)

    // 2. Generate RSA keys (in production, load from secure storage).
    privKey, err := rsa.GenerateKey(rand.Reader, 2048)
    if err != nil {
        log.Fatal(err)
    }

    // 3. Build typed storage with integrity protection.
    typed := integrity.NewTypedBuilder[MyConfig](baseStorage).
        WithPrefix("/config").
        WithHasher(hasher.NewSHA256Hasher()).          // adds SHA‑256 hash verification.
        WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privKey)). // adds RSA‑PSS signatures.
        Build()

    // 4. Store a configuration object with automatic integrity data.
    ctx := context.Background()
    config := MyConfig{Environment: "production", Timeout: 30}
    if err := typed.Put(ctx, "app/settings", config); err != nil {
        log.Fatal(err)
    }

    // 5. Retrieve and verify integrity.
    result, err := typed.Get(ctx, "app/settings")
    if err != nil {
        log.Fatal(err)
    }

    if result.Error != nil {
        log.Printf("Integrity check failed: %v", result.Error)
    } else {
        cfg, _ := result.Value.Get()
        log.Printf("Retrieved valid config: %+v", cfg)
    }

    // 6. Range over all configurations under a prefix.
    results, err := typed.Range(ctx, "app/")
    if err != nil {
        log.Fatal(err)
    }
    for _, res := range results {
        log.Printf("Found config %s (valid: %v)", res.Name, res.Error == nil)
    }
}

type MyConfig struct {
    Environment string `yaml:"environment"`
    Timeout     int    `yaml:"timeout"`
}
```

##### Key Features
- Automatic Hash & Signature Generation: Values are stored together with
  their hashes and/or signatures.
- Validation on read: `Get` and `Range` operations verify hashes and
  signatures; invalid data is reported.
- Configurable Algorithms: Plug in any hasher (`hasher.Hasher`) and
  signer/verifier (`crypto.SignerVerifier`).
- Prefix Isolation**: Each typed storage uses a configurable key prefix,
  avoiding collisions.
- **Watch Support**: `Watch` method filters events for the typed namespace.

The `integrity.Typed` builder also accepts custom marshallers (default is
YAML), custom namers, and separate signer/verifier instances for asymmetric
setups.

### Examples

Comprehensive examples are available in the driver packages:

- **etcd examples**: [`driver/etcd/examples_test.go`](driver/etcd/examples_test.go)
- **TCS examples**: [`driver/tcs/examples_test.go`](driver/tcs/examples_test.go)

Run them with `go test -v -run Example ./driver/etcd` or `./driver/tcs`.

### Contributing

Contributions are welcome! Please see the [CONTRIBUTING.md](CONTRIBUTING.md)
file for guidelines (if present) or open an issue to discuss your ideas.

### License

This project is licensed under the BSD 2‑Clause License – see the [LICENSE](LICENSE) file for details.

[godoc-badge]: https://pkg.go.dev/badge/github.com/tarantool/go-storage.svg
[godoc-url]: https://pkg.go.dev/github.com/tarantool/go-storage
[coverage-badge]: https://coveralls.io/repos/github/tarantool/go-storage/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/tarantool/go-storage?branch=master
[telegram-badge]: https://img.shields.io/badge/Telegram-join%20chat-blue.svg
[telegram-en-url]: http://telegram.me/tarantool
[telegram-ru-url]: http://telegram.me/tarantoolru
