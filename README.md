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
- Schema-Driven Integrity API: `Codec[T]` / `Store[T]` for type-safe values
  plus multi-key atomic transactions via `Tx`
- Namespace Scoping: `Prefixed` wrapper that scopes every operation under a
  key prefix
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

### Connection Utilities

The `connect` package provides a simplified way to connect to storage backends
using a unified configuration. It handles connection establishment, SSL/TLS
setup, and authentication.

> **Note**: Connecting to Tarantool Config Storage (TCS) with SSL requires the
> `go_storage_ssl` build tag. Without this tag, SSL support is disabled and
> attempting to connect with `SSL.Enable = true` will return `ErrSSLDisabled`.

#### Quick Start with Connect

```go
package main

import (
    "context"
    "log"

    "github.com/tarantool/go-storage/connect"
)

func main() {
    ctx := context.Background()

    cfg := connect.Config{
        Endpoints: []string{"localhost:2379"},
        Username:  "user",
        Password:  "pass",
    }

    // Automatically tries etcd first, then TCS.
    stor, cleanup, err := connect.NewStorage(ctx, cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer cleanup()

    // Use the storage...
    _ = stor
}
```

#### Explicit Backend Selection

```go
// Connect to etcd specifically.
stor, cleanup, err := connect.NewEtcdStorage(ctx, cfg)

// Connect to TCS specifically.
stor, cleanup, err := connect.NewTCSStorage(ctx, cfg)
```

#### SSL/TLS Configuration

```go
cfg := connect.Config{
    Endpoints: []string{"localhost:2379"},
    SSL: connect.SSLConfig{
        Enable:     true,
        CaFile:     "/path/to/ca.crt",
        CertFile:   "/path/to/client.crt",
        KeyFile:    "/path/to/client.key",
        VerifyPeer: true,
    },
}
```

### API Overview

#### Storage Interface
The core `Storage` interface (`storage.Storage`) provides high‑level methods:

- `Watch(ctx, key, opts) <-chan watch.Event` – watch for changes
- `Tx(ctx) tx.Tx` – create a transaction builder
- `Range(ctx, opts) ([]kv.KeyValue, error)` – range query with prefix/limit

#### Namespace Scoping with `Prefixed`

`storage.Prefixed(prefix, inner)` returns a `Storage` that transparently
prepends `prefix` to every operation, predicate, Range, and Watch call, and
strips it back from any keys returned to the caller. Nested wrappers are
flattened at construction (`Prefixed("/a", Prefixed("/b", base))` is
equivalent to `Prefixed("/a/b", base)`).

```go
scoped := storage.Prefixed("/ns", storage.NewStorage(driver))

// Caller writes /cfg/version; the driver actually stores /ns/cfg/version.
_, err := scoped.Tx(ctx).Then(
    operation.Put([]byte("/cfg/version"), []byte("1.0.0")),
).Commit()
```

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

    ctx := context.Background()

    // 4. Store a configuration object with automatic integrity data.
    config := MyConfig{Environment: "production", Timeout: 30}
    if err := typed.Put(ctx, "app/settings", config); err != nil {
        log.Fatal(err)
    }

    // 4.5 Store an object using predicates.
    p, _ := typed.ValueEqual(config)

    if err := typed.Put(ctx,
        "app/settings",
        config,
        integrity.WithPutPredicates(p),
    ); err != nil {
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
- **Prefix Isolation**: Each typed storage uses a configurable key prefix,
  avoiding collisions.
- **Watch Support**: `Watch` method filters events for the typed namespace.

The `integrity.Typed` builder also accepts custom marshallers (default is
YAML), custom namers, and separate signer/verifier instances for asymmetric
setups.

#### Schema-Driven Integrity API (`Codec`, `Store`, `Tx`)

Alongside `integrity.Typed`, the package exposes a schema-first API split
into three pieces:

- `integrity.Codec[T]` describes the on-disk layout (object location,
  hashers, signers, marshaller) without binding to any storage handle. It is
  built via the fluent `CodecBuilder[T]`, which validates location-override
  keys eagerly so typos like `WithHashLocation("sah256", …)` fail at
  `Build()` instead of being silently ignored.
- `integrity.Store[T]` is a codec bound to a `storage.Storage` and exposes
  the familiar `Get` / `Put` / `Delete` / `Range` / `Watch` methods.
- `integrity.Tx` accumulates `TxGet` / `TxPut` / `TxDelete` / `TxRange`
  calls from one or more codecs and commits them atomically through a
  single storage call. Reads return typed futures (`GetFuture[T]`,
  `RangeFuture[T]`) whose `Result()` is populated after `Commit`.

##### Codec and Store

```go
codec, err := integrity.NewCodecBuilder[MyConfig]().
    WithObjectLocation("config").
    WithHasher(hasher.NewSHA256Hasher()).
    Build()
if err != nil {
    log.Fatal(err)
}

store := codec.Bind(baseStorage)

if err := store.Put(ctx, "app/settings", MyConfig{...}); err != nil {
    log.Fatal(err)
}

res, err := store.Get(ctx, "app/settings")
if err != nil {
    log.Fatal(err)
}
cfg := res.Value.Unwrap()
```

##### Multi-Key Transactions

`Tx` batches reads and writes — across multiple codecs if needed — into one
atomic storage call. `If` predicates are routed by `Then` / `Else`; futures
attached to the branch that did not fire return `ErrBranchNotFired`.

```go
txn := integrity.NewTx(baseStorage)

pred, _ := codec.ValueEqual(MyConfig{...})
bound, _ := codec.BindPredicate("app/settings", pred)
txn.If(bound)

newFut := codec.TxGet(txn.Then(), "app/new-settings")
_ = codec.TxPut(txn.Then(), "app/settings", MyConfig{...})

resp, err := txn.Commit(ctx)
if err != nil {
    log.Fatal(err)
}
if !resp.Succeeded {
    // The Then branch did not fire; newFut.Result() returns
    // integrity.ErrBranchNotFired.
}
```

##### Layered Key Layout

The new API uses `namer.LayeredNamer` by default, which places each key
category under its own top-level location segment:

```
/<objectLocation>/<name>                       (value)
/hash/<hashLocation>/<objectLocation>/<name>   (one per hasher)
/sig/<sigLocation>/<objectLocation>/<name>     (one per signer)
```

`namer.CompactSingleHash()` and `namer.CompactSingleSig()` drop the
per-hasher / per-signer segment when exactly one is configured. `ParseKey`
parses a raw key back to `(name, KeyType, property)` unambiguously.

##### Marshallers

Beyond the default YAML marshaller, the `marshaller` package now ships:

- `TypedJSONMarshaller[T]` — `encoding/json`-based marshalling for any
  Go type.
- `TypedBytesMarshaller` — passthrough `TypedMarshaller[[]byte]` for values
  that are already serialized or stored as opaque blobs.

### Examples

Comprehensive examples are available in the driver packages:

- **etcd examples**: [`driver/etcd/examples_test.go`](driver/etcd/examples_test.go)
- **TCS examples**: [`driver/tcs/examples_test.go`](driver/tcs/examples_test.go)

Run them with `go test -v -run Example ./driver/etcd` or `./driver/tcs`.

### Build Tags

The library supports the following build tags:

#### `go_storage_ssl`

Enables SSL/TLS support for Tarantool Config Storage connections. This tag
requires the [`go-tlsdialer`](https://github.com/tarantool/go-tlsdialer)
dependency.

```bash
# Build with SSL support for TCS
go build -tags go_storage_ssl ./...
```

Without this tag:
- SSL support for TCS is disabled
- Connecting to TCS with `SSL.Enable = true` returns `ErrSSLDisabled`
- The `go-tlsdialer` dependency and CGO is not required on build-time

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
