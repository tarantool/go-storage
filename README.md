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
- Distributed Locks: `Locker` interface for cross-process mutual exclusion, backed by the same drivers (etcd's `concurrency.Mutex`, a TCS "smallest mod_revision wins" protocol, and an in-memory dummy)
- Key‑Value Operations: Get, Put, Delete with prefix support
- Range Queries: Efficient scanning of keys with filters
- Extensible Drivers: Easy to add new storage backends

### Installation

```bash
go get github.com/tarantool/go-storage/v2
```

### Quick Start

#### Using etcd Driver

```go
package main

import (
    "context"
    "log"

    "go.etcd.io/etcd/client/v3"
    "github.com/tarantool/go-storage/v2/driver/etcd"
    "github.com/tarantool/go-storage/v2/operation"
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

    "github.com/tarantool/go-tarantool/v3"
    "github.com/tarantool/go-storage/v2/driver/tcs"
    "github.com/tarantool/go-storage/v2/operation"
)

func main() {
    ctx := context.Background()

    // Connect to Tarantool.
    conn, err := tarantool.Connect(ctx, tarantool.NetDialer{
        Address: "localhost:3301",
    }, tarantool.Opts{})
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Create TCS driver.
    driver := tcs.New(conn)

    // Execute a transaction.
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

    "github.com/tarantool/go-storage/v2/connect"
)

func main() {
    ctx := context.Background()

    cfg := connect.Config{
        Endpoints: []string{"localhost:2379"},
        Username:  "user",
        Password:  "pass",
    }

    // Automatically tries etcd first, then TCS.
    stor, cleanup, err := connect.Connect(ctx, cfg)
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

- `Watch(ctx, key) <-chan watch.Event` – watch for changes (key ending in `/` watches a prefix)
- `Tx(ctx) tx.Tx` – create a transaction builder
- `Range(ctx, opts) ([]kv.KeyValue, error)` – range query filtered by `WithPrefix`
- `NewLocker(ctx, name, opts) (locker.Locker, error)` – create a distributed lock bound to this storage

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
The `operation` package defines `Get`, `Put`, `Delete` operations.

#### Predicates
The `predicate` package provides value and version comparisons:

- `ValueEqual`, `ValueNotEqual`
- `VersionEqual`, `VersionNotEqual`, `VersionGreater`, `VersionLess`

#### Watch
The `watch` package delivers real‑time change events. Watch can be set on a
single key or a prefix.

#### Distributed Locks

`Storage.NewLocker(ctx, name, opts...)` returns a `locker.Locker` backed by the underlying driver — etcd uses `concurrency.Mutex`, TCS layers a "smallest mod_revision wins" protocol over its config-storage primitives, and the dummy driver provides an in-memory implementation suitable for tests. The `ctx` passed to `NewLocker` is the locker-lifetime context: cancelling it stops any keepalive goroutine and aborts a blocking `Lock`. Lock names live in the same key-space as values, so `storage.Prefixed` scopes them under its namespace too.

`locker.Do(ctx, factory, name, fn, opts...)` is the recommended pattern when the lifetime of the lock matches a single function call: it creates the Locker via the supplied `locker.Factory`, acquires it, runs `fn` while the lock is held, and releases the lock on return — even if `fn` errors. Use the manual `Lock`/`Unlock` dance only when the lock must outlive a single function (e.g. leader election).

`locker.Prefixed(prefix, inner)` is the Factory-level counterpart to `storage.Prefixed`: it returns a `locker.Factory` that namespaces every caller-supplied lock name under `prefix`, so a subcomponent can be handed a scoped lock binder without being given the full `Storage`. The same `/`-rooted, no-trailing-slash rules apply as for `storage.Prefixed`.

```go
ctx := context.Background()

stor, cleanup, err := connect.NewEtcdStorage(ctx, connect.Config{
    Endpoints: []string{"localhost:2379"},
})
if err != nil {
    log.Fatal(err)
}
defer cleanup()

err = locker.Do(ctx, stor.LockerFactory(), "/locks/leader", func(ctx context.Context) error {
    // critical section
    return nil
})
if err != nil {
    log.Fatal(err)
}
```

#### Data Integrity (`Codec`, `Store`, `Tx`)

The [`integrity`](https://pkg.go.dev/github.com/tarantool/go-storage/v2/integrity)
package stores and retrieves values with built‑in integrity protection. It
automatically computes hashes and signatures (using configurable algorithms)
and verifies them on retrieval.

##### Key Features
- Automatic Hash & Signature Generation: Values are stored together with
  their hashes and/or signatures.
- Validation on read: `Get` and `Range` operations verify hashes and
  signatures; invalid data is reported.
- Configurable Algorithms: Plug in any hasher (`hasher.Hasher`) and
  signer/verifier (`crypto.SignerVerifier`).
- **Layered Key Layout**: each key category lives under its own top-level
  location segment, keeping value/hash/signature keys collision-free.
- **Watch Support**: `Watch` reports changes for the codec's namespace.

The API is split into three pieces:

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
type MyConfig struct {
    Environment string `yaml:"environment"`
    Timeout     int    `yaml:"timeout"`
}

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

The integrity API builds its namer via `namer.New` by default, which places
each key category under its own top-level location segment:

```
/<objectLocation>/<name>                         (value)
/hashes/<hashLocation>/<objectLocation>/<name>   (one per hasher)
/sig/<sigLocation>/<objectLocation>/<name>       (one per signer)
```

`objectLocation` may itself be a multi-segment path (e.g. `"settings/ldap"`)
— useful when the on-disk hierarchy of a feature is fixed at codec build
time rather than per item. `hashLocation` and `sigLocation` are still
single tokens because they index per-hasher / per-signer maps. The first
segment of `objectLocation` must not equal the reserved markers `hashes` or
`sig` (they would collide with the parser's category dispatch).

`namer.CompactSingleHash()` and `namer.CompactSingleSig()` drop the
per-hasher / per-signer segment when exactly one is configured. `ParseKey`
parses a raw key back to `(name, KeyType, property)` unambiguously.

##### Singleton Store: One Fixed Key per Codec

For configuration objects that live at a single, known key (e.g.
`/settings/auth`) — not under a directory of `<objectLocation>/<id>`
items — `Codec[T].BindSingleton(storage, name)` returns a
`*SingletonStore[T]` that bakes the name in once. All operations
(`Get` / `Put` / `Delete` / `Watch`, plus `TxGet` / `TxPut` / `TxDelete`
for multi-op transactions) drop the `name` parameter:

```go
codec, err := integrity.NewCodecBuilder[AuthConfig]().
    WithObjectLocation("settings").
    WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privKey)).
    Build()
if err != nil {
    log.Fatal(err)
}

auth, err := codec.BindSingleton(baseStorage, "auth")
if err != nil {
    log.Fatal(err)
}

// Wire layout:
//   /settings/auth                        (value)
//   /hashes/<hashLoc>/settings/auth       (hash, per hasher)
//   /sig/<sigLoc>/settings/auth           (sig,  per signer)

if err := auth.Put(ctx, AuthConfig{Issuer: "example"}); err != nil {
    log.Fatal(err)
}

res, err := auth.Get(ctx)
```

The same `Codec[T]` can serve both shapes: `codec.Bind(storage)` for a
directory of items keyed by name, `codec.BindSingleton(storage, name)`
for a fixed singleton. Predicates from the codec (`ValueEqual`,
`VersionEqual`, etc.) are name-agnostic and work as-is when passed
through `WithPutPredicates(...)` / `WithDeletePredicates(...)`. For
multi-op transactions, `auth.BindPredicate(pred)` resolves the predicate
to the singleton's value-layer key for use in `Tx.If`.

##### Marshallers

Beyond the default YAML marshaller, the `marshaller` package now ships:

- `JSONMarshaller[T]` — `encoding/json`-based marshalling for any
  Go type.
- `BytesMarshaller` — passthrough `Marshaller[[]byte]` for values
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

[godoc-badge]: https://pkg.go.dev/badge/github.com/tarantool/go-storage/v2.svg
[godoc-url]: https://pkg.go.dev/github.com/tarantool/go-storage/v2
[coverage-badge]: https://coveralls.io/repos/github/tarantool/go-storage/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/tarantool/go-storage?branch=master
[telegram-badge]: https://img.shields.io/badge/Telegram-join%20chat-blue.svg
[telegram-en-url]: http://telegram.me/tarantool
[telegram-ru-url]: http://telegram.me/tarantoolru
