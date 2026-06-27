# Migration guide

## Migration from v1.x.x to v2.x.x

* [Major changes](#major-changes-v2)
* [Updating import paths](#updating-import-paths)
* [go.mod](#gomod)
* [No API changes](#no-api-changes)

### <a id="major-changes-v2">Major changes</a>

The only breaking change in `v2.0.0` is the Go module path. Following the
[Go module versioning rules][go-modules-v2], a major version `v2` and above
must carry a `/vN` suffix in the module path:

```
github.com/tarantool/go-storage  →  github.com/tarantool/go-storage/v2
```

There are **no source-level API changes**: every package, type, function, and
method keeps the same name and signature as in `v1.6.0`. Migration is a
mechanical rewrite of import paths, so the upgrade is safe and reversible.

The minimum required Go version is unchanged (`1.25`).

### <a id="updating-import-paths">Updating import paths</a>

Add the `/v2` suffix to every `go-storage` import. For example:

Before:
```Go
import (
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/connect"
	"github.com/tarantool/go-storage/driver/etcd"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/locker"
)
```

After:
```Go
import (
	storage "github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/connect"
	"github.com/tarantool/go-storage/v2/driver/etcd"
	"github.com/tarantool/go-storage/v2/integrity"
	"github.com/tarantool/go-storage/v2/locker"
)
```

> **Note:** the root package is still named `storage`. When the import path ends
> in `/v2`, `goimports` cannot infer the package name, so add an explicit
> `storage` alias on the root import (as shown above) to keep references like
> `storage.Storage` unambiguous.

You can rewrite all import paths across a module in one pass:

```sh
# preview the affected files
grep -rl 'tarantool/go-storage' --include='*.go' .

# rewrite v1 paths to v2 (skips paths already carrying the /v2 suffix)
grep -rl 'tarantool/go-storage' --include='*.go' . | xargs sed -i '' \
	-e 's#tarantool/go-storage/v2#tarantool/go-storage#g' \
	-e 's#tarantool/go-storage#tarantool/go-storage/v2#g'
```

On GNU `sed` drop the `''` after `-i`. The double substitution first normalizes
any already-migrated paths back to the bare form, then appends `/v2` to all of
them, so the command is idempotent and safe to run more than once.

### <a id="gomod">go.mod</a>

Pull the new major version with `go get`:

```sh
go get github.com/tarantool/go-storage/v2@latest
go mod tidy
```

Your own module path does not change. Only the `require` line for `go-storage`
gains the `/v2` suffix:

```
require github.com/tarantool/go-storage/v2 v2.0.0
```

### <a id="no-api-changes">No API changes</a>

`v2.0.0` ships the exact same API surface as `v1.6.0`. Once the import paths are
updated and `go mod tidy` succeeds, no further code changes are required — your
package will compile and behave identically.

## v2 API cleanup (unreleased)

The following **breaking** changes land on the `v2` line after `v2.0.0`. They
remove dead API, make errors play well with `errors.Is`/`errors.As`, and drop
redundant name qualifiers now that the v2 cleanup left a single implementation
in several places. See the `[Unreleased]` section of [CHANGELOG.md](CHANGELOG.md)
for the full list.

* [integrity: Typed storage removed](#integrity-typed)
* [namer changes](#namer-changes)
* [marshaller renames](#marshaller-renames)
* [Removed dead options](#dead-options)
* [watch.Event field rename](#watch-event)
* [connect.NewStorage renamed](#connect-rename)
* [Error handling](#error-handling)
* [hasher/crypto: encoding modes](#encoding-modes)
* [hasher.Hasher gains a Verify method](#hasher-verify)
* [crypto.NewRSAPSSSignerVerifier renamed](#rsapss-rename)
* [On-disk encoding of hashes and signatures](#on-disk-encoding)
* [driver/etcd: locking constructor](#etcd-locker)
* [driver/tcs: DoerWatcher renamed to Client](#tcs-client)
* [locker: lock-name validation](#locker-name)
* [predicate: string values normalized to bytes](#predicate-normalize)
* [namer.KeyType.String wire form](#keytype-string)
* [crypto.RSAPSS unexported](#rsapss-unexport)
* [Other source-level changes](#other-cleanup)

### <a id="integrity-typed">integrity: Typed storage removed</a>

`integrity.Typed[T]` and `integrity.NewTypedBuilder` are removed. Use the
schema-first `integrity.Codec[T]` API, which covers the same
Get/Put/Delete/Range/Watch surface plus multi-key transactions.

Before:
```Go
typed := integrity.NewTypedBuilder[MyConfig](baseStorage).
	WithPrefix("/config").
	WithHasher(hasher.NewSHA256Hasher()).
	Build()

err := typed.Put(ctx, "app/settings", cfg)
res, err := typed.Get(ctx, "app/settings")
```

After:
```Go
codec, err := integrity.NewCodecBuilder[MyConfig]().
	WithObjectLocation("config").
	WithHasher(hasher.NewSHA256Hasher()).
	Build()
if err != nil {
	// ...
}
store := codec.Bind(baseStorage)

err = store.Put(ctx, "app/settings", cfg)
res, err := store.Get(ctx, "app/settings")
```

The shared options (`WithPutPredicates`, `WithDeletePredicates`, `WithPrefix`,
`IgnoreVerificationError`, `IgnoreMoreThanOneResult`) and the error sentinels
are unchanged. Note the on-disk key layout differs: the `Codec` API uses the
layered layout (`/<category>/<location>/<objectLocation>/<name>`) — see the
namer section below.

### <a id="namer-changes">namer changes</a>

* `namer.DefaultNamer` / `namer.NewDefaultNamer` are removed. Use `namer.New`
  (the default for `integrity.Codec`), which emits the layered
  `/<category>/<location>/<objectLocation>/<name>` layout.
* The `Layered` qualifier is dropped now that it is the only namer:

  | Before | After |
  |---|---|
  | `namer.NewLayeredNamer` | `namer.New` |
  | `namer.LayeredHashLocation` | `namer.HashLocation` |
  | `namer.LayeredSigLocation` | `namer.SigLocation` |
  | `namer.LayeredOption` | `namer.Option` |

  The `namer.Namer` interface and the option constructors (`CompactSingleHash`,
  `CompactSingleSig`, `LegacyHashSigLayout`, `WithKeyPrefix`) keep their names.
* The single-implementation `Key` interface is now a concrete struct:
  `namer.DefaultKey` → `namer.Key`, `NewDefaultKey` → `namer.NewKey`.
  `Namer.ParseKey` now returns `Key` (was `DefaultKey`).

### <a id="marshaller-renames">marshaller renames</a>

The redundant `Typed` qualifier is dropped (the generic parameter already
conveys "typed"); the unused untyped `Marshaller`/`Marshallable` interfaces are
removed.

| Before | After |
|---|---|
| `marshaller.TypedMarshaller[T]` | `marshaller.Marshaller[T]` |
| `marshaller.NewTypedYamlMarshaller` | `marshaller.NewYamlMarshaller` |
| `marshaller.TypedJSONMarshaller[T]` | `marshaller.JSONMarshaller[T]` |
| `marshaller.TypedBytesMarshaller` | `marshaller.BytesMarshaller` |

### <a id="dead-options">Removed dead options</a>

Several option types were no-ops or uninstantiable and are removed:

* `storage.Option`, `storage.WithTimeout()`, `storage.WithRetry()` — and
  `NewStorage`'s variadic. Use `storage.NewStorage(driver)`.
* `storage.WithLimit` — `Range` never honored a limit. Filter with
  `storage.WithPrefix` only.
* `watch.Option` and the `opts ...watch.Option` parameter on `Storage.Watch` /
  `Driver.Watch`. Prefix watches are still selected by ending the key with `/`.
* `operation.Option` (an empty struct) and `Operation.Options()`. Construct
  operations with `operation.Get(key)` / `Put(key, value)` / `Delete(key)`.

### <a id="watch-event">watch.Event field rename</a>

`watch.Event.Prefix` is renamed to `watch.Event.Key` — the field carries the
key (or key prefix) that changed.

```Go
// Before:               // After:
ev := <-ch               ev := <-ch
_ = ev.Prefix            _ = ev.Key
```

### <a id="connect-rename">connect.NewStorage renamed</a>

`connect.NewStorage` (the probe-etcd-then-TCS helper) is renamed to
`connect.Connect` so it no longer collides with the unrelated
`storage.NewStorage(driver)` constructor. `NewEtcdStorage` / `NewTCSStorage`
are unchanged.

### <a id="error-handling">Error handling</a>

* `integrity.ValidationError` now implements `Unwrap()` (was the non-standard
  `Unpack()`), so the wrapped cause is reachable via `errors.Is`/`errors.As`.
* `integrity.ErrInvalidName` is now a plain `errors.New` sentinel (the
  `integrity.InvalidNameError` type is removed). Match it with
  `errors.Is(err, integrity.ErrInvalidName)` as before.

### <a id="encoding-modes">hasher/crypto: encoding modes</a>

`hasher.Hasher` and the `crypto` RSA-PSS signer/verifier gained variadic
options and an explicit encoding mode, selected with `WithMode()`:

* `ModeAuto` (default) — produces **raw** bytes and, on verification, accepts a
  stored digest/signature in **either raw or lower-case hex** form.
* `ModeHex` — produces and accepts **only** lower-case hex.
* `ModeBin` — produces and accepts **only** raw bytes.

The default (`ModeAuto`) keeps the previous raw output, so writes are
unchanged; the only new behaviour is that verification now also tolerates a hex
encoding, which lets you migrate stored data to hex incrementally.

```Go
// Default: raw output, reads both raw and hex.
h := hasher.NewSHA256Hasher()
digest, _ := h.Hash(data) // 32 raw bytes, as before

// Force hex everywhere.
hexHasher := hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeHex))
hexDigest, _ := hexHasher.Hash(data) // 64-char hex string

// Force raw everywhere.
binHasher := hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeBin))

sv := crypto.NewRSAPSS(priv, crypto.WithMode(crypto.ModeHex)) // hex signatures
v := crypto.NewRSAPSSVerifier(pub)                            // auto: accepts both
```

`Name()` is unchanged for both, so the on-disk key layout is preserved — only
the stored payload encoding can change (see below). The digest fed internally to
RSA-PSS is always raw, regardless of the mode.

### <a id="hasher-verify">hasher.Hasher gains a Verify method</a>

The `Hasher` interface now requires:

```Go
Verify(data, stored []byte) error
```

It reports whether `stored` is an accepted encoding of the digest of `data`
under the hasher's mode (`nil` on match). The built-in SHA-1/SHA-256 hashers
implement it; **custom `Hasher` implementations must add it**. The integrity
validator now calls `Verify` instead of byte-comparing `Hash` output, so hash
verification respects the mode (and `ModeAuto` accepts both encodings).

### <a id="rsapss-rename">crypto.NewRSAPSSSignerVerifier renamed</a>

`crypto.NewRSAPSSSignerVerifier` is renamed to `crypto.NewRSAPSS`. Both it and
`crypto.NewRSAPSSVerifier` now take variadic options.

```Go
// Before:                                  // After:
sv := crypto.NewRSAPSSSignerVerifier(priv)  sv := crypto.NewRSAPSS(priv)
```

### <a id="on-disk-encoding">On-disk encoding of hashes and signatures</a>

With the default `ModeAuto`, an `integrity.Codec` built with the default hasher
and signer/verifier still stores **raw** hashes and signatures, exactly as
before — so data round-trips with older versions. Because `ModeAuto` also reads
hex, you can move a store to hex incrementally: switch writers to `ModeHex`
while readers stay on `ModeAuto`, then tighten readers to `ModeHex` once all
data is migrated.

```Go
// Write hex, while existing readers on ModeAuto keep accepting old raw data.
codec, _ := integrity.NewCodecBuilder[MyConfig]().
	WithHasher(hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeHex))).
	WithSignerVerifier(crypto.NewRSAPSS(priv, crypto.WithMode(crypto.ModeHex))).
	Build()
```

### <a id="etcd-locker">driver/etcd: locking constructor</a>

`driver/etcd.New` now builds a driver **without** locking support: its
`NewLocker` returns `locker.ErrUnsupported`. To get a locking-capable driver,
build it with the new `NewWithLocker(*etcd.Client)`.

```Go
// Before — locking worked implicitly when client was a concrete *etcd.Client:
drv := etcd.New(client)

// After — choose locking explicitly:
drv := etcd.NewWithLocker(client) // KV/watch/tx + locking
// or
drv := etcd.New(client)           // KV/watch/tx only; NewLocker → ErrUnsupported
```

`connect.NewEtcdStorage` / `connect.Connect` use `NewWithLocker`, so storage
obtained through `connect` keeps locking with no change on your side.

### <a id="tcs-client">driver/tcs: DoerWatcher renamed to Client</a>

The `driver/tcs` connection interface `DoerWatcher` is renamed to `Client`
(matching `driver/etcd.Client`). The interface contents are unchanged. The
generated mock is now `mocks.TCSClientMock` (was `mocks.DoerWatcherMock`).

```Go
// Before:                        // After:
var c tcs.DoerWatcher             var c tcs.Client
m := mocks.NewDoerWatcherMock(t)  m := mocks.NewTCSClientMock(t)
```

### <a id="locker-name">locker: lock-name validation</a>

Every driver's `NewLocker` now requires `name` to **start with `/`** and **not
end with `/`** (previously only tcs enforced this; etcd and dummy accepted any
string). Prefix bare names with `/`:

```Go
// Before:                          // After:
lk, err := stg.NewLocker(ctx, "leader")  lk, err := stg.NewLocker(ctx, "/leader")
```

Violations return `locker.ErrNameNoLeadingSlash` / `locker.ErrNameTrailingSlash`.
The shared `locker.ValidateName(name)` helper is exported if you want to
pre-check.

### <a id="predicate-normalize">predicate: string values normalized to bytes</a>

`predicate.ValueEqual` / `ValueNotEqual` now convert a `string` comparison
value to `[]byte`, so `Predicate.Value()` returns `[]byte` for both `[]byte`
and `string` inputs. This removes a driver-dependent difference (a `string`
value used to work on dummy/tcs but was rejected by etcd). If you inspected
`Value()` and type-switched on `string`, expect `[]byte` now.

### <a id="keytype-string">namer.KeyType.String wire form</a>

`namer.KeyType.String()` returns the wire form instead of the Go identifier:

| KeyType | Before | After |
|---|---|---|
| `KeyTypeValue` | `"KeyTypeValue"` | `"value"` |
| `KeyTypeHash` | `"KeyTypeHash"` | `"hash"` |
| `KeyTypeSignature` | `"KeyTypeSignature"` | `"signature"` |
| unknown | `"KeyType[N]"` | `"KeyType(N)"` |

The method is display-only (key construction is unaffected). Update any logs or
assertions that relied on the old strings.

### <a id="rsapss-unexport">crypto.RSAPSS unexported</a>

The concrete `crypto.RSAPSS` type is unexported. The constructors already
return interfaces, so hold the interface type instead of the concrete struct:

```Go
// Before:                                // After:
var sv crypto.RSAPSS = ...                var sv crypto.SignerVerifier = crypto.NewRSAPSS(priv)
```

`NewRSAPSS` / `NewRSAPSSVerifier` and the `crypto.AlgoRSAPSS` name constant are
unchanged.

### <a id="other-cleanup">Other source-level changes</a>

These rarely require action:

* **Error message prefixes.** Leaf error sentinels in `connect`, `crypto`,
  `hasher`, `driver/tcs` and `integrity` now carry a package prefix (e.g.
  `integrity: not found`, `hasher: hash mismatch`). Sentinel **identity** is
  unchanged, so `errors.Is` keeps working; only match on `Error()` text needs
  updating.
* **Driver receivers.** `driver/etcd.Driver` and `driver/tcs.Driver` use
  pointer receivers (the constructors already returned `*Driver`).
* **`namer.Results` receivers.** Its accessors switched to value receivers;
  existing call sites keep compiling.
* **`integrity.ModRevisionEmpty` removed.** It was an internal `0` sentinel;
  compare `ModRevision` against `0` directly.
* **New exported names.** `integrity.GetOption`/`PutOption`/`DeleteOption`
  option aliases and the `hasher.AlgoSHA256`/`AlgoSHA1`/`crypto.AlgoRSAPSS`
  algorithm-name constants are additive.

[go-modules-v2]: https://go.dev/ref/mod#major-version-suffixes
