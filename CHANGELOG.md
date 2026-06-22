# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

## [v1.6.1] - 2026-06-22

This release adds hex-encoding decorators for hashers and signer/verifiers.
New constructors wrap an existing `Hasher` or `SignerVerifier` so the stored
payload is lower-case hex while `Name()` is passed through unchanged,
preserving the on-disk key layout.

### Added

- hasher, crypto: hex-encoding decorators that wrap an existing `Hasher` or
  `SignerVerifier` so the stored payload is lower-case hex while `Name()` is
  passed through unchanged, preserving the on-disk key layout. New
  constructors: `hasher.NewHexHasher`, `hasher.NewHexSHA256Hasher`,
  `hasher.NewHexSHA1Hasher`, `crypto.NewHexSignerVerifier`,
  `crypto.NewHexVerifier`, `crypto.NewHexRSAPSSSignerVerifier`, and
  `crypto.NewHexRSAPSSVerifier`. The raw constructors are unchanged (#114).

## [v1.6.0] - 2026-06-09

This release introduces a new `locker` package providing distributed locking
across all drivers: `Driver` and `Storage` gain a `NewLocker` method (with
etcd, TCS, and in-memory dummy implementations), plus `locker.Factory`,
`Prefixed`, `Do`, and `Done` helpers. It also adds key-prefix support to
`namer.LayeredNamer` and `integrity.CodecBuilder` (`WithKeyPrefix`) so
multiple codecs can share one storage in disjoint sub-trees for atomic
commits, along with new `ValueKey`/`FullKeys` accessors on integrity codecs
and stores. Includes fixes for name validation, empty-value hashing, and
etcd connections for non-admin users.

### Added

- namer.LayeredNamer: `WithKeyPrefix(prefix)` option prepends a fixed path
  prefix to every emitted/parsed key. Lets two codecs sit in disjoint
  sub-trees of a single `storage.Storage` so they can be committed atomically
  in one `integrity.Tx` (which cannot span multiple storage handles, the way
  `storage.Prefixed` wrappers can). Validation matches `storage.Prefixed`:
  must start with `/`, must not end with `/`; an empty prefix is a no-op.
  Returns `namer.ErrKeyPrefixNoLeadingSlash` /
  `namer.ErrKeyPrefixTrailingSlash` on malformed input; `ParseKey` returns
  `namer.ErrKeyPrefixMissing` for keys that do not start with the configured
  prefix (#101).
- integrity.CodecBuilder: `WithKeyPrefix(prefix)` threads the namer option
  through to the underlying `namer.NewLayeredNamer`, so codecs can be built
  directly with a prefix without writing a custom `CodecNamerConstructor`
  (#101).
- `locker.Do(ctx, f, name, fn, opts...)`: helper that creates a Locker via
  the supplied `Factory`, acquires it, runs `fn` while the lock is held, and
  releases the lock on return. `fn`'s error leads any joined Unlock error so
  `errors.Is` against domain errors works without peeling off lock-machinery
  wrappers; Unlock always runs after `fn` regardless of `fn`'s result (#104).
- `locker.Prefixed(prefix, inner)`: Factory-level name-scoping helper that
  concatenates `prefix` to every caller-supplied lock name before delegating
  to `inner`, mirroring `storage.Prefixed`'s rewrite convention. Lets a
  component receive a name-scoped `locker.Factory` without seeing the full
  `Storage`; an empty prefix is a transparent passthrough, and a non-empty
  prefix must start with `/` (`ErrPrefixNoLeadingSlash`) and not end with `/`
  (`ErrPrefixTrailingSlash`) (#104).
- integrity.Codec: `ValueKey(name)` returns the namer-relative value-layer
  key; `FullKeys(name)` returns every key the codec's namer would emit
  (value + hashes + signatures), in namer-emitted order. Both reject empty,
  leading-slash, and trailing-slash names with `ErrInvalidName`, matching the
  rule applied by `Put`/`Delete`/`Get`/`Watch` (#102).
- integrity.Store: `ValueKey(name)` and `FullKeys(name)` mirror the codec
  methods but return on-disk keys — when the storage is wrapped with
  `storage.Prefixed`, the wrapper's prefix is prepended (#102).
- integrity.SingletonStore: no-arg `ValueKey()` and `FullKeys()` delegate to
  the inner `Store` with the bound name (#102).
- storage.Prefixer: optional interface implemented by `Prefixed` storages to
  expose their key prefix. `storage.StoragePrefix(s)` returns the prefix or
  `nil` for storages that are not wrapped (#102).
- test_helpers/etcd: new public helper package built on
  `go.etcd.io/etcd/server/v3/embed`, replacing the old `internal/testing/etcd`
  helper that pulled in conflicting `genproto` and `grpc-middleware` versions
  and broke module-mode builds. Exposes `New`, `Cluster`, `ClusterConfig`,
  `EndpointsGRPC`, `EndpointsHTTP`, `Terminate`, and a `LazyCluster` for
  sharing one embedded cluster across a test suite (#105).

### Changed

- `driver.Driver` and `storage.Storage` interfaces gained a
  `NewLocker(ctx, name, opts...) (locker.Locker, error)` method backed by a
  new `locker` package. Minor breaking change for out-of-tree implementers of
  either interface. The dummy driver ships an in-memory implementation; the
  etcd driver wires `concurrency.Mutex` and supports `NewLocker` only when the
  `Client` passed to `etcd.New` is a concrete `*etcd.Client` (the
  `concurrency` package needs the concrete type which the `Client` interface
  does not expose); otherwise `NewLocker` returns `locker.ErrUnsupported`. The
  TCS driver layers a "smallest mod_revision wins" protocol over
  `config.storage.put`/`keepalive`/`get`/`delete` and requires a TCS schema
  with both `features.ttl` and `features.keepalive`. The `Locker` interface
  also gained a `Done() <-chan struct{}` method that closes when the lock is
  no longer held — either via `Unlock` or because the backend session was lost
  (TTL elapsed without renewal, connection dropped, etc.); calling `Done`
  before any successful acquire returns an already-closed channel. Minor
  breaking change for out-of-tree `Locker` implementers (same flavor as the
  original `NewLocker` addition) (#103, #98, #99, #100, #104).
- `locker.Factory` and `Storage.LockerFactory()`: a lightweight "create a
  lock" surface for components that do not need the full `Storage` interface.
  `Prefixed` implements it so factory-issued lockers keep the prefix-rewrite
  path that scopes lock names under the wrapper's namespace. **Breaking
  change:** `locker.Factory` is now an interface with a
  `NewLocker(ctx, name, opts...) (Locker, error)` method instead of a function
  type. `Storage` and the `Prefixed` wrapper already satisfy it via their
  `NewLocker` method; adapt a bare function with the new `locker.FactoryFunc`
  (mirrors `http.HandlerFunc`). `locker.Prefixed` composition is now flattened
  at construction and is outer-first —
  `locker.Prefixed("/a", locker.Prefixed("/b", inner))` is equivalent to
  `locker.Prefixed("/a/b", inner)`, matching `storage.Prefixed` (#104).

### Fixed

- integrity.Codec.BindPredicate now rejects empty, leading-slash, and
  trailing-slash names with `ErrInvalidName`, matching `Put`/`Delete`/
  `Get`/`Watch`. Without it, a leading slash was silently stripped by the
  namer and aliased `"/foo"` to `"foo"`, letting a predicate match a row no
  sibling call could see (#102).
- hasher: nil and empty input now hash to the empty-string digest instead of
  failing with "data is nil". Storage backends round-trip empty stored values
  as nil, so reading a legitimately-empty value previously exploded in the
  SHA-256 hasher and the RSA-PSS verifier, forcing `IgnoreVerificationError()`
  as the only workaround. The internal-only `ErrDataIsNil` is removed (#107).
- connect.NewEtcdStorage failed if user has no admin permission (#106).

## [v1.5.0] - 2026-05-15

This release renames the namer's hash key path marker from `hash` to
`hashes`, adds `tx.Factory` for handing transaction-begin capability
to components without exposing the full `Storage` interface,
introduces a `LegacyHashSigLayout` namer option for compatibility with
the legacy product layout, tightens `storage.Prefixed` to require a
leading `/`, and makes the `connect` package accept scheme-prefixed
endpoint URLs and probe every configured endpoint before failing.

### Added

- tx.Factory and `Storage.TxFactory()`: a lightweight "begin a
  transaction" surface for components that do not need the full
  `Storage` interface. `Prefixed` implements it so factory-issued
  transactions keep the prefix-rewrite path.
- `storage.Prefixed` now rejects a non-empty prefix that does not start
  with `/`, returning the new `storage.ErrPrefixNoLeadingSlash`. Interior
  `/` separators remain allowed; a trailing `/` is still rejected with
  `storage.ErrPrefixTrailingSlash`.
- namer.LayeredNamer: `LegacyHashSigLayout()` option emits hash and
  signature keys without the per-codec `objectLocation` segment (value
  keys keep it) to match the layout produced by the legacy product:

      /<objectLocation>/<name>
      /hashes/<hashLocation>/<name>
      /sig/<sigLocation>/<name>

  Composes with `CompactSingleHash` / `CompactSingleSig` and is a no-op
  in unnamed mode.

### Changed

- **BREAKING:** namer: hash key path marker changed from `hash` to
  `hashes`. The default layout is now
  `/<objectLocation>/hashes/<hashLocation>/<name>` (default namer) and
  `/hashes/<hashLocation>/<objectLocation>/<name>` (layered namer); the
  unnamed-layered and compact-hash variants follow the same rename.
  Existing keys written under `/hash/...` will not be parsed by the new
  namer — operators upgrading must migrate stored keys to the new prefix.
  The reserved-marker check on `objectLocation` and the unnamed-mode
  reserved-first-segment check now reject `hashes` instead of `hash`.

### Fixed

- connect: TCS connection failure when an endpoint URL contained
  `http://` or `https://` scheme. Endpoints are now normalized by
  stripping the scheme prefix; etcd endpoints use `http://` or
  `https://` based on `SSL.Enable`.
- connect: a configured endpoint list is now probed in order — the
  first reachable endpoint wins and the client/pool is returned.
  Previously only `Endpoints[0]` was probed, so the connection failed
  if the first endpoint was down even when later ones were healthy.
  When every endpoint fails, a single joined error is returned.

## [v1.4.0] - 2026-05-12

This release adds an unnamed codec layout and a `SingletonStore[T]` for
single-key configuration objects, switches `watch` to a signal-only
`Event.Prefix` contract, makes `storage.Prefixed` return an error, and
fixes several cases where `Range` and `Watch` dropped every result under
integrity.

### Added

- integrity.SingletonStore[T] and `Codec[T].BindSingleton`: bind a codec
  to one fixed key at construction time, then call
  `Get`/`Put`/`Delete`/`Watch` (and the `Tx*` variants) without passing a
  name on every call. Suited for configuration objects that live at a
  single known key (e.g. `/settings/auth`) rather than under a directory
  of `<objectLocation>/<id>` items; the on-disk layout matches `Store[T]`
  for the same name.
- integrity, namer: unnamed codec layout. A `CodecBuilder` without
  `WithObjectLocation`, or `NewLayeredNamer` with the new
  `ObjectLocationMissing` sentinel, emits keys as `/<name>`,
  `/hash/<hashLocation>/<name>`, `/sig/<sigLocation>/<name>` — the
  per-codec location segment is dropped. Object names whose first
  slash-separated segment is `hash` or `sig` are rejected to avoid
  colliding with the category markers.
- namer.LayeredNamer: `objectLocation` may now be a multi-segment path
  (e.g. `"settings/ldap"`); the reserved `hash`/`sig` marker check
  applies only to the first segment.
- namer.Namer: new `Prefixes(val, isPrefix) []string` method returning
  one range prefix per key category (value, hash, sig). `DefaultNamer`
  and `LayeredNamer` implement it; third-party `Namer` implementations
  must add the method.

### Changed

- integrity.CodecBuilder: omitting `WithObjectLocation` no longer falls
  back to `"objects"`; it now produces an unnamed codec (keys at
  `/<name>` instead of `/objects/<name>`). Callers who relied on the
  silent default must add `.WithObjectLocation("objects")` explicitly.
  This is a breaking change.
- storage.Prefixed: signature is now
  `Prefixed(prefix, inner) (Storage, error)`. A non-empty prefix ending
  with `/` is rejected with `ErrPrefixTrailingSlash`; an empty prefix
  still yields a transparent wrapper. This is a breaking change — callers
  must handle the returned error.
- watch: all drivers (etcd, dummy, tcs) now follow a signal-only
  `Event.Prefix` contract. Every driver emits the watched key with any
  trailing `/` stripped — a signal that something at or under the watched
  key changed, not the per-event changed key. Consumers that need the
  changed key must follow up with `Range`. The tcs per-watcher buffer is
  bumped to 16 with a blocking, ctx-aware send so server bursts no longer
  drop on a full channel.

### Fixed

- integrity: `Store[T].Range(ctx, "")` and `Typed[T].Range(ctx, "")`
  returned an empty slice as soon as a hasher or signer/verifier was
  configured, because the empty-name branch fetched only the value-layer
  prefix and the validator then dropped every result as missing its
  hash/signature. Both methods now fan out across every category prefix.
- integrity: prefix-shaped `Watch` calls (e.g. `Watch(ctx, "acl/")`) and
  whole-codec watches (`Watch(ctx, "")`) silently dropped every event.
  Trailing slashes are now normalised on both the driver and integrity
  sides, and whole-codec watches forward driver events as-is.
- connect: `NewEtcdStorage` could spin in an infinite loop with bad
  endpoints; `NewTCSStorage` now validates credentials.

## [v1.3.0] - 2026-05-05

This release introduces a new transaction-aware integrity API
(`Codec[T]`, `Store[T]`, and `Tx`), a `Prefixed` storage wrapper for
namespace scoping, a `LayeredNamer` with per-category key layout, and
additional typed marshallers. It also bumps dependencies to address
vulnerabilities reported by govulncheck.

### Added

- storage.Prefixed: wrapper that scopes every storage operation,
  predicate, Range, and Watch call under a given namespace prefix.
  Nested wrappers are flattened automatically (#67).
- namer.LayeredNamer: namespace-agnostic namer that places each key
  category under its own top-level location segment
  (`/<objectLocation>/<name>`, `/hash/<hashLocation>/...`,
  `/sig/<sigLocation>/...`). Segments are validated at construction
  and `ParseKey` parses keys back to `(name, KeyType, property)`
  unambiguously (#68).
- integrity: new schema-driven API for integrity-protected storage.
  `Codec[T]` describes the value layout independently of any storage
  handle and is built via the fluent `CodecBuilder[T]`, which
  validates location-override keys eagerly so typos like
  `WithHashLocation("sah256", …)` are no longer silently ignored.
  `Store[T]` binds a codec to a storage handle and exposes
  `Get`/`Put`/`Delete`/`Range`/`Watch`. Conditional multi-key updates
  are available through `Tx` with `Branch` and typed futures;
  multi-codec transactions are lowered to a single storage call
  (#69, #70, #71).
- marshaller: `TypedJSONMarshaller[T]` for `encoding/json`-based
  marshalling and `TypedBytesMarshaller` passthrough for values
  already stored as opaque bytes (#73).

### Fixed

- Bumped `google.golang.org/grpc` to `v1.79.3` (GO-2026-4762:
  authorization bypass via missing leading slash in `:path`) and
  `go.opentelemetry.io/otel/sdk` to `v1.40.0` (GO-2026-4394: arbitrary
  code execution via PATH hijacking) (#75).

## [v1.2.0] - 2026-04-29

This release introduces a new `connect` package for building `Storage`
instances from configuration, fixes a goroutine leak in `storage.Watch`,
and improves TCS predicate compatibility with etcd's absence/presence
idioms.

### Added

- connect: Added a convenience package to create `Storage` instances from
  configuration for etcd and Tarantool Config Storage backends. TLS support
  for TCS is available via the `go_storage_ssl` build tag (requires CGO) (#55).

### Changed

- driver.etcd: etcd.New now accepts an interface that is compatible with
  `*etcdclientv3.Client` instead of the concrete type, which should not cause
  any issues when upgrading the version.

### Fixed

- storage.Watch: prevent goroutine leak when the consumer stops reading from
  the event channel before the watch is cancelled.
- driver.tcs: predicates of the form `VersionEqual(key, 0)` and
  `VersionNotEqual(key, 0)` are now transparently rewritten to TCS's
  `count == 0` / `count != 0` predicates on the wire. Previously these
  errored against TCS because `mod_revision` is undefined for absent
  keys. This brings parity with etcd's canonical absence/presence
  idioms (#61).

## [v1.1.2] - 2026-03-30

This release fixes an issue where Range returned empty results for names
with a trailing slash.

### Fixed

- integrity.Typed: Range returns empty results when name has trailing slash.

## [v1.1.1] - 2026-03-06

This release updates the TypedBuilder to use a generic marshaller interface
for more flexibility.

### Changed

- integrity.Typed: TypedBuilder uses generic TypedMarshaller interface
  instead of TypedYamlMarshaller (#51).

## [v1.1.0] - 2026-03-03

This release adds prefix deletion, predicates support for integrity.Typed,
and a dummy driver for testing purposes.

### Added

- integrity.Validator: Ability to get ModRevision from a validated result
  (#23).
- integrity: Integration tests have been implemented (#30).
- integrity.Typed: The possibility of prefix deletion (#24).
- integrity.Typed: Ability to use predicates (#25).
- driver: Added dummy driver implementation (#47).

### Fixed

- namer.Namer: Fixed a bug where a double slash was placed at the end of a
  prefix (#41).
- integrity.Typed: Fixed an ability to use Range with verification
  using non-empty names (#26).

## [v1.0.0] - 2025-12-22

The release introduces the initial version of the library.

### Added

- storage.Storage: Middle-level interface with Watch, Tx, and Range operations.
- tx.Tx: Conditional transaction execution with predicates and operations.
- operation.Operation: Get, Put, Delete operations with typed interfaces.
- predicate.Predicate: Value and version comparisons for conditional logic.
- watch.Event: Real-time change monitoring with prefix support.
- driver.tcs: Tarantool Config Storage backend implementation with transaction
  support.
- driver.etcd: Basic etcd backend implementation with conditional transactions.
- hasher: SHA1 and SHA256 hash implementations.
- integrity: Signer and verifier for data integrity checking.
- namer.Namer: Key naming and metadata management.
- integrity.Typed: High-level interface for integrity-protected storage
  operations with Get, Put, Delete, Range, and Watch methods.

### Fixed
