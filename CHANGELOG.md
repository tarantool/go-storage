# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `storage.Prefixed` now rejects a non-empty prefix that does not start with
  `/`, returning the new `storage.ErrPrefixNoLeadingSlash`. Interior `/`
  separators remain allowed; a trailing `/` is still rejected with
  `storage.ErrPrefixTrailingSlash`.
- namer.LayeredNamer: `LegacyHashSigLayout()` option emits hash and signature
  keys without the per-codec `objectLocation` segment (value keys keep it) to
  match the layout produced by the legacy product:

      /<objectLocation>/<name>
      /hashes/<hashLocation>/<name>
      /sig/<sigLocation>/<name>

  Composes with `CompactSingleHash` / `CompactSingleSig` and is a no-op in
  unnamed mode.

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

- connect: fix TCS connection failure when endpoint URL contains `http://` or
  `https://` scheme. Endpoints are now normalized by stripping the scheme
  prefix; etcd endpoints use `http://` or `https://` based on `SSL.Enable`.

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

[v1.3.0]: https://github.com/tarantool/go-storage/releases/tag/v1.3.0
[v1.2.0]: https://github.com/tarantool/go-storage/releases/tag/v1.2.0
[v1.1.2]: https://github.com/tarantool/go-storage/releases/tag/v1.1.2
[v1.1.1]: https://github.com/tarantool/go-storage/releases/tag/v1.1.1
[v1.1.0]: https://github.com/tarantool/go-storage/releases/tag/v1.1.0
[v1.0.0]: https://github.com/tarantool/go-storage/releases/tag/v1.0.0
