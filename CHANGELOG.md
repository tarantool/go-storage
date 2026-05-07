# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- namer.LayeredNamer: `objectLocation` may now be a multi-segment path
  (e.g. `"settings/ldap"`). Previously the segment validator rejected any
  inner `/`. The reserved-marker check still applies to the first segment,
  so `objectLocation` values like `"hash/foo"` or `"sig/foo"` remain
  rejected because they would collide with the hash/sig key dispatch.

### Changed

- watch: unified all drivers (etcd, dummy, tcs) on a signal-only
  `Event.Prefix` contract. Every driver now emits the watched key with
  any trailing `/` stripped — a signal that something at or under the
  watched key changed, not the per-event changed key. Consumers that
  need the changed key must follow up with `Range`. tcs's per-watcher
  buffer is bumped to 16 with a blocking, ctx-aware send so server
  bursts no longer drop on a full channel.

### Fixed

- watch: prefix-shaped Watch calls (e.g. `Watch(ctx, "acl/")`) used to
  silently drop every event because drivers emitted the watched key
  with the trailing `/` intact and the integrity layer's `ParseKey`
  rejected it. Drivers now strip the trailing `/`; the integrity store
  filter strips it from the user-supplied name as well so the
  neighbour-codec filter matches both single-key and prefix watches.

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
