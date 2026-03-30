# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

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

[v1.1.2]: https://github.com/tarantool/go-storage/releases/tag/v1.1.2
[v1.1.1]: https://github.com/tarantool/go-storage/releases/tag/v1.1.1
[v1.1.0]: https://github.com/tarantool/go-storage/releases/tag/v1.1.0
[v1.0.0]: https://github.com/tarantool/go-storage/releases/tag/v1.0.0
