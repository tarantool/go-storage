# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- integrity.Validator: Ability to get ModRevision from a validated result (#23).
- integrity: Integration tests have been implemented (#30).
- integrity.Typed: The possibility of prefix deletion (#24).
- integrity.Typed: Ability to use predicates (#25).
- driver: Added dummy driver implementation.

### Changed

### Fixed

- namer.Namer: Fixed a bug where a double slash was placed at the end of a prefix (#41).
- integrity.Typed: Fixed an ability to use Range with verification
  using non-empty names (#26).

## [v1.0.0] - 2025-12-22

The release introduces the initial version of the library.

### Added

- storage.Storage: middle-level interface with Watch, Tx, and Range operations.
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
- integrity.Typed: high-level interface for integrity-protected storage
  operations with Get, Put, Delete, Range, and Watch methods.

### Changed

### Fixed

[v1.0.0]: https://github.com/tarantool/go-storage/releases/tag/v1.0.0
