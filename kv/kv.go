// Package kv provides key-value data structures and interfaces for storage operations.
// It defines the core KeyValue type used throughout the storage system.
package kv

// KeyValue represents a key-value pair with revision metadata.
// This structure is used to store and retrieve data from the key-value storage.
type KeyValue struct {
	// Key is the serialized representation of the key.
	Key []byte
	// Value is the serialized representation of the value.
	Value []byte

	// ModRevision is the revision number of the last modification to this key.
	ModRevision int64
}
