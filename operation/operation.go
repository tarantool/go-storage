// Package operation provides types and interfaces for storage operations.
// It defines operation types and configurations used in transactional contexts.
package operation

// Operation represents a storage operation to be executed.
// This is used within transactions and other operation contexts.
type Operation struct {
	// Type specifies the operation type (Get, Put, Delete).
	Type Type
	// Key is the target key for the operation.
	Key []byte
	// Value contains the data for put operations, nil for get/delete.
	Value []byte
	// Options contains additional operation configuration.
	Options []Option
}
