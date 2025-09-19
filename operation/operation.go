// Package operation provides types and interfaces for storage operations.
// It defines operation types and configurations used in transactional contexts.
package operation

// Type represents the type of storage operation.
type Type int

const (
	// OperationTypeGet represents a read operation.
	OperationTypeGet Type = iota
	// OperationTypePut represents a write operation.
	OperationTypePut
	// OperationTypeDelete represents a delete operation.
	OperationTypeDelete
)

// Option contains configuration options for operations.
type Option struct{}

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
