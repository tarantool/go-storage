// Package operation provides types and interfaces for storage operations.
// It defines operation types and configurations used in transactional contexts.
package operation

import (
	"bytes"
)

var (
	// isPrefixSuffix is a byte sequence that indicates that key is a prefix (ends with /).
	isPrefixSuffix = []byte("/") //nolint:gochecknoglobals
)

// Operation represents a storage operation to be executed.
// This is used within transactions and other operation contexts.
type Operation struct {
	// tp specifies the operation type (Get, Put, Delete).
	tp Type
	// key is the target key for the operation.
	key []byte
	// value contains the data for put operations, nil for get/delete.
	value []byte
	// options contains additional operation configuration.
	options []Option
}

// Type returns the operation type (Get, Put, or Delete).
func (o Operation) Type() Type {
	return o.tp
}

// Key returns the key associated with the operation.
func (o Operation) Key() []byte {
	return o.key
}

// Value returns the value associated with the operation.
func (o Operation) Value() []byte {
	return o.value
}

// Options returns the configuration options for the operation.
func (o Operation) Options() []Option {
	return o.options
}

// IsPrefix returns true if the operation is a prefix operation.
func (o Operation) IsPrefix() bool {
	if o.tp == TypeGet || o.tp == TypeDelete {
		return bytes.HasSuffix(o.key, isPrefixSuffix)
	}

	return false
}

// Get creates a new read operation for the specified key.
// Returns an Operation configured for reading data from storage.
func Get(key []byte, options ...Option) Operation {
	return Operation{
		tp:      TypeGet,
		key:     key,
		value:   nil,
		options: options,
	}
}

// Put creates a new write operation for the specified key-value pair.
// Returns an Operation configured for writing data to storage.
func Put(key, value []byte, options ...Option) Operation {
	return Operation{
		tp:      TypePut,
		key:     key,
		value:   value,
		options: options,
	}
}

// Delete creates a new delete operation for the specified key.
// Returns an Operation configured for removing data from storage.
func Delete(key []byte, options ...Option) Operation {
	return Operation{
		tp:      TypeDelete,
		key:     key,
		value:   nil,
		options: options,
	}
}
