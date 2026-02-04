package integrity

import (
	"github.com/tarantool/go-option"
)

// ValidatedResult represents a validated named value.
type ValidatedResult[T any] struct {
	// Name is the object identifier under which the value was stored.
	Name string
	// Value contains the unmarshalled value if decoding succeeded.
	Value option.Generic[T]
	// ModRevision is the storage revision when this value was last modified.
	ModRevision int64
	// Error contains validation errors if integrity verification failed.
	Error error
}
