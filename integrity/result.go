package integrity

import (
	"github.com/tarantool/go-option"
)

// ValidatedResult represents a validated named value.
type ValidatedResult[T any] struct {
	Name        string
	Value       option.Generic[T]
	ModRevision int64
	Error       error
}
