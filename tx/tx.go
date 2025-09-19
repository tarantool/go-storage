// Package tx provides transactional interfaces for atomic storage operations.
// It supports conditional execution with predicates for complex transaction logic.
package tx

import (
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
)

// Tx represents a transactional interface for atomic operations.
// Transactions support conditional execution with predicates.
type Tx interface {
	// If specifies predicates for conditional transaction execution.
	// Empty predicate list means always true (unconditional execution).
	If(predicates ...predicate.Predicate) Tx
	// Then specifies operations to execute if predicates evaluate to true.
	// At least one Then call is required.
	Then(operations ...operation.Operation) Tx
	// Else specifies operations to execute if predicates evaluate to false.
	// This is optional.
	Else(operations ...operation.Operation) Tx
	// Commit atomically executes the transaction and returns the result.
	Commit() (TxnResponse, error)
}

// TxnResponse contains the result of a transaction execution.
type TxnResponse struct {
	// Succeeded indicates whether the transaction predicates evaluated to true.
	Succeeded bool
	// Results contains the responses for each operation in Then/Else blocks.
	Results []TxnRequestResponse
}

// TxnRequestResponse represents the response for an individual transaction operation.
type TxnRequestResponse struct {
}
