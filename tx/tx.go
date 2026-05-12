// Package tx provides transactional interfaces for atomic storage operations.
// It supports conditional execution with predicates for complex transaction logic.
package tx

import (
	"context"

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
	Commit() (Response, error)
}

// Factory creates new transactions bound to a storage instance.
//
// It is the lightest "begin a transaction" surface — bind once via
// Storage.TxFactory and pass the resulting Factory around to components that
// only need to start transactions, instead of handing out the full Storage.
// A Storage's Tx method satisfies this signature directly, so a Factory can
// also be obtained as a method value: var f tx.Factory = s.Tx.
type Factory func(ctx context.Context) Tx
