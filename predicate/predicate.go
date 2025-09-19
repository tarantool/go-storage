// Package predicate provides types and interfaces for conditional operations.
// It defines predicate logic used in transactional conditional execution.
package predicate

// Predicate represents a condition used for conditional operations.
// Predicates are used in transactions to specify conditions for execution.
type Predicate interface {
	// Key returns the key that this predicate applies to.
	Key() []byte
	// Operation returns the comparison operation (Equal, NotEqual, Greater, Less).
	Operation() Op
	// Target returns what aspect of the key to compare (Version, Value).
	Target() Target
	// Value returns the comparison value for the predicate.
	Value() any
}
