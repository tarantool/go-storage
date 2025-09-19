// Package predicate provides types and interfaces for conditional operations.
// It defines predicate logic used in transactional conditional execution.
package predicate

// Op represents the comparison operation type for predicates.
type Op int

const (
	// PredicateOpEqual represents equality comparison.
	PredicateOpEqual Op = iota
	// PredicateOpNotEqual represents inequality comparison.
	PredicateOpNotEqual
	// PredicateOpGreater represents greater than comparison.
	PredicateOpGreater
	// PredicateOpLess represents less than comparison.
	PredicateOpLess
)

// Target represents what aspect of a key to compare in predicates.
type Target int

const (
	// PredicateTargetVersion compares the version of the key.
	PredicateTargetVersion Target = iota
	// PredicateTargetValue compares the value of the key.
	PredicateTargetValue
)

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
	Value() interface{}
}
