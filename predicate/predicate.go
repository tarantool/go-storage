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

// predicate is the concrete implementation of the Predicate interface.
// It represents a condition used for conditional operations in transactions.
type predicate struct {
	key    []byte
	op     Op
	target Target
	value  interface{}
}

// Key returns the key that this predicate applies to.
func (p predicate) Key() []byte {
	return p.key
}

// Operation returns the comparison operation (Equal, NotEqual, Greater, Less).
func (p predicate) Operation() Op {
	return p.op
}

// Target returns what aspect of the record to compare (e.g. Version or Value).
func (p predicate) Target() Target {
	return p.target
}

// Value returns the comparison value for the predicate.
func (p predicate) Value() interface{} {
	return p.value
}

// ValueNotEqual creates a predicate that checks if a key's value is not equal to the specified value.
func ValueNotEqual(key []byte, value interface{}) Predicate {
	return &predicate{
		key:    key,
		op:     OpNotEqual,
		target: TargetValue,
		value:  value,
	}
}

// ValueEqual creates a predicate that checks if a key's value equals the specified value.
func ValueEqual(key []byte, value interface{}) Predicate {
	return &predicate{
		key:    key,
		op:     OpEqual,
		target: TargetValue,
		value:  value,
	}
}

// VersionEqual creates a predicate that checks if a key's version equals the specified version.
func VersionEqual(key []byte, version int64) Predicate {
	return &predicate{
		key:    key,
		op:     OpEqual,
		target: TargetVersion,
		value:  version,
	}
}

// VersionNotEqual creates a predicate that checks if a key's version is not equal to the specified version.
func VersionNotEqual(key []byte, version int64) Predicate {
	return &predicate{
		key:    key,
		op:     OpNotEqual,
		target: TargetVersion,
		value:  version,
	}
}

// VersionGreater creates a predicate that checks if a key's version is greater than the specified version.
func VersionGreater(key []byte, version int64) Predicate {
	return &predicate{
		key:    key,
		op:     OpGreater,
		target: TargetVersion,
		value:  version,
	}
}

// VersionLess creates a predicate that checks if a key's version is less than the specified version.
func VersionLess(key []byte, version int64) Predicate {
	return &predicate{
		key:    key,
		op:     OpLess,
		target: TargetVersion,
		value:  version,
	}
}
