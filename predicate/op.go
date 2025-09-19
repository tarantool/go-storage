package predicate

// Op represents the comparison operation type for predicates.
type Op int

const (
	// OpEqual represents equality comparison.
	OpEqual Op = iota
	// OpNotEqual represents inequality comparison.
	OpNotEqual
	// OpGreater represents greater than comparison.
	OpGreater
	// OpLess represents less than comparison.
	OpLess
)

func (op Op) String() string {
	switch op {
	case OpEqual:
		return "Equal"
	case OpNotEqual:
		return "NotEqual"
	case OpGreater:
		return "Greater"
	case OpLess:
		return "Less"
	default:
		return "Unknown"
	}
}
