package predicate

// Target represents what aspect of a key to compare in predicates.
type Target int

const (
	// TargetVersion compares the version of the key.
	TargetVersion Target = iota
	// TargetValue compares the value of the key.
	TargetValue
)

func (t Target) String() string {
	switch t {
	case TargetVersion:
		return "Version"
	case TargetValue:
		return "Value"
	default:
		return "Unknown"
	}
}
