package operation

// Type represents the type of storage operation.
type Type int

const (
	// TypeGet represents a read operation.
	TypeGet Type = iota
	// TypePut represents a write operation.
	TypePut
	// TypeDelete represents a delete operation.
	TypeDelete
)

func (t Type) String() string {
	switch t {
	case TypeGet:
		return "Get"
	case TypePut:
		return "Put"
	case TypeDelete:
		return "Delete"
	default:
		return "Unknown"
	}
}
