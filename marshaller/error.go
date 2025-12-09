package marshaller

import (
	"fmt"
)

// MarshalError represents an error when marshalling fails.
type MarshalError struct {
	parent error
}

func errMarshal(parent error) error {
	if parent == nil {
		return nil
	}

	return MarshalError{parent: parent}
}

// Unwrap returns the underlying error that caused the marshalling failure.
func (e MarshalError) Unwrap() error {
	return e.parent
}

// Error returns a string representation of the marshalling error.
func (e MarshalError) Error() string {
	return fmt.Sprintf("Failed to marshal: %s", e.parent)
}

// UnmarshalError represents an error when unmarshalling fails.
type UnmarshalError struct {
	parent error
}

func errUnmarshal(parent error) error {
	if parent == nil {
		return nil
	}

	return UnmarshalError{parent: parent}
}

// Unwrap returns the underlying error that caused the unmarshalling failure.
func (e UnmarshalError) Unwrap() error {
	return e.parent
}

// Error returns a string representation of the unmarshalling error.
func (e UnmarshalError) Error() string {
	return fmt.Sprintf("Failed to unmarshal: %s", e.parent)
}
