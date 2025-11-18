package namer

import (
	"fmt"
)

// InvalidKeyError represents an error for invalid key format.
type InvalidKeyError struct {
	Key     string
	Problem string
}

func (e InvalidKeyError) Error() string {
	return fmt.Sprintf("invalid key '%s': %s", e.Key, e.Problem)
}

func errInvalidKey(key string, problem string) error {
	return InvalidKeyError{
		Key:     key,
		Problem: problem,
	}
}

// InvalidNameError represents an error for invalid name format.
type InvalidNameError struct {
	Name    string
	Problem string
}

func (e InvalidNameError) Error() string {
	return fmt.Sprintf("invalid name '%s': %s", e.Name, e.Problem)
}

func errInvalidName(name string, problem string) error {
	return InvalidNameError{
		Name:    name,
		Problem: problem,
	}
}
