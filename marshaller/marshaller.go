// Package marshaller represent interface to transformation.
package marshaller

import (
	"errors"

	"gopkg.in/yaml.v3"
)

// ErrMarshall is returned if Marshalling failed.
var ErrMarshall = errors.New("failed to marshal")

// ErrUnmarshall is returned if Unmarshalling failed.
var ErrUnmarshall = errors.New("failed to unmarshal")

// DefaultMarshaller - serialization by default (JSON/Protobuf/etc),
// implements one time for all objects.
// Required for `integrity.Storage` to set marshalling format for any type object
// and as recommendation for developers of `Storage` wrappers.
type DefaultMarshaller interface { //nolint:iface
	Marshal(data any) ([]byte, error)
	Unmarshal(data []byte, out any) error
}

// Marshallable - custom object serialization, implements for each object.
// Required for `integrity.Storage` type to set marshalling format to specific object
// and as recommendation for developers of `Storage` wrappers.
type Marshallable interface { //nolint:iface
	Marshal(data any) ([]byte, error)
	Unmarshal(data []byte, out any) error
}

// YAMLMarshaller struct represent realization.
type YAMLMarshaller struct{}

// NewYamlMarshaller creates new NewYamlMarshaller object.
func NewYamlMarshaller() Marshallable {
	return YAMLMarshaller{}
}

// Marshal implements interface.
func (m YAMLMarshaller) Marshal(data any) ([]byte, error) {
	marshalled, err := yaml.Marshal(data)
	if err != nil {
		return []byte{}, ErrMarshall
	}

	return marshalled, nil
}

// Unmarshal implements interface.
func (m YAMLMarshaller) Unmarshal(data []byte, out any) error {
	err := yaml.Unmarshal(data, &out)
	if err != nil {
		return ErrUnmarshall
	}

	return nil
}

// TypedYamlMarshaller is a generic YAML marshaller for typed objects.
type TypedYamlMarshaller[T any] struct{}

// NewTypedYamlMarshaller creates a new TypedYamlMarshaller for the specified type.
func NewTypedYamlMarshaller[T any]() TypedYamlMarshaller[T] {
	return TypedYamlMarshaller[T]{}
}

// Marshal serializes the typed data to YAML format.
func (m TypedYamlMarshaller[T]) Marshal(data T) ([]byte, error) {
	marshalled, err := yaml.Marshal(data)
	if err != nil {
		return []byte{}, ErrMarshall
	}

	return marshalled, nil
}

func zero[T any]() T {
	var out T
	return out
}

// Unmarshal deserializes YAML data into a typed object.
func (m TypedYamlMarshaller[T]) Unmarshal(data []byte) (T, error) {
	var out T

	err := yaml.Unmarshal(data, &out)
	if err != nil {
		return zero[T](), ErrUnmarshall
	}

	return out, nil
}
