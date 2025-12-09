package marshaller

import (
	"gopkg.in/yaml.v3"
)

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
		return []byte{}, errMarshal(err)
	}

	return marshalled, nil
}

// Unmarshal deserializes YAML data into a typed object.
func (m TypedYamlMarshaller[T]) Unmarshal(data []byte) (T, error) {
	var out T

	err := yaml.Unmarshal(data, &out)
	if err != nil {
		return zero[T](), errUnmarshal(err)
	}

	return out, nil
}
