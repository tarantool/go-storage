package marshaller

import (
	"encoding/json"

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

// TypedJSONMarshaller is a generic JSON marshaller for typed objects.
type TypedJSONMarshaller[T any] struct{}

// NewTypedJSONMarshaller creates a new TypedJSONMarshaller for the specified type.
func NewTypedJSONMarshaller[T any]() TypedJSONMarshaller[T] {
	return TypedJSONMarshaller[T]{}
}

// Marshal serializes the typed data to JSON format.
func (m TypedJSONMarshaller[T]) Marshal(data T) ([]byte, error) {
	marshalled, err := json.Marshal(data)
	if err != nil {
		return []byte{}, errMarshal(err)
	}

	return marshalled, nil
}

// Unmarshal deserializes JSON data into a typed object.
func (m TypedJSONMarshaller[T]) Unmarshal(data []byte) (T, error) {
	var out T

	err := json.Unmarshal(data, &out)
	if err != nil {
		return zero[T](), errUnmarshal(err)
	}

	return out, nil
}

// TypedBytesMarshaller is a passthrough marshaller for raw []byte payloads.
// It implements TypedMarshaller[[]byte] without performing any encoding,
// useful when values are already serialized or stored as opaque blobs.
type TypedBytesMarshaller struct{}

// NewTypedBytesMarshaller creates a new TypedBytesMarshaller.
func NewTypedBytesMarshaller() TypedBytesMarshaller {
	return TypedBytesMarshaller{}
}

// Marshal returns the input bytes unchanged.
func (m TypedBytesMarshaller) Marshal(data []byte) ([]byte, error) {
	return data, nil
}

// Unmarshal returns the input bytes unchanged.
func (m TypedBytesMarshaller) Unmarshal(data []byte) ([]byte, error) {
	return data, nil
}
