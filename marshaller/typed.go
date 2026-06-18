package marshaller

import (
	"encoding/json"

	"gopkg.in/yaml.v3"
)

// YamlMarshaller is a generic YAML marshaller for typed objects.
type YamlMarshaller[T any] struct{}

// NewYamlMarshaller creates a new YamlMarshaller for the specified type.
func NewYamlMarshaller[T any]() YamlMarshaller[T] {
	return YamlMarshaller[T]{}
}

// Marshal serializes the typed data to YAML format.
func (m YamlMarshaller[T]) Marshal(data T) ([]byte, error) {
	marshalled, err := yaml.Marshal(data)
	if err != nil {
		return []byte{}, errMarshal(err)
	}

	return marshalled, nil
}

// Unmarshal deserializes YAML data into a typed object.
func (m YamlMarshaller[T]) Unmarshal(data []byte) (T, error) {
	var out T

	err := yaml.Unmarshal(data, &out)
	if err != nil {
		return zero[T](), errUnmarshal(err)
	}

	return out, nil
}

// JSONMarshaller is a generic JSON marshaller for typed objects.
type JSONMarshaller[T any] struct{}

// NewJSONMarshaller creates a new JSONMarshaller for the specified type.
func NewJSONMarshaller[T any]() JSONMarshaller[T] {
	return JSONMarshaller[T]{}
}

// Marshal serializes the typed data to JSON format.
func (m JSONMarshaller[T]) Marshal(data T) ([]byte, error) {
	marshalled, err := json.Marshal(data)
	if err != nil {
		return []byte{}, errMarshal(err)
	}

	return marshalled, nil
}

// Unmarshal deserializes JSON data into a typed object.
func (m JSONMarshaller[T]) Unmarshal(data []byte) (T, error) {
	var out T

	err := json.Unmarshal(data, &out)
	if err != nil {
		return zero[T](), errUnmarshal(err)
	}

	return out, nil
}

// BytesMarshaller is a passthrough marshaller for raw []byte payloads.
// It implements Marshaller[[]byte] without performing any encoding,
// useful when values are already serialized or stored as opaque blobs.
type BytesMarshaller struct{}

// NewBytesMarshaller creates a new BytesMarshaller.
func NewBytesMarshaller() BytesMarshaller {
	return BytesMarshaller{}
}

// Marshal returns the input bytes unchanged.
func (m BytesMarshaller) Marshal(data []byte) ([]byte, error) {
	return data, nil
}

// Unmarshal returns the input bytes unchanged.
func (m BytesMarshaller) Unmarshal(data []byte) ([]byte, error) {
	return data, nil
}
