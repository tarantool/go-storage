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
