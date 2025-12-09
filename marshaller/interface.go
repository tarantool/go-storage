package marshaller

// Marshaller - serialization by default (JSON/Protobuf/etc),
// implements one time for all objects.
// Required for `integrity.Storage` to set marshalling format for any type object
// and as recommendation for developers of `Storage` wrappers.
type Marshaller interface {
	Marshal(data any) ([]byte, error)
	Unmarshal(data []byte, out any) error
}

// Marshallable - custom object serialization, implements for each object.
// Required for `integrity.Storage` type to set marshalling format to specific object
// and as recommendation for developers of `Storage` wrappers.
type Marshallable interface {
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// TypedMarshaller is a generic interface for typed marshalling operations.
type TypedMarshaller[T any] interface {
	Marshal(data T) ([]byte, error)
	Unmarshal(data []byte) (T, error)
}
