package marshaller

// Marshaller is a generic interface for typed marshalling operations.
type Marshaller[T any] interface {
	Marshal(data T) ([]byte, error)
	Unmarshal(data []byte) (T, error)
}
