package tx

import "github.com/tarantool/go-storage/kv"

// RequestResponse represents the response for an individual transaction operation.
type RequestResponse struct {
	// KeyValue contains the result data for Get operations.
	KeyValue *kv.KeyValue

	// Success indicates whether the operation was successful.
	Success bool

	// Error contains any error that occurred during the operation.
	Error error
}
