package tx

import "github.com/tarantool/go-storage/kv"

// RequestResponse represents the response for an individual transaction operation.
type RequestResponse struct {
	// KeyValue contains the result data for Get operations.
	Values []kv.KeyValue
}
