package tkv

import (
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-storage/operation"
)

// Error definitions for err113 compliance.
var (
	// ErrUnknownOperation is returned when the operation is unknown.
	ErrUnknownOperation = errors.New("unknown operation")
)

// FailedToEncodeTkvOperationError is returned when we failed to encode tkvOperation.
type FailedToEncodeTkvOperationError struct {
	Text string
	Err  error
}

// Error returns the error message.
func (e FailedToEncodeTkvOperationError) Error() string {
	return fmt.Sprintf("failed to encode tkvOperation, %s: %s", e.Text, e.Err)
}

const (
	// putOperationArrayLen is the length of the array that is used to encode a put operation.
	putOperationArrayLen = 3
	// otherOperationArrayLen is the length of the array that is used to encode a delete operation.
	otherOperationArrayLen = 2
)

var (
	//nolint: gochecknoglobals
	ops = map[operation.Type]string{
		operation.TypeGet:    "get",
		operation.TypePut:    "put",
		operation.TypeDelete: "delete",
	}
)

// getOperation returns the TKV operation string for an operation type.
func getOperation(opType operation.Type) (string, bool) {
	result, ok := ops[opType]
	return result, ok
}

type tkvOperation struct {
	operation.Operation
}

// newTKVOperations returns a slice of TKV operations from a slice of operations.
func newTKVOperations(operations []operation.Operation) []tkvOperation {
	tkvOperations := make([]tkvOperation, 0, len(operations))
	for _, o := range operations {
		tkvOperations = append(tkvOperations, tkvOperation{o})
	}

	return tkvOperations
}

func (o tkvOperation) EncodeMsgpack(encoder *msgpack.Encoder) error {
	op, ok := getOperation(o.Type()) //nolint:varnamelen
	if !ok {
		return ErrUnknownOperation
	}

	switch {
	case o.Type() == operation.TypePut:
		err := encoder.EncodeArrayLen(putOperationArrayLen)
		if err != nil {
			return FailedToEncodeTkvOperationError{Text: "encode put operation array length", Err: err}
		}

		err = encoder.EncodeString(op)
		if err != nil {
			return FailedToEncodeTkvOperationError{Text: "encode put operation", Err: err}
		}

		err = encoder.EncodeString(string(o.Key()))
		if err != nil {
			return FailedToEncodeTkvOperationError{Text: "encode put operation key", Err: err}
		}

		err = encoder.EncodeString(string(o.Value()))
		if err != nil {
			return FailedToEncodeTkvOperationError{Text: "encode put operation value", Err: err}
		}
	default:
		err := encoder.EncodeArrayLen(otherOperationArrayLen)
		if err != nil {
			return FailedToEncodeTkvOperationError{Text: "encode operation array length", Err: err}
		}

		err = encoder.EncodeString(op)
		if err != nil {
			return FailedToEncodeTkvOperationError{Text: "encode operation", Err: err}
		}

		err = encoder.EncodeString(string(o.Key()))
		if err != nil {
			return FailedToEncodeTkvOperationError{Text: "encode operation key", Err: err}
		}
	}

	return nil
}
