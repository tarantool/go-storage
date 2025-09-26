package tcs

import (
	"errors"

	"github.com/vmihailenco/msgpack/v5"

	goOperation "github.com/tarantool/go-storage/operation"
)

// Error definitions for err113 compliance.
var (
	// ErrUnknownOperation is returned when the operation is unknown.
	ErrUnknownOperation = errors.New("unknown operation")
)

const (
	// putOperationArrayLen is the length of the array that is used to encode a put operation.
	putOperationArrayLen = 3
	// otherOperationArrayLen is the length of the array that is used to encode a delete operation.
	otherOperationArrayLen = 2
)

var (
	//nolint: gochecknoglobals
	ops = map[goOperation.Type]string{
		goOperation.TypeGet:    "get",
		goOperation.TypePut:    "put",
		goOperation.TypeDelete: "delete",
	}
)

// getOperation returns the TCS operation string for an operation type.
func getOperation(opType goOperation.Type) (string, bool) {
	result, ok := ops[opType]
	return result, ok
}

type operation struct {
	goOperation.Operation
}

// newOperations returns a slice of TCS operations from a slice of operations.
func newOperations(inOperations []goOperation.Operation) []operation {
	outOperations := make([]operation, 0, len(inOperations))
	for _, o := range inOperations {
		outOperations = append(outOperations, operation{o})
	}

	return outOperations
}

func (o operation) EncodeMsgpack(encoder *msgpack.Encoder) error {
	op, ok := getOperation(o.Type()) //nolint:varnamelen
	if !ok {
		return ErrUnknownOperation
	}

	switch {
	case o.Type() == goOperation.TypePut:
		err := encoder.EncodeArrayLen(putOperationArrayLen)
		if err != nil {
			return NewOperationEncodingError("encode put operation array length", err)
		}

		err = encoder.EncodeString(op)
		if err != nil {
			return NewOperationEncodingError("encode put operation", err)
		}

		// We're deliberately using here conversion from byte to string, since MsgPack API doesn't have a way to
		// write byte array as string.
		err = encoder.EncodeString(string(o.Key()))
		if err != nil {
			return NewOperationEncodingError("encode put operation key", err)
		}

		// We're deliberately using here conversion from byte to string, since MsgPack API doesn't have a way to
		// write byte array as string.
		err = encoder.EncodeString(string(o.Value()))
		if err != nil {
			return NewOperationEncodingError("encode put operation value", err)
		}
	default:
		err := encoder.EncodeArrayLen(otherOperationArrayLen)
		if err != nil {
			return NewOperationEncodingError("encode operation array length", err)
		}

		err = encoder.EncodeString(op)
		if err != nil {
			return NewOperationEncodingError("encode operation", err)
		}

		// We're deliberately using here conversion from byte to string, since MsgPack API doesn't have a way to
		// write byte array as string.
		err = encoder.EncodeString(string(o.Key()))
		if err != nil {
			return NewOperationEncodingError("encode operation key", err)
		}
	}

	return nil
}
