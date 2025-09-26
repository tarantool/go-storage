package tkv

import (
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-storage/predicate"
)

var (
	// ErrUnknownOperator is returned when the operator is unknown.
	ErrUnknownOperator = errors.New("unknown operator")
	// ErrUnknownTarget is returned when the target is unknown.
	ErrUnknownTarget = errors.New("unknown target")

	_ msgpack.CustomEncoder = tkvPredicate{Predicate: nil}

	//nolint: gochecknoglobals
	operators = map[predicate.Op]string{
		predicate.OpEqual:    "==",
		predicate.OpNotEqual: "!=",
		predicate.OpGreater:  ">",
		predicate.OpLess:     "<",
	}

	//nolint: gochecknoglobals
	targets = map[predicate.Target]string{
		predicate.TargetValue:   "value",
		predicate.TargetVersion: "mod_revision",
	}
)

// FailedToEncodeTkvPredicateError is returned when we failed to encode tkvPredicate.
type FailedToEncodeTkvPredicateError struct {
	Text string
	Err  error
}

// Error returns the error message.
func (e FailedToEncodeTkvPredicateError) Error() string {
	return fmt.Sprintf("failed to encode tkvPredicate, %s: %s", e.Text, e.Err)
}

// getOperator returns the TKV operator string for a predicate operation.
func getOperator(op predicate.Op) (string, bool) {
	result, ok := operators[op]
	return result, ok
}

// getTarget returns the TKV target string for a predicate target.
func getTarget(target predicate.Target) (string, bool) {
	result, ok := targets[target]
	return result, ok
}

type tkvPredicate struct {
	predicate.Predicate
}

func newTKVPredicates(predicates []predicate.Predicate) []tkvPredicate {
	tkvPredicates := make([]tkvPredicate, 0, len(predicates))
	for _, p := range predicates {
		tkvPredicates = append(tkvPredicates, tkvPredicate{p})
	}

	return tkvPredicates
}

const (
	defaultPredicateArrayLen = 4
)

func (p tkvPredicate) EncodeMsgpack(encoder *msgpack.Encoder) error {
	op, ok := getOperator(p.Operation()) //nolint:varnamelen
	if !ok {
		return ErrUnknownOperator
	}

	target, ok := getTarget(p.Target())
	if !ok {
		return ErrUnknownTarget
	}

	err := encoder.EncodeArrayLen(defaultPredicateArrayLen)
	if err != nil {
		return FailedToEncodeTkvPredicateError{Text: "encode array length", Err: err}
	}

	err = encoder.EncodeString(target)
	if err != nil {
		return FailedToEncodeTkvPredicateError{Text: "encode target", Err: err}
	}

	err = encoder.EncodeString(op)
	if err != nil {
		return FailedToEncodeTkvPredicateError{Text: "encode operator", Err: err}
	}

	err = encoder.Encode(p.Value())
	if err != nil {
		return FailedToEncodeTkvPredicateError{Text: "encode value", Err: err}
	}

	// We're deliberately using here conversion from byte to string, since MsgPack API doesn't have a way to
	// write byte array as string.
	err = encoder.EncodeString(string(p.Key()))
	if err != nil {
		return FailedToEncodeTkvPredicateError{Text: "encode key", Err: err}
	}

	return nil
}
