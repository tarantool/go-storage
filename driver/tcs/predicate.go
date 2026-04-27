package tcs

import (
	"errors"

	"github.com/vmihailenco/msgpack/v5"

	goPredicate "github.com/tarantool/go-storage/predicate"
)

var (
	// ErrUnknownOperator is returned when the operator is unknown.
	ErrUnknownOperator = errors.New("unknown operator")
	// ErrUnknownTarget is returned when the target is unknown.
	ErrUnknownTarget = errors.New("unknown target")

	_ msgpack.CustomEncoder = predicate{Predicate: nil}

	//nolint: gochecknoglobals
	operators = map[goPredicate.Op]string{
		goPredicate.OpEqual:    "==",
		goPredicate.OpNotEqual: "!=",
		goPredicate.OpGreater:  ">",
		goPredicate.OpLess:     "<",
	}

	//nolint: gochecknoglobals
	targets = map[goPredicate.Target]string{
		goPredicate.TargetValue:   "value",
		goPredicate.TargetVersion: "mod_revision",
	}
)

// getOperator returns the TCS operator string for a predicate operation.
func getOperator(op goPredicate.Op) (string, bool) {
	result, ok := operators[op]
	return result, ok
}

// getTarget returns the TCS target string for a predicate target.
func getTarget(target goPredicate.Target) (string, bool) {
	result, ok := targets[target]
	return result, ok
}

type predicate struct {
	goPredicate.Predicate
}

func newPredicates(inPredicates []goPredicate.Predicate) []predicate {
	outPredicates := make([]predicate, 0, len(inPredicates))
	for _, p := range inPredicates {
		outPredicates = append(outPredicates, predicate{p})
	}

	return outPredicates
}

const (
	predicateArrayLen = 4

	tcsTargetCount = "count"
)

// EncodeMsgpack writes the predicate in TCS wire format.
//
// Quirk: TCS errors when a `mod_revision` predicate references a key that
// does not exist. Etcd's canonical absence/presence checks —
// VersionEqual(key, 0) and VersionNotEqual(key, 0) — would therefore fail
// on TCS. To preserve cross-driver portability, this encoder transparently
// rewrites those two shapes to the TCS-native `count` target on the wire
// (target == "count", same op, same value 0). The Predicate value returned
// by predicate.VersionEqual / predicate.VersionNotEqual is unchanged; only
// the wire bytes differ. This mirrors the rewrite TCS's server-side etcd
// shim performs for `MOD == 0` and `MOD != 0`.
func (p predicate) EncodeMsgpack(encoder *msgpack.Encoder) error {
	op, ok := getOperator(p.Operation()) //nolint:varnamelen
	if !ok {
		return ErrUnknownOperator
	}

	target, ok := getTarget(p.Target())
	if !ok {
		return ErrUnknownTarget
	}

	value := p.Value()
	if p.Target() == goPredicate.TargetVersion {
		if v, isInt64 := value.(int64); isInt64 && v == 0 &&
			(p.Operation() == goPredicate.OpEqual || p.Operation() == goPredicate.OpNotEqual) {
			target = tcsTargetCount
		}
	}

	err := encoder.EncodeArrayLen(predicateArrayLen)
	if err != nil {
		return NewPredicateEncodingError("encode array length", err)
	}

	err = encoder.EncodeString(target)
	if err != nil {
		return NewPredicateEncodingError("encode target", err)
	}

	err = encoder.EncodeString(op)
	if err != nil {
		return NewPredicateEncodingError("encode operator", err)
	}

	// TCS compares stored values as Lua strings; encoding []byte as msgpack bin
	// causes a type mismatch in comparisons, so we encode it as string.
	if b, ok := value.([]byte); ok {
		value = string(b)
	}

	err = encoder.Encode(value)
	if err != nil {
		return NewPredicateEncodingError("encode value", err)
	}

	// We're deliberately using here conversion from byte to string, since MsgPack API doesn't have a way to
	// write byte array as string.
	err = encoder.EncodeString(string(p.Key()))
	if err != nil {
		return NewPredicateEncodingError("encode key", err)
	}

	return nil
}
