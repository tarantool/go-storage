package etcd

import (
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-storage/predicate"
)

// predicatesToCmps converts a predicate list to an etcd comparison list.
func predicatesToCmps(predicates []predicate.Predicate) ([]etcd.Cmp, error) {
	convertedPredicates := make([]etcd.Cmp, 0, len(predicates))
	for _, pred := range predicates {
		convertedPredicate, err := predicateToCmp(pred)
		if err != nil {
			return nil, err
		}

		convertedPredicates = append(convertedPredicates, convertedPredicate)
	}

	return convertedPredicates, nil
}

// predicateToCmp converts a predicate to an etcd comparison.
func predicateToCmp(pred predicate.Predicate) (etcd.Cmp, error) {
	switch pred.Target() {
	case predicate.TargetValue:
		return valuePredicateToCmp(pred)
	case predicate.TargetVersion:
		return versionPredicateToCmp(pred)
	default:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedPredicateTarget, pred.Target())
	}
}

// valuePredicateToCmp converts a value predicate to an etcd comparison.
func valuePredicateToCmp(pred predicate.Predicate) (etcd.Cmp, error) {
	key := string(pred.Key())
	value, ok := pred.Value().([]byte)

	if !ok {
		return etcd.Cmp{}, errValuePredicateRequiresBytes
	}

	switch pred.Operation() {
	case predicate.OpEqual:
		return etcd.Compare(etcd.Value(key), "=", string(value)), nil
	case predicate.OpNotEqual:
		return etcd.Compare(etcd.Value(key), "!=", string(value)), nil
	case predicate.OpGreater, predicate.OpLess:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedValueOperation, pred.Operation())
	default:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedValueOperation, pred.Operation())
	}
}

// versionPredicateToCmp converts a version predicate to an etcd comparison.
func versionPredicateToCmp(pred predicate.Predicate) (etcd.Cmp, error) {
	key := string(pred.Key())
	version, ok := pred.Value().(int64)

	if !ok {
		return etcd.Cmp{}, errVersionPredicateRequiresInt
	}

	switch pred.Operation() {
	case predicate.OpEqual:
		return etcd.Compare(etcd.ModRevision(key), "=", version), nil
	case predicate.OpNotEqual:
		return etcd.Compare(etcd.ModRevision(key), "!=", version), nil
	case predicate.OpGreater:
		return etcd.Compare(etcd.ModRevision(key), ">", version), nil
	case predicate.OpLess:
		return etcd.Compare(etcd.ModRevision(key), "<", version), nil
	default:
		return etcd.Cmp{}, fmt.Errorf("%w: %v", errUnsupportedVersionOperation, pred.Operation())
	}
}
