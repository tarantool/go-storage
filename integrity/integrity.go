// Package integrity provides typed storage with built-in data integrity
// protection. It automatically computes and verifies hashes and signatures for
// stored values.
//
// See [CodecBuilder] (via [NewCodecBuilder]) for configuring a [Codec], and
// [Store] / [SingletonStore] for the storage-bound operation surfaces.
package integrity

import (
	"errors"
	"strings"

	"github.com/tarantool/go-storage/v2/internal/options"
	"github.com/tarantool/go-storage/v2/predicate"
)

// Predicate builds a key-scoped predicate.Predicate once the on-disk key is
// known.
type Predicate func(key []byte) predicate.Predicate

func checkName(name string) bool {
	switch {
	case len(name) == 0:
		return false
	case strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/"):
		return false
	default:
		return true
	}
}

func checkPrefix(prefix string) bool {
	switch {
	case len(prefix) == 0:
		return false
	case strings.HasPrefix(prefix, "/") && len(prefix) > 1:
		return false
	case !strings.HasSuffix(prefix, "/"):
		return false
	default:
		return true
	}
}

func checkRangeName(name string) bool {
	return !strings.HasPrefix(name, "/")
}

type getOptions struct {
	ignoreVerificationError bool
	ignoreMoreThanOneResult bool
}

type putOptions struct {
	Predicates []Predicate
}

type deleteOptions struct {
	withPrefix bool
	Predicates []Predicate
}

// WithPutPredicates configures predicates for conditional Put operations.
// The Put operation will only succeed if all predicates evaluate to true.
// If predicates are specified but fail, [ErrPredicateFailed] is returned.
func WithPutPredicates(predicates ...Predicate) options.OptionCallback[putOptions] {
	return func(opts *putOptions) {
		opts.Predicates = append(opts.Predicates, predicates...)
	}
}

// WithDeletePredicates configures predicates for conditional Delete operations.
// The Delete operation will only succeed if all predicates evaluate to true.
// If predicates are specified but fail, [ErrPredicateFailed] is returned.
func WithDeletePredicates(predicates ...Predicate) options.OptionCallback[deleteOptions] {
	return func(opts *deleteOptions) {
		opts.Predicates = append(opts.Predicates, predicates...)
	}
}

// WithPrefix configures the ability to delete keys by a prefix.
func WithPrefix() options.OptionCallback[deleteOptions] {
	return func(opts *deleteOptions) {
		opts.withPrefix = true
	}
}

// IgnoreVerificationError returns an option that allows Get and Range operations
// to return results even if hash or signature verification fails.
// The returned result will still contain the Error field with verification details.
func IgnoreVerificationError() options.OptionCallback[getOptions] {
	return func(opts *getOptions) {
		opts.ignoreVerificationError = true
	}
}

// IgnoreMoreThanOneResult returns an option that allows Get operation
// to succeed when multiple results are returned for a single name.
// By default, Get returns ErrMoreThanOneResult in such cases.
func IgnoreMoreThanOneResult() options.OptionCallback[getOptions] {
	return func(opts *getOptions) {
		opts.ignoreMoreThanOneResult = true
	}
}

var (
	ErrNotFound                  = errors.New("not found")
	ErrMoreThanOneResult         = errors.New("more than one result was returned")
	ErrInvalidPredicateValueType = errors.New("invalid predicate value type")
	ErrNoValueKey                = errors.New("no value key found in generated keys")
	// ErrPredicateFailed is returned by Put or Delete when predicates are specified
	// but the transaction predicate check fails (i.e., the conditions are not met).
	// Use [WithPutPredicates] or [WithDeletePredicates] to specify predicates.
	ErrPredicateFailed = errors.New("predicate check failed")
)
