package integrity

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ImpossibleError represents an error when an integrity operation cannot be performed due to internal problems.
type ImpossibleError struct {
	text string
}

func errImpossibleHasher(property string) error {
	return ImpossibleError{
		text: "hasher not found: " + property,
	}
}

func errImpossibleSigner(property string) error {
	return ImpossibleError{
		text: "signer not found: " + property,
	}
}

func errImpossibleKeyType(keyType string) error {
	return ImpossibleError{
		text: "unknown key type: " + keyType,
	}
}

func (e ImpossibleError) Error() string {
	return e.text
}

// ValidationError represents an error when validation fails.
type ValidationError struct {
	text   string
	parent error
}

// Error returns a string representation of the validation error.
func (e ValidationError) Error() string {
	if e.parent == nil {
		return e.text
	}

	return fmt.Sprintf("%s: %s", e.text, e.parent)
}

func (e ValidationError) Unpack() error {
	return e.parent
}

// FailedToGenerateKeysError represents an error when key generation fails.
type FailedToGenerateKeysError struct {
	parent error
}

func errFailedToGenerateKeys(parent error) error {
	return FailedToGenerateKeysError{parent: parent}
}

// Unwrap returns the underlying error that caused the key generation failure.
func (e FailedToGenerateKeysError) Unwrap() error {
	return e.parent
}

// Error returns a string representation of the key generation error.
func (e FailedToGenerateKeysError) Error() string {
	if e.parent == nil {
		return "failed to generate keys"
	}

	return "failed to generate keys: " + e.parent.Error()
}

// FailedToMarshalValueError represents an error when value marshalling fails.
type FailedToMarshalValueError struct {
	parent error
}

func errFailedToMarshalValue(parent error) error {
	return FailedToMarshalValueError{parent: parent}
}

// Unwrap returns the underlying error that caused the marshalling failure.
func (e FailedToMarshalValueError) Unwrap() error {
	return e.parent
}

// Error returns a string representation of the marshalling error.
func (e FailedToMarshalValueError) Error() string {
	if e.parent == nil {
		return "failed to marshal value"
	}

	return "failed to marshal value: " + e.parent.Error()
}

// FailedToComputeHashError represents an error when hash computation fails.
type FailedToComputeHashError struct {
	parent error
}

func errFailedToComputeHash(parent error) error {
	return FailedToComputeHashError{parent: parent}
}

// Unwrap returns the underlying error that caused the hash computation failure.
func (e FailedToComputeHashError) Unwrap() error {
	return e.parent
}

// Error returns a string representation of the hash computation error.
func (e FailedToComputeHashError) Error() string {
	if e.parent == nil {
		return "failed to compute hash"
	}

	return "failed to compute hash: " + e.parent.Error()
}

// FailedToGenerateSignatureError represents an error when signature generation fails.
type FailedToGenerateSignatureError struct {
	parent error
}

func errFailedToGenerateSignature(parent error) error {
	return FailedToGenerateSignatureError{parent: parent}
}

// Unwrap returns the underlying error that caused the signature generation failure.
func (e FailedToGenerateSignatureError) Unwrap() error {
	return e.parent
}

// Error returns a string representation of the signature generation error.
func (e FailedToGenerateSignatureError) Error() string {
	if e.parent == nil {
		return "failed to generate signature"
	}

	return "failed to generate signature: " + e.parent.Error()
}

// FailedToValidateAggregatedError represents aggregated validation errors.
type FailedToValidateAggregatedError struct {
	parent []error
}

func errHashNotVerifiedMissing(hasherName string) error {
	return ValidationError{
		text:   fmt.Sprintf("hash \"%s\" not verified (missing)", hasherName),
		parent: nil,
	}
}

func errSignatureNotVerifiedMissing(verifierName string) error {
	return ValidationError{
		text:   fmt.Sprintf("signature \"%s\" not verified (missing)", verifierName),
		parent: nil,
	}
}

func errHashMismatch(hasherName string, expected, got []byte) error {
	return ValidationError{
		text:   fmt.Sprintf("hash mismatch for \"%s\"", hasherName),
		parent: hashMismatchDetailError{expected: expected, got: got},
	}
}

type hashMismatchDetailError struct {
	expected []byte
	got      []byte
}

func (h hashMismatchDetailError) Error() string {
	return fmt.Sprintf("expected \"%s\", got \"%s\"", hex.Dump(h.expected), hex.Dump(h.got))
}

func errFailedToParseKey(parent error) error {
	return ValidationError{
		text:   "failed to parse key",
		parent: parent,
	}
}

func errFailedToComputeHashWith(hasherName string, parent error) error {
	return ValidationError{
		text:   fmt.Sprintf("failed to calculate hash \"%s\"", hasherName),
		parent: parent,
	}
}

func errSignatureVerificationFailed(verifierName string, parent error) error {
	return ValidationError{
		text:   fmt.Sprintf("signature verification failed for \"%s\"", verifierName),
		parent: parent,
	}
}

func errFailedToUnmarshal(parent error) error {
	return ValidationError{
		text:   "failed to unmarshal record",
		parent: parent,
	}
}

// Unwrap returns the underlying slice of errors.
func (e *FailedToValidateAggregatedError) Unwrap() []error {
	return e.parent
}

// Append adds an error to the aggregated error.
func (e *FailedToValidateAggregatedError) Append(err error) {
	if err != nil {
		e.parent = append(e.parent, err)
	}
}

// Error returns a string representation of the aggregated error.
func (e *FailedToValidateAggregatedError) Error() string {
	switch {
	case len(e.parent) == 0:
		return ""
	case len(e.parent) == 1:
		return e.parent[0].Error()
	default:
		var errStrings []string
		for _, p := range e.parent {
			errStrings = append(errStrings, p.Error())
		}

		return "aggregated error: " + strings.Join(errStrings, ", ")
	}
}

// Finalize returns nil if there are no errors, otherwise returns error or the aggregated error.
func (e *FailedToValidateAggregatedError) Finalize() error {
	switch len(e.parent) {
	case 0:
		return nil
	case 1:
		return e.parent[0]
	default:
		return e
	}
}

// InvalidNameError represents an error when a name is invalid.
type InvalidNameError struct {
	name string
}

// Error returns a string representation of the invalid name error.
func (e InvalidNameError) Error() string {
	return "invalid name: " + e.name
}

// ErrInvalidName is a sentinel error for invalid names.
var ErrInvalidName = InvalidNameError{name: ""}
