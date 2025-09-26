package tcs

import (
	"fmt"
)

// DecodingError represents an error that occurs during decoding operations.
type DecodingError struct {
	ObjectType string
	Text       string
	Err        error
}

// Error returns the error message.
func (e DecodingError) Error() string {
	suffix := e.ObjectType
	if e.Text != "" {
		suffix = fmt.Sprintf("%s, %s", suffix, e.Text)
	}

	return fmt.Sprintf("failed to decode %s: %s", suffix, e.Err)
}

func (e DecodingError) Unwrap() error {
	return e.Err
}

// NewTxnOpResponseDecodingError returns a new txnOpResponse decoding error.
func NewTxnOpResponseDecodingError(err error) error {
	if err == nil {
		return nil
	}

	return DecodingError{
		ObjectType: "txnOpResponse",
		Text:       "",
		Err:        err,
	}
}

// EncodingError represents an error that occurs during encoding operations.
type EncodingError struct {
	ObjectType string
	Text       string
	Err        error
}

// Error returns the error message.
func (e EncodingError) Error() string {
	if e.Text == "" {
		return fmt.Sprintf("failed to encode %s: %s", e.ObjectType, e.Err)
	}

	return fmt.Sprintf("failed to encode %s, %s: %s", e.ObjectType, e.Text, e.Err)
}

func (e EncodingError) Unwrap() error {
	return e.Err
}

// NewOperationEncodingError returns a new operation encoding error.
func NewOperationEncodingError(text string, err error) error {
	if err == nil {
		return nil
	}

	return EncodingError{
		ObjectType: "operation",
		Text:       text,
		Err:        err,
	}
}

// NewPredicateEncodingError returns a new predicate encoding error.
func NewPredicateEncodingError(text string, err error) error {
	if err == nil {
		return nil
	}

	return EncodingError{
		ObjectType: "predicate",
		Text:       text,
		Err:        err,
	}
}
