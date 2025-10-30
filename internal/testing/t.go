package testing

import (
	"fmt"
	"os"
)

// T is a dummy implementation of the testing.T interface to use in examples.
type T interface {
	Helper()
	Log(args ...interface{})
	Logf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type dummyT struct{}

func (t *dummyT) Helper() {}

func (t *dummyT) Log(args ...interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, args...)
}

func (t *dummyT) Logf(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format, args...)
}

func (t *dummyT) Fatalf(format string, args ...interface{}) {
	panic("fatal error: " + fmt.Sprintf(format, args...))
}

func (t *dummyT) Errorf(format string, args ...interface{}) {
	panic("error: " + fmt.Sprintf(format, args...))
}

// NewT returns a new dummy T instance.
func NewT() T {
	return &dummyT{}
}
