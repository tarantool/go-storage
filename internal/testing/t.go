package testing

import (
	"fmt"
	"os"
	"testing"
)

// T is a dummy implementation of the testing.T interface to use in examples.
type T interface {
	Helper()
	Log(args ...any)
	Logf(format string, args ...any)
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
}

// DummyT is a dummy implementation of the testing.T interface to use in examples.
type DummyT struct {
	testing.T

	cleanups []func()
}

// NewT returns a new dummy T instance.
func NewT() *DummyT {
	return &DummyT{T: testing.T{}, cleanups: nil}
}

var (
	_ T          = &DummyT{} //nolint:exhaustruct
	_ testing.TB = &DummyT{} //nolint:exhaustruct
)

// Attr is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Attr(_, _ string) {}

// Error is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Error(_ ...any) {
}

// Fail is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Fail() {
	panic("fail")
}

// FailNow is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) FailNow() {
	panic("fail now")
}

// Failed is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Failed() bool {
	return false
}

// Fatal is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Fatal(args ...any) {
	args = append([]any{"fatal: "}, args...)
	panic(fmt.Sprint(args...))
}

// Name is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Name() string {
	return "DummyT"
}

// Setenv is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Setenv(_, _ string) {}

// Chdir is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Chdir(_ string) {}

// Skip is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Skip(_ ...any) {
	panic("skip")
}

// SkipNow is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) SkipNow() {
	panic("skip now")
}

// Skipf is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Skipf(format string, args ...any) {
	panic(fmt.Sprintf("skip: "+format, args...))
}

// Skipped is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Skipped() bool {
	return false
}

// Helper is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Helper() {}

// Log is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Log(args ...any) {
	_, _ = fmt.Fprintln(os.Stderr, args...)
}

// Logf is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Logf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format, args...)
}

// Fatalf is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Fatalf(format string, args ...any) {
	panic("fatal error: " + fmt.Sprintf(format, args...))
}

// Errorf is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Errorf(format string, args ...any) {
	panic("error: " + fmt.Sprintf(format, args...))
}

// Cleanup is a dummy implementation of the testing.T interface to use in examples.
func (t *DummyT) Cleanup(f func()) {
	t.cleanups = append(t.cleanups, f)
}

// Cleanups should be called to call all cleanup functions on "test" exit.
func (t *DummyT) Cleanups() {
	for _, cleanupFunc := range t.cleanups {
		cleanupFunc()
	}
}
