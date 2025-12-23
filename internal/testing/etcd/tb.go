package etcd

import (
	"fmt"
	"os"

	"go.etcd.io/etcd/client/pkg/v3/testutil"
)

// NoopTB is a testutil.TB implementation that discards all logs.
type NoopTB struct {
	name     string
	failed   bool
	cleanups []func()
}

var _ testutil.TB = &NoopTB{} //nolint:exhaustruct

// NewNoopTB creates a new no-op testutil.TB implementation.
// Returns the TB and a cleanup function that should be called when done.
func NewNoopTB(name string) (testutil.TB, func()) {
	tb := &NoopTB{name: name} //nolint:exhaustruct
	return tb, tb.close
}

func (t *NoopTB) Helper() {}

func (t *NoopTB) Cleanup(f func()) {
	t.cleanups = append(t.cleanups, f)
}

func (t *NoopTB) Error(args ...any) {
	t.Fail()
}

func (t *NoopTB) Errorf(format string, args ...any) {
	t.Fail()
}

func (t *NoopTB) Fail() {
	t.failed = true
}

func (t *NoopTB) FailNow() {
	t.failed = true

	panic("FailNow() called")
}

func (t *NoopTB) Failed() bool {
	return t.failed
}

func (t *NoopTB) Fatal(args ...any) {
	panic("Fatal: " + fmt.Sprint(args...))
}

func (t *NoopTB) Fatalf(format string, args ...any) {
	panic("Fatal: " + fmt.Sprintf(format, args...))
}

func (t *NoopTB) Log(...any) {}

func (t *NoopTB) Logf(string, ...any) {}

func (t *NoopTB) Name() string {
	return t.name
}

func (t *NoopTB) TempDir() string {
	dir, err := os.MkdirTemp("", t.name)
	if err != nil {
		t.Fatal(err)
	}

	t.cleanups = append([]func(){func() {
		err := os.RemoveAll(dir)
		if err != nil {
			fmt.Println("failed to remove temporary directory: ", err) //nolint:forbidigo
		}
	}}, t.cleanups...)

	return dir
}

func (t *NoopTB) Skip(...any) {}

func (t *NoopTB) close() {
	for i := len(t.cleanups) - 1; i >= 0; i-- {
		t.cleanups[i]()
	}
}

// SilentTB wraps a testutil.TB and discards all logs.
type SilentTB struct {
	testutil.TB
}

var _ testutil.TB = &SilentTB{} //nolint:exhaustruct

// NewSilentTB creates a new silent testutil.TB wrapper.
func NewSilentTB(tb testutil.TB) testutil.TB {
	return &SilentTB{TB: tb}
}

func (s *SilentTB) Log(args ...any) {}

func (s *SilentTB) Logf(format string, args ...any) {}
