//nolint:testpackage
package connect

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
)

type doerWatcherStub struct {
	doErr error
	doN   int
}

func (s *doerWatcherStub) Do(req tarantool.Request) *tarantool.Future {
	s.doN++

	fut := tarantool.NewFuture(req)
	fut.SetError(s.doErr)

	return fut
}

func (s *doerWatcherStub) NewWatcher(_ string, _ tarantool.WatchCallback) (tarantool.Watcher, error) {
	return nil, errors.New("not implemented")
}

func TestProbeTCSConnection_OK(t *testing.T) {
	t.Parallel()

	stub := &doerWatcherStub{doErr: nil, doN: 0}
	err := probeTCSConnection(context.Background(), stub)
	require.NoError(t, err)
	require.Equal(t, 1, stub.doN)
}

func TestProbeTCSConnection_ErrorWrapped(t *testing.T) {
	t.Parallel()

	stubErr := errors.New("dial/auth failed")
	stub := &doerWatcherStub{doErr: stubErr, doN: 0}

	err := probeTCSConnection(context.Background(), stub)
	require.Error(t, err)
	require.ErrorIs(t, err, errFailedTarantoolProbe)
	require.ErrorIs(t, err, stubErr)
	require.Equal(t, 1, stub.doN)
}
