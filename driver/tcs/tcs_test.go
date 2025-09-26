// Package tcs_test provides unit tests for the TCS driver implementation.
// It uses mocks to test the driver without requiring a real Tarantool connection.
package tcs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-storage/driver/tcs"
	"github.com/tarantool/go-storage/internal/mocks"
	gsTesting "github.com/tarantool/go-storage/internal/testing"
	goOperation "github.com/tarantool/go-storage/operation"
	goPredicate "github.com/tarantool/go-storage/predicate"
)

const (
	defaultUnregisterTimeout = 10 * time.Second
	defaultWaitTimeout       = 10 * time.Second
)

func TestNew(t *testing.T) {
	t.Parallel()

	mockDoer := mocks.NewDoerWatcherMock(t)
	driver := tcs.New(mockDoer)

	assert.NotNil(t, driver)
}

func TestDriver_Watch_WatcherSuccess(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)

	mockDoer := mocks.NewDoerWatcherMock(mc)
	watcher := mocks.NewWatcherMock(mc).UnregisterMock.Expect().Times(1).Return()

	mockDoer.NewWatcherMock.Set(func(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
		assert.Equal(t, "config.storage:test-key", key)
		assert.NotNil(t, callback)

		return watcher, nil
	})

	driver := tcs.New(mockDoer)

	ctx := context.Background()
	key := []byte("test-key")

	events, cleanup, err := driver.Watch(ctx, key)

	require.NoError(t, err)
	assert.NotNil(t, events)
	assert.NotNil(t, cleanup)

	cleanup()

	// We should wait for the watcher to be unregistered in separate goroutine.
	// Without testing internals of this library - best way is to simply sleep a little here.
	mc.Wait(defaultUnregisterTimeout)
}

func TestDriver_Watch_WatcherError(t *testing.T) {
	t.Parallel()

	var (
		mc         = minimock.NewController(t)
		mockDoer   = mocks.NewDoerWatcherMock(mc)
		watcherErr = errors.New("watcher creation failed")
	)

	mockDoer.NewWatcherMock.Set(func(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
		assert.Equal(t, "config.storage:test-key", key)
		assert.NotNil(t, callback)

		return nil, watcherErr
	})

	driver := tcs.New(mockDoer)

	ctx := context.Background()
	key := []byte("test-key")

	events, cleanup, err := driver.Watch(ctx, key)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create watcher")
	assert.Contains(t, err.Error(), "watcher creation failed")
	assert.Nil(t, events)
	assert.Nil(t, cleanup)
}

func TestDriver_Watch_ContextCanceled_Before(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)

	mockDoer := mocks.NewDoerWatcherMock(mc)
	watcher := mocks.NewWatcherMock(mc).UnregisterMock.Expect().Times(1).Return()

	mockDoer.NewWatcherMock.Set(func(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
		assert.Equal(t, "config.storage:test-key", key)
		assert.NotNil(t, callback)

		return watcher, nil
	})

	driver := tcs.New(mockDoer)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	key := []byte("test-key")

	events, cleanup, err := driver.Watch(ctx, key)

	require.NoError(t, err)
	assert.NotNil(t, events)
	assert.NotNil(t, cleanup)

	mc.Wait(defaultUnregisterTimeout)

	require.NotPanics(t, func() { cleanup() })
}

func TestDriver_Watch_ContextCanceled_After(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)

	mockDoer := mocks.NewDoerWatcherMock(mc)
	watcher := mocks.NewWatcherMock(mc).UnregisterMock.Expect().Times(1).Return()

	mockDoer.NewWatcherMock.Set(func(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
		assert.Equal(t, "config.storage:test-key", key)
		assert.NotNil(t, callback)

		return watcher, nil
	})

	driver := tcs.New(mockDoer)

	ctx, cancel := context.WithCancel(context.Background())

	key := []byte("test-key")

	events, cleanup, err := driver.Watch(ctx, key)

	require.NoError(t, err)
	assert.NotNil(t, events)
	assert.NotNil(t, cleanup)

	cancel()

	mc.Wait(defaultUnregisterTimeout)

	require.NotPanics(t, func() { cleanup() })
}

func TestDriver_Watch_CallbackOnceCalled(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)

	mockDoer := mocks.NewDoerWatcherMock(mc)
	watcher := mocks.NewWatcherMock(mc).UnregisterMock.Expect().Times(1).Return()

	mockDoer.NewWatcherMock.Set(func(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
		assert.Equal(t, "config.storage:test-key", key)
		assert.NotNil(t, callback)

		callback(tarantool.WatchEvent{
			Conn:  nil,
			Key:   "config.storage:test-key",
			Value: []byte("test-value"),
		})

		return watcher, nil
	})

	driver := tcs.New(mockDoer)

	ctx := context.Background()
	key := []byte("test-key")

	events, cleanup, err := driver.Watch(ctx, key)

	require.NoError(t, err)
	assert.NotNil(t, events)
	assert.NotNil(t, cleanup)

	select {
	case val, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, []byte("test-key"), val.Prefix)
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "timeout")
	}

	cleanup()
	mc.Wait(defaultUnregisterTimeout)
}

func TestDriver_Watch_CallbackTwiceCalled(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)

	mockDoer := mocks.NewDoerWatcherMock(mc)
	watcher := mocks.NewWatcherMock(mc).UnregisterMock.Expect().Times(1).Return()

	mockDoer.NewWatcherMock.Set(func(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
		assert.Equal(t, "config.storage:test-key", key)
		assert.NotNil(t, callback)

		callback(tarantool.WatchEvent{
			Conn:  nil,
			Key:   "config.storage:test-key",
			Value: []byte("test-value-1"),
		})

		callback(tarantool.WatchEvent{
			Conn:  nil,
			Key:   "config.storage:test-key",
			Value: []byte("test-value-2"),
		})

		return watcher, nil
	})

	driver := tcs.New(mockDoer)

	ctx := context.Background()
	key := []byte("test-key")

	events, cleanup, err := driver.Watch(ctx, key)

	require.NoError(t, err)
	assert.NotNil(t, events)
	assert.NotNil(t, cleanup)

	select {
	case val, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, []byte("test-key"), val.Prefix)
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "timeout")
	}

	select {
	case val, ok := <-events:
		require.True(t, ok)
		assert.Fail(t, "must be empty", ": %v, %v", val, ok)
	default:
	}

	cleanup()
	mc.Wait(defaultUnregisterTimeout)
}

func TestDriver_Execute_Get_Empty(t *testing.T) {
	t.Parallel()

	data := []any{
		map[string]any{
			"data": map[string]any{
				"responses":  [][]any{{}},
				"is_success": true,
			},
			"revision": 1000,
		},
	}
	mock := gsTesting.NewMockDoer(t,
		gsTesting.NewMockResponse(t, data),
	)

	driver := tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))

	var (
		ctx        = context.Background()
		predicates []goPredicate.Predicate
		elseOps    []goOperation.Operation

		ifOps = []goOperation.Operation{
			goOperation.Get([]byte("/123")),
		}
	)

	resp, err := driver.Execute(ctx, predicates, ifOps, elseOps)
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Results, 1)
	require.Empty(t, resp.Results[0].Values)
}

func TestDriver_Execute_Get_NonEmpty(t *testing.T) {
	t.Parallel()

	data := []any{
		map[string]any{
			"data": map[string]any{
				"responses": [][]any{{
					map[string]any{
						"path":         []byte("/123"),
						"value":        []byte("123"),
						"mod_revision": 1000,
					},
				}},
				"is_success": true,
			},
			"revision": 1000,
		},
	}
	mock := gsTesting.NewMockDoer(t,
		gsTesting.NewMockResponse(t, data),
	)

	driver := tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))

	var (
		ctx        = context.Background()
		predicates []goPredicate.Predicate
		elseOps    []goOperation.Operation

		ifOps = []goOperation.Operation{
			goOperation.Get([]byte("/123")),
		}
	)

	resp, err := driver.Execute(ctx, predicates, ifOps, elseOps)
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Results, 1)
	require.Len(t, resp.Results[0].Values, 1)
	require.Equal(t, []byte("/123"), resp.Results[0].Values[0].Key)
	require.Equal(t, []byte("123"), resp.Results[0].Values[0].Value)
	require.Equal(t, int64(1000), resp.Results[0].Values[0].ModRevision)
}

func TestDriver_Execute_Get_PrefixMulti(t *testing.T) {
	t.Parallel()

	data := []any{
		map[string]any{
			"data": map[string]any{
				"responses": [][]any{
					{},
					{
						map[string]any{
							"path":         []byte("/123/1"),
							"value":        []byte("124"),
							"mod_revision": 1000,
						},
						map[string]any{
							"path":         []byte("/123/2"),
							"value":        []byte("125"),
							"mod_revision": 900,
						},
					},
					{
						map[string]any{
							"path":         []byte("/120"),
							"value":        []byte("121"),
							"mod_revision": 800,
						},
					},
					{},
				},
				"is_success": true,
			},
			"revision": 1000,
		},
	}
	mock := gsTesting.NewMockDoer(t,
		gsTesting.NewMockResponse(t, data),
	)

	driver := tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))

	var (
		ctx        = context.Background()
		predicates []goPredicate.Predicate
		elseOps    []goOperation.Operation

		ifOps = []goOperation.Operation{
			goOperation.Get([]byte("/122/")),
			goOperation.Get([]byte("/123/")),
			goOperation.Get([]byte("/120")),
			goOperation.Get([]byte("/121")),
		}
	)

	resp, err := driver.Execute(ctx, predicates, ifOps, elseOps)
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Results, 4)
	require.Empty(t, resp.Results[0].Values)
	require.Len(t, resp.Results[1].Values, 2)
	require.Len(t, resp.Results[2].Values, 1)
	require.Empty(t, resp.Results[3].Values)

	assert.Equal(t, []byte("/123/1"), resp.Results[1].Values[0].Key)
	assert.Equal(t, []byte("124"), resp.Results[1].Values[0].Value)
	assert.Equal(t, int64(1000), resp.Results[1].Values[0].ModRevision)

	assert.Equal(t, []byte("/123/2"), resp.Results[1].Values[1].Key)
	assert.Equal(t, []byte("125"), resp.Results[1].Values[1].Value)
	assert.Equal(t, int64(900), resp.Results[1].Values[1].ModRevision)

	assert.Equal(t, []byte("/120"), resp.Results[2].Values[0].Key)
	assert.Equal(t, []byte("121"), resp.Results[2].Values[0].Value)
	assert.Equal(t, int64(800), resp.Results[2].Values[0].ModRevision)
}

func TestDriver_Execute_Delete(t *testing.T) {
	t.Parallel()

	data := []any{
		map[string]any{
			"data": map[string]any{
				"responses":  [][]any{{}},
				"is_success": true,
			},
			"revision": 1000,
		},
	}
	mock := gsTesting.NewMockDoer(t,
		gsTesting.NewMockResponse(t, data),
	)

	driver := tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))

	var (
		ctx        = context.Background()
		predicates []goPredicate.Predicate
		elseOps    []goOperation.Operation

		ifOps = []goOperation.Operation{
			goOperation.Delete([]byte("/123")),
		}
	)

	resp, err := driver.Execute(ctx, predicates, ifOps, elseOps)
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Results, 1)
	require.Empty(t, resp.Results[0].Values)
}

func TestDriver_Execute_Put(t *testing.T) {
	t.Parallel()

	data := []any{
		map[string]any{
			"data": map[string]any{
				"responses":  [][]any{{}},
				"is_success": true,
			},
			"revision": 1000,
		},
	}
	mock := gsTesting.NewMockDoer(t,
		gsTesting.NewMockResponse(t, data),
	)

	driver := tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))

	var (
		ctx        = context.Background()
		predicates []goPredicate.Predicate
		elseOps    []goOperation.Operation

		ifOps = []goOperation.Operation{
			goOperation.Put([]byte("/123"), []byte("123")),
		}
	)

	resp, err := driver.Execute(ctx, predicates, ifOps, elseOps)
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Results, 1)
	require.Empty(t, resp.Results[0].Values)
}

func TestDriver_Execute_InvalidBody(t *testing.T) {
	t.Parallel()

	data := []any{
		map[string]any{
			"data": map[string]any{
				"responses":  [][]any{{}},
				"is_success": true,
			},
			"revision": 1000,
		},
		map[string]any{
			"data": map[string]any{
				"responses":  [][]any{{}},
				"is_success": true,
			},
			"revision": 1000,
		},
	}
	mock := gsTesting.NewMockDoer(t,
		gsTesting.NewMockResponse(t, data),
	)

	driver := tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))

	var (
		ctx        = context.Background()
		predicates []goPredicate.Predicate
		elseOps    []goOperation.Operation

		ifOps = []goOperation.Operation{
			goOperation.Delete([]byte("/123")),
		}
	)

	_, err := driver.Execute(ctx, predicates, ifOps, elseOps)
	require.Error(t, err)
	require.ErrorIs(t, err, tcs.ErrUnexpectedResponse)
}

func TestDriver_Execute_WithPredicatesAndElse(t *testing.T) {
	t.Parallel()

	data := []any{
		map[string]any{
			"data": map[string]any{
				"responses":  [][]any{{}},
				"is_success": false,
			},
			"revision": 1000,
		},
	}
	mock := gsTesting.NewMockDoer(t,
		gsTesting.NewMockResponse(t, data),
	)

	driver := tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))

	var (
		ctx        = context.Background()
		predicates = []goPredicate.Predicate{
			goPredicate.VersionLess([]byte("/123"), 100),
			goPredicate.VersionGreater([]byte("/123"), 100),
			goPredicate.VersionEqual([]byte("/123"), 100),
			goPredicate.VersionNotEqual([]byte("/123"), 100),
			goPredicate.ValueEqual([]byte("/123"), []byte("123")),
			goPredicate.ValueNotEqual([]byte("/123"), []byte("123")),
		}
		ifOps = []goOperation.Operation{
			goOperation.Delete([]byte("/123")),
		}

		elseOps = []goOperation.Operation{
			goOperation.Put([]byte("/123"), []byte("123")),
		}
	)

	resp, err := driver.Execute(ctx, predicates, ifOps, elseOps)
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Results, 1)
	require.Empty(t, resp.Results[0].Values)
}
