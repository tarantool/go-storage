package etcd //nolint:testpackage

import (
	"context"
	"testing"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/internal/mocks"
)

func TestDriver_Watch_CancelClosesEventChannel(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	client := mocks.NewEtcdClientMock(mc)

	watchCh := make(chan etcd.WatchResponse)

	client.WatchMock.Set(func(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
		return watchCh
	})

	driver := New(client)

	eventCh, cancelWatch, err := driver.Watch(context.Background(), []byte("test-key"))
	require.NoError(t, err)

	cancelWatch()

	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "event channel should be closed after cancel")
	case <-time.After(time.Second):
		t.Fatal("event channel should be closed after cancel")
	}
}

func TestDriver_Watch_CancelDoesNotCallClientClose(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	client := mocks.NewEtcdClientMock(mc)

	watchCh := make(chan etcd.WatchResponse)

	client.WatchMock.Set(func(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
		return watchCh
	})

	driver := New(client)

	eventCh, cancelWatch, err := driver.Watch(context.Background(), []byte("test-key"))
	require.NoError(t, err)

	cancelWatch()

	select {
	case _, ok := <-eventCh:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("event channel should be closed")
	}

	assert.Equal(t, uint64(1), client.WatchAfterCounter(), "Watch should be called once")
}

func TestDriver_Watch_ContextCancellationClosesEventChannel(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	client := mocks.NewEtcdClientMock(mc)

	watchCh := make(chan etcd.WatchResponse)

	client.WatchMock.Set(func(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
		return watchCh
	})

	driver := New(client)

	ctx, cancel := context.WithCancel(context.Background())

	eventCh, cancelWatch, err := driver.Watch(ctx, []byte("test-key"))
	require.NoError(t, err)

	defer cancelWatch()

	cancel()

	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "event channel should be closed after context cancellation")
	case <-time.After(time.Second):
		t.Fatal("event channel should be closed after context cancellation")
	}
}

func TestDriver_Watch_EventsForwarded(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	client := mocks.NewEtcdClientMock(mc)

	watchCh := make(chan etcd.WatchResponse, 1)

	client.WatchMock.Set(func(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
		return watchCh
	})

	driver := New(client)
	key := []byte("test-key")

	eventCh, cancelWatch, err := driver.Watch(context.Background(), key)
	require.NoError(t, err)

	defer cancelWatch()

	watchCh <- etcd.WatchResponse{ //nolint:exhaustruct
		Events: []*etcd.Event{
			{Type: etcd.EventTypePut},
		},
	}

	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Prefix)
	case <-time.After(time.Second):
		t.Fatal("expected event to be forwarded")
	}
}

func TestDriver_Watch_PrefixKeyAddsWithPrefixOption(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	client := mocks.NewEtcdClientMock(mc)

	watchCh := make(chan etcd.WatchResponse)

	var capturedOpts []etcd.OpOption

	client.WatchMock.Set(func(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
		capturedOpts = opts
		return watchCh
	})

	driver := New(client)

	_, cancelWatch, err := driver.Watch(context.Background(), []byte("test-prefix/"))
	require.NoError(t, err)

	defer cancelWatch()

	assert.NotEmpty(t, capturedOpts, "prefix key should add WithPrefix option")
}

func TestDriver_Watch_NonPrefixKeyNoWithPrefixOption(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	client := mocks.NewEtcdClientMock(mc)

	watchCh := make(chan etcd.WatchResponse)

	var capturedOpts []etcd.OpOption

	client.WatchMock.Set(func(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
		capturedOpts = opts
		return watchCh
	})

	driver := New(client)

	_, cancelWatch, err := driver.Watch(context.Background(), []byte("test-key"))
	require.NoError(t, err)

	defer cancelWatch()

	assert.Empty(t, capturedOpts, "non-prefix key should not add WithPrefix option")
}
