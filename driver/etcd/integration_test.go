// Package etcd_test provides integration tests for the etcd driver.
// These tests require a running etcd instance and test full functionality.
//
// Due to inability to start multiple LazyClusters - we're using one LazyCluster
// and won't start tests in parallel here.
//
//nolint:paralleltest
package etcd_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdclient "go.etcd.io/etcd/client/v3"
	etcdfintegration "go.etcd.io/etcd/tests/v3/framework/integration"

	etcddriver "github.com/tarantool/go-storage/driver/etcd"
	"github.com/tarantool/go-storage/internal/testing/etcd"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
)

const (
	defaultWaitTimeout = 5 * time.Second
	testDialTimeout    = 5 * time.Second
)

// createTestDriver creates an etcd driver for testing using the integration framework.
// Returns driver and cleanup function for simple test scenarios.
func createTestDriver(t *testing.T) (*etcddriver.Driver, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	etcdfintegration.BeforeTest(etcd.NewSilentTB(t), etcdfintegration.WithoutGoLeakDetection())

	cluster := etcd.NewLazyCluster()

	t.Cleanup(func() { cluster.Terminate() })

	endpoints := cluster.EndpointsGRPC()

	client, err := etcdclient.New(etcdclient.Config{
		Endpoints:   endpoints,
		DialTimeout: testDialTimeout,

		AutoSyncInterval:      0,
		DialKeepAliveTime:     0,
		DialKeepAliveTimeout:  0,
		MaxCallSendMsgSize:    0,
		MaxCallRecvMsgSize:    0,
		TLS:                   nil,
		Username:              "",
		Password:              "",
		RejectOldCluster:      false,
		DialOptions:           nil,
		Context:               nil,
		Logger:                nil,
		LogConfig:             nil,
		PermitWithoutStream:   false,
		MaxUnaryRetries:       0,
		BackoffWaitBetween:    0,
		BackoffJitterFraction: 0,
	})
	require.NoError(t, err, "Failed to create etcd client")
	t.Cleanup(func() { _ = client.Close() })

	driver := etcddriver.New(client)

	return driver, func() {}
}

// testKey generates a unique test key to avoid conflicts between tests.
func testKey(t *testing.T, prefix string) []byte {
	t.Helper()

	return []byte("/" + t.Name() + "/" + prefix)
}

func testNestedKey(t *testing.T, prefix, suffix string) []byte {
	t.Helper()

	return []byte("/" + strings.Join([]string{t.Name(), prefix, suffix}, "/"))
}

// putValue is a helper that puts a key-value pair and fails the test on error.
func putValue(ctx context.Context, t *testing.T, driver *etcddriver.Driver, key, value []byte) {
	t.Helper()

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err, "Put operation failed")
	assert.True(t, response.Succeeded, "Put operation should succeed")
}

// getValue is a helper that gets a value and returns the key-value pair.
func getValue(ctx context.Context, t *testing.T, driver *etcddriver.Driver, key []byte) kv.KeyValue {
	t.Helper()

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err, "Get operation failed")
	assert.True(t, response.Succeeded, "Get operation should succeed")
	require.Len(t, response.Results, 1, "Get operation should return one result")
	require.Len(t, response.Results[0].Values, 1, "Get operation should return one value")

	return response.Results[0].Values[0]
}

// deleteValue is a helper that deletes a key and returns the deleted key-value pair.
func deleteValue(ctx context.Context, t *testing.T, driver *etcddriver.Driver, key []byte) kv.KeyValue {
	t.Helper()

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)

	require.NoError(t, err, "Delete operation failed")
	assert.True(t, response.Succeeded, "Delete operation should succeed")
	require.Len(t, response.Results, 1, "Delete operation should return one result")
	require.Len(t, response.Results[0].Values, 1, "Delete operation should return one value")

	return response.Results[0].Values[0]
}

func TestEtcdDriver_Put(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "put")
	value := []byte("put-test-value")

	putValue(ctx, t, driver, key, value)
}

func TestEtcdDriver_Get(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "get")
	value := []byte("get-test-value")

	putValue(ctx, t, driver, key, value)

	retrievedKv := getValue(ctx, t, driver, key)

	assert.Equal(t, key, retrievedKv.Key, "Returned key should match requested key")
	assert.Equal(t, value, retrievedKv.Value, "Returned value should match stored value")
	assert.Positive(t, retrievedKv.ModRevision, "ModRevision should be greater than 0")
}

func TestEtcdDriver_GetPrefix(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "get")
	value := []byte("get-test-value")

	putValue(ctx, t, driver, append(key, []byte("/123")...), value)
	putValue(ctx, t, driver, append(key, []byte("/124")...), value)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(append(key, []byte("/")...), operation.Option{WithPrefix: true}),
	}, nil)
	require.NoError(t, err, "Get operation failed")
	assert.True(t, response.Succeeded, "Get operation should succeed")
	require.Len(t, response.Results, 1, "Get operation should return one result")
	require.Len(t, response.Results[0].Values, 2, "Get operation should return one value")
}

func TestEtcdDriver_Delete(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "delete")
	value := []byte("delete-test-value")

	putValue(ctx, t, driver, key, value)

	deletedKv := deleteValue(ctx, t, driver, key)

	assert.Equal(t, key, deletedKv.Key, "Returned key should match deleted key")
	assert.Equal(t, value, deletedKv.Value, "Returned value should match deleted value")
	assert.Positive(t, deletedKv.ModRevision, "ModRevision should be greater than 0")
}

func TestEtcdDriver_DeletePrefix(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "delete")
	value := []byte("delete-test-value")

	putValue(ctx, t, driver, append(key, []byte("/obj1")...), value)
	putValue(ctx, t, driver, append(key, []byte("/obj2")...), value)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(append(key, []byte("/")...), operation.Option{WithPrefix: true}),
	}, nil)

	require.NoError(t, err, "Delete operation failed")
	assert.True(t, response.Succeeded, "Delete operation should succeed")

	response, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(append(key, []byte("/obj1")...)),
	}, nil)

	require.NoError(t, err, "Get operation failed")
	require.Empty(t, response.Results[0].Values, "Get operation have any result")

	response, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(append(key, []byte("/obj2")...)),
	}, nil)

	require.NoError(t, err, "Get operation failed")
	require.Empty(t, response.Results[0].Values, "Get operation have any result")
}

func TestEtcdDriver_ValueEqualPredicate(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "value-equal")
	initialValue := []byte("initial")
	updatedValue := []byte("updated")

	putValue(ctx, t, driver, key, initialValue)

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueEqual(key, initialValue),
	}, []operation.Operation{
		operation.Put(key, updatedValue),
	}, nil)
	require.NoError(t, err, "Predicate transaction should succeed")
	assert.True(t, response.Succeeded, "Should succeed when value matches")

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Empty(t, response.Results[0].Values, "Put operation should not return any values in response")
}

func TestEtcdDriver_VersionEqualPredicate(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "version-equal")
	value := []byte("version-test")
	updatedValue := []byte("updated")

	putValue(ctx, t, driver, key, value)

	kvi := getValue(ctx, t, driver, key)
	initialRevision := kvi.ModRevision

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionEqual(key, initialRevision),
	}, []operation.Operation{
		operation.Put(key, updatedValue),
	}, nil)
	require.NoError(t, err, "Predicate transaction should succeed")
	assert.True(t, response.Succeeded, "Should succeed when version matches")
}

func TestEtcdDriver_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "context-cancellation")
	value := []byte("value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.Error(t, err, "Should return error when context is cancelled")
	assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
		"Error should be context cancellation related")
}

func TestEtcdDriver_Watch_SingleKey(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "watch-single")
	initialValue := []byte("initial-value")
	updatedValue := []byte("updated-value")

	putValue(ctx, t, driver, key, initialValue)

	eventCh, cancelWatch, err := driver.Watch(ctx, key)
	require.NoError(t, err, "Watch should not return an error")

	defer cancelWatch()

	putValue(ctx, t, driver, key, updatedValue)

	// Wait for the watch event.
	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Prefix, "Event should contain the watched key")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "Expected watch event but timed out")
	}

	deleteValue(ctx, t, driver, key)

	// Wait for the delete event.
	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Prefix, "Event should contain the watched key")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "Expected watch event for delete but timed out")
	}
}

func TestEtcdDriver_Watch_Prefix(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	prefix := append(testKey(t, "watch-prefix"), '/')

	eventCh, cancelWatch, err := driver.Watch(ctx, prefix)
	require.NoError(t, err, "Watch should not return an error")

	defer cancelWatch()

	key1 := testNestedKey(t, "watch-prefix", "key1")
	key2 := testNestedKey(t, "watch-prefix", "key2")
	value1 := []byte("value1")
	value2 := []byte("value2")

	putValue(ctx, t, driver, key1, value1)
	putValue(ctx, t, driver, key2, value2)

	// Wait for watch events for both puts.
	eventCount := 0
	for eventCount < 2 {
		select {
		case event := <-eventCh:
			assert.Equal(t, prefix, event.Prefix, "Event should contain the watched prefix")

			eventCount++
		case <-time.After(defaultWaitTimeout):
			t.Fatalf("Expected %d watch events but only received %d", 2, eventCount)
		}
	}

	deleteValue(ctx, t, driver, key1)

	// Wait for the delete event.
	select {
	case event := <-eventCh:
		assert.Equal(t, prefix, event.Prefix, "Event should contain the watched prefix")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "Expected watch event for delete but timed out")
	}
}

func TestEtcdDriver_Watch_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "watch-context-cancel")

	eventCh, cancelWatch, err := driver.Watch(ctx, key)
	require.NoError(t, err, "Watch should not return an error")

	defer cancelWatch()

	// Wait for context cancellation and verify channel is closed.
	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "Event channel should be closed after context cancellation")
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Event channel should be closed immediately after context cancellation")
	}
}

func TestEtcdDriver_Watch_CancelWatch(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "watch-cancel")

	eventCh, cancelWatch, err := driver.Watch(ctx, key)
	require.NoError(t, err, "Watch should not return an error")

	cancelWatch()

	time.Sleep(100 * time.Millisecond)

	// Verify channel is closed after cancelling watch.
	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "Event channel should be closed after cancelling watch")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "Event channel should be closed after cancelling watch")
	}
}
