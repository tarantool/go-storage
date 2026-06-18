// Package etcd_test provides integration tests for the etcd driver.
// These tests require a running etcd instance and test full functionality.
//
// All Test* functions share a single embedded etcd cluster started in TestMain
// (see sharedEtcdCluster); per-test data isolation is achieved by namespacing
// keys with t.Name(). Sharing one cluster across the whole package avoids the
// ephemeral-port reservation race that surfaces under -race -count=N and
// keeps total cluster boots at one per `go test` invocation.
//
// Example* functions still use their own per-example cluster because some of
// them assert on etcd's global ModRevision counter in `// Output:` blocks.
//
//nolint:paralleltest
package etcd_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-storage/v2/tx"
	etcdclient "go.etcd.io/etcd/client/v3"

	etcddriver "github.com/tarantool/go-storage/v2/driver/etcd"
	"github.com/tarantool/go-storage/v2/kv"
	"github.com/tarantool/go-storage/v2/operation"
	"github.com/tarantool/go-storage/v2/predicate"
	etcdtest "github.com/tarantool/go-storage/v2/test_helpers/etcd"
)

const (
	defaultWaitTimeout = 5 * time.Second
	testDialTimeout    = 5 * time.Second
)

// sharedEtcdCluster is a single embedded etcd reused across every Test* in
// this package. Spinning up one cluster per subtest under `-race -count=N`
// both blows past the 30-minute CI timeout and racily collides on the
// ephemeral port snapshot in etcdtest.freeURL. Sharing reduces total cluster
// boots from N × #tests to one.
//
//nolint:gochecknoglobals
var sharedEtcdCluster *etcdtest.LazyCluster

func TestMain(m *testing.M) {
	flag.Parse()

	sharedEtcdCluster = etcdtest.NewLazyCluster(etcdtest.ClusterConfig{Size: 1}) //nolint:exhaustruct

	os.Exit(func() int {
		defer sharedEtcdCluster.Terminate()

		return m.Run()
	}())
}

// createTestDriver creates an etcd driver for testing using the integration framework.
// It reuses the package-wide sharedEtcdCluster; per-test data isolation is
// the caller's responsibility (tests already namespace their keys by t.Name()).
// Returns driver and cleanup function for simple test scenarios.
func createTestDriver(t *testing.T) (*etcddriver.Driver, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	endpoints := sharedEtcdCluster.EndpointsGRPC()

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

func deletePrefix(ctx context.Context, t *testing.T, driver *etcddriver.Driver, prefix []byte) tx.RequestResponse {
	t.Helper()

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(prefix),
	}, nil)

	require.NoError(t, err, "Delete operation failed")
	assert.True(t, response.Succeeded, "Delete operation should succeed")

	return response.Results[0]
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
		operation.Get(append(key, []byte("/")...)),
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

func TestEtcdDriver_Delete_Prefix(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	keys := make([][]byte, 0, 3)

	prefixKey := testKey(t, "delete/")
	value := []byte("delete-test-value")

	for i := range 3 {
		key := testKey(t, fmt.Sprintf("delete/%d", i))
		putValue(ctx, t, driver, key, value)

		keys = append(keys, key)
	}

	deletedKv := deletePrefix(ctx, t, driver, prefixKey)
	require.Len(t, deletedKv.Values, 3, "Delete by prefix operation should return three value in response")

	for i, val := range deletedKv.Values {
		expectedKey := keys[i]
		require.Equal(t, expectedKey, val.Key, "Returned key should match requested key")
		require.Equal(t, value, val.Value, "Returned value should match stored value")
		require.Positive(t, val.ModRevision, "ModRevision should be greater than 0")
	}
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
		assert.Equal(t, key, event.Key, "Event should contain the watched key")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "Expected watch event but timed out")
	}

	deleteValue(ctx, t, driver, key)

	// Wait for the delete event.
	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Key, "Event should contain the watched key")
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

	expected := bytes.TrimSuffix(prefix, []byte("/"))

	// Wait for watch events for both puts. Each event carries the watched
	// prefix (trailing slash stripped) as a signal, not the changed key.
	for i := range 2 {
		select {
		case event := <-eventCh:
			assert.Equal(t, expected, event.Key, "Event should signal the watched prefix")
		case <-time.After(defaultWaitTimeout):
			t.Fatalf("Expected 2 watch events but only received %d", i)
		}
	}

	deleteValue(ctx, t, driver, key1)

	// Wait for the delete event.
	select {
	case event := <-eventCh:
		assert.Equal(t, expected, event.Key, "Event should signal the watched prefix")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "Expected watch event for delete but timed out")
	}
}

func TestEtcdDriver_Watch_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key := testKey(t, "watch-context-cancel")

	eventCh, cancelWatch, err := driver.Watch(ctx, key)
	require.NoError(t, err, "Watch should not return an error")

	defer cancelWatch()

	// Cancel the outer context and verify the event channel observes it.
	// The wait window is generous because the goal is to confirm the channel
	// closes — not to assert any particular latency.
	cancel()

	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "Event channel should be closed after context cancellation")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "Event channel should be closed after context cancellation")
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

func TestEtcdDriver_Watch_CancelDoesNotAffectOtherWatchers(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	key1 := testKey(t, "watch-1")
	key2 := testKey(t, "watch-2")
	value := []byte("value")

	putValue(ctx, t, driver, key1, value)
	putValue(ctx, t, driver, key2, value)

	eventCh1, cancelWatch1, err := driver.Watch(ctx, key1)
	require.NoError(t, err)

	eventCh2, cancelWatch2, err := driver.Watch(ctx, key2)
	require.NoError(t, err)

	defer cancelWatch2()

	cancelWatch1()

	select {
	case _, ok := <-eventCh1:
		assert.False(t, ok, "first event channel should be closed after cancel")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "first event channel should be closed after cancel")
	}

	updatedValue := []byte("updated")
	putValue(ctx, t, driver, key2, updatedValue)

	select {
	case event := <-eventCh2:
		assert.Equal(t, key2, event.Key, "second watcher should still receive events")
	case <-time.After(defaultWaitTimeout):
		assert.Fail(t, "second watcher should still receive events after first watcher is cancelled")
	}
}
