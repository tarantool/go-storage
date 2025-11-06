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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdclient "go.etcd.io/etcd/client/v3"
	etcdfintegration "go.etcd.io/etcd/tests/v3/framework/integration"
	etcdintegration "go.etcd.io/etcd/tests/v3/integration"

	etcddriver "github.com/tarantool/go-storage/driver/etcd"
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

	etcdfintegration.BeforeTest(t, etcdfintegration.WithoutGoLeakDetection())

	cluster := etcdintegration.NewLazyCluster()

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

	return []byte("/test/" + prefix + "/" + t.Name())
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

func TestEtcdDriverWithIntegrationCluster(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := createTestDriver(t)
	defer cleanup()

	t.Run("Basic Operations", func(t *testing.T) {
		key := testKey(t, "basic-operations")
		value := []byte("test-value")

		putValue(ctx, t, driver, key, value)

		retrievedGetKv := getValue(ctx, t, driver, key)

		assert.Equal(t, key, retrievedGetKv.Key, "Returned key should match requested key")
		assert.Equal(t, value, retrievedGetKv.Value, "Returned value should match stored value")
		assert.Positive(t, retrievedGetKv.ModRevision, "ModRevision should be greater than 0")
	})

	t.Run("Conditional Transaction", func(t *testing.T) {
		key := testKey(t, "conditional-txn")
		initialValue := []byte("initial-value")
		updatedValue := []byte("new-value")

		putValue(ctx, t, driver, key, initialValue)

		response, err := driver.Execute(ctx, []predicate.Predicate{}, []operation.Operation{
			operation.Put(key, updatedValue),
		}, []operation.Operation{
			operation.Get(key),
		})
		require.NoError(t, err, "Transaction should succeed")
		assert.True(t, response.Succeeded, "Transaction should succeed")

		retrievedKv := getValue(ctx, t, driver, key)
		assert.Equal(t, updatedValue, retrievedKv.Value, "Value should be updated to new value")
	})

	t.Run("Watch Functionality", func(t *testing.T) {
		key := testKey(t, "watch-functionality")
		value := []byte("watch-value")

		eventCh, cancel, err := driver.Watch(ctx, key)
		require.NoError(t, err, "Watch setup failed")

		defer cancel()

		putValue(ctx, t, driver, key, value)

		select {
		case event := <-eventCh:
			assert.NotNil(t, event, "Should receive watch event")
			assert.Equal(t, key, event.Prefix, "Event prefix should match key")
		case <-time.After(defaultWaitTimeout):
			t.Fatal("Did not receive watch event within timeout")
		}
	})
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

	kv := getValue(ctx, t, driver, key)
	initialRevision := kv.ModRevision

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

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
