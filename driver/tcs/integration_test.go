package tcs_test

import (
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
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	tcshelper "github.com/tarantool/go-tarantool/v2/test_helpers/tcs"

	"github.com/tarantool/go-storage/driver/tcs"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
)

var (
	haveTCS      bool     //nolint: gochecknoglobals
	tcsEndpoints []string //nolint: gochecknoglobals
)

// createTestDriver creates a TCS driver for testing.
// It skips the test if no Tarantool instance is available.
func createTestDriver(ctx context.Context, t *testing.T) (*tcs.Driver, func()) {
	t.Helper()

	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
	}

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	// Create connection pool.
	instances := make([]pool.Instance, 0, len(tcsEndpoints))
	for i, addr := range tcsEndpoints {
		instances = append(instances, pool.Instance{
			Name: string(rune('a' + i)),
			Dialer: &tarantool.NetDialer{
				Address:  addr,
				User:     "client",
				Password: "secret",
				RequiredProtocolInfo: tarantool.ProtocolInfo{
					Auth:     0,
					Version:  0,
					Features: nil,
				},
			},
			Opts: tarantool.Opts{
				Timeout:       0,
				Reconnect:     0,
				MaxReconnects: 0,
				RateLimit:     0,
				RLimitAction:  0,
				Concurrency:   0,
				SkipSchema:    false,
				Notify:        nil,
				Handle:        nil,
				Logger:        nil,
			},
		})
	}

	conn, err := pool.Connect(ctx, instances)
	require.NoError(t, err, "Failed to connect to Tarantool pool")

	// Wrap the pool connection to implement DoerWatcher.
	wrapper := pool.NewConnectorAdapter(conn, pool.RW)

	return tcs.New(wrapper), func() { _ = wrapper.Close() }
}

// cleanupTestKey deletes a test key to ensure clean state.
func cleanupTestKey(ctx context.Context, driver *tcs.Driver, key []byte) {
	_, _ = driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
}

// testKey generates a unique test key to avoid conflicts between tests.
func testKey(t *testing.T, prefix string) []byte {
	t.Helper()

	return []byte("/test/" + prefix + "/" + t.Name())
}

func TestTCSDriver_Put(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "put")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("put-test-value")

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Empty(t, response.Results[0].Values, "Put operation should not return any values in response")
}

func TestTCSDriver_Get(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "get")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("get-test-value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "Get operation should return one result")
	assert.Len(t, response.Results[0].Values, 1, "Get operation should return one value in response")
	assert.Equal(t, key, response.Results[0].Values[0].Key, "Returned key should match requested key")
	assert.Equal(t, value, response.Results[0].Values[0].Value, "Returned value should match stored value")
	assert.Positive(t, response.Results[0].Values[0].ModRevision, "ModRevision should be greater than 0")
}

func TestTCSDriver_Delete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("delete-test-value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Equal(t, key, response.Results[0].Values[0].Key, "Returned key should match deleted key")
	assert.Equal(t, value, response.Results[0].Values[0].Value, "Returned value should match deleted value")
	assert.Positive(t, response.Results[0].Values[0].ModRevision, "ModRevision should be greater than 0")
}

func TestTCSDriver_DeletePrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("delete-test-value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(append(key, []byte("/obj1")...), value),
	}, nil)
	require.NoError(t, err)

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(append(key, []byte("/obj2")...), value),
	}, nil)
	require.NoError(t, err)

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

func TestTCSDriver_GetAfterDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "get-after-delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("get-after-delete-test-value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Len(t, response.Results[0].Values, 1, "Delete operation should return one value in response")

	response, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Empty(t, response.Results[0].Values, "Get operation on deleted key should return empty values")
}

func TestTCSDriver_ValueEqualPredicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "value-equal")
	defer cleanupTestKey(ctx, driver, key)

	initialValue := []byte("initial")
	updatedValue := []byte("updated")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, initialValue),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueEqual(key, "initial"),
	}, []operation.Operation{
		operation.Put(key, updatedValue),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Should succeed when value matches")

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Empty(t, response.Results[0].Values, "Put operation should not return any values in response")
}

func TestTCSDriver_ValueNotEqualPredicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "value-not-equal")
	defer cleanupTestKey(ctx, driver, key)

	initialValue := []byte("initial")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, initialValue),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueNotEqual(key, "different"),
	}, []operation.Operation{
		operation.Put(key, []byte("new-value")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Should succeed when value doesn't match")

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Empty(t, response.Results[0].Values, "Put operation should not return any values in response")
}

func TestTCSDriver_VersionEqualPredicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-equal")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("version-test")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)

	initialRevision := getRevisionFromResponse(t, response, 0)

	response, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionEqual(key, initialRevision),
	}, []operation.Operation{
		operation.Put(key, []byte("updated")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Should succeed when version matches")
}

func TestTCSDriver_VersionGreaterPredicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-greater")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("version-test")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)

	initialRevision := getRevisionFromResponse(t, response, 0)

	response, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionGreater(key, initialRevision-1),
	}, []operation.Operation{
		operation.Put(key, []byte("new-value")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Should succeed when version is greater than specified version")
}

func TestTCSDriver_MultipleKeysPut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key1 := testKey(t, "multi-put1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "multi-put2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 2, "TX should return two results")

	for i := range response.Results {
		assert.Empty(t, response.Results[i].Values, "Put operation %d should not return any values in response", i)
	}
}

func TestTCSDriver_MultiplePredicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key1 := testKey(t, "multi-pred1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "multi-pred2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueEqual(key1, "value1"),
		predicate.ValueEqual(key2, "value2"),
	}, []operation.Operation{
		operation.Put(key1, []byte("updated1")),
		operation.Put(key2, []byte("updated2")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Transaction with multiple predicates should succeed")

	require.Len(t, response.Results, 2, "Transaction with multiple predicates should return two results")

	for i := range response.Results {
		assert.Empty(t, response.Results[i].Values, "Operation %d should not return any values in response", i)
	}
}

func TestTCSDriver_MultipleOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key1 := testKey(t, "multi-op1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "multi-op2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, []byte("updated1")),
		operation.Put(key2, []byte("updated2")),
		operation.Get(key1),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Transaction with multiple operations should succeed")

	require.Len(t, response.Results, 3, "Transaction with multiple operations should return three results")
	assert.Equal(t, key1, response.Results[2].Values[0].Key, "Get operation should return the correct key")
	assert.Equal(t, []byte("updated1"), response.Results[2].Values[0].Value,
		"Get operation should return the updated value")
	assert.Positive(t, response.Results[2].Values[0].ModRevision,
		"Get operation should return a valid mod_revision")
}

func TestTCSDriver_ElseOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key1 := testKey(t, "else-op1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "else-op2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueEqual(key1, "wrong-value"),
	}, []operation.Operation{
		operation.Put(key1, []byte("should-not-execute")),
	}, []operation.Operation{
		operation.Delete(key1),
		operation.Delete(key2),
	})
	require.NoError(t, err)
	assert.False(t, response.Succeeded, "Transaction should fail when predicates don't match")

	require.Len(t, response.Results, 2, "Transaction with else operations should return two results")
	assert.Equal(t, key1, response.Results[0].Values[0].Key, "Delete operation should delete key1")
	assert.Equal(t, key2, response.Results[1].Values[0].Key, "Delete operation should delete key2")
}

func TestTCSDriver_WatchPutEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "watch-put")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("watch-test-value")

	watchCtx, cancel := context.WithTimeout(ctx, defaultWaitTimeout)
	defer cancel()

	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	require.NoError(t, err)

	defer stopWatch()

	time.Sleep(500 * time.Millisecond)

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Prefix)
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for put event")
	}
}

func TestTCSDriver_WatchDeleteEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "watch-delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("watch-test-value")

	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	require.NoError(t, err)

	defer stopWatch()

	time.Sleep(500 * time.Millisecond)

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	select {
	case <-eventCh:
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for initial put event")
	}

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)

	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Prefix)
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for delete event")
	}
}

func TestTCSDriver_GetByPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	basePrefix := "/test/prefix-test/" + t.Name() + "/"

	key1 := []byte(basePrefix + "key1")
	key2 := []byte(basePrefix + "key2")
	key3 := []byte(basePrefix + "key3")
	key4 := []byte("/test/other-prefix/other-key")

	defer cleanupTestKey(ctx, driver, key1)
	defer cleanupTestKey(ctx, driver, key2)
	defer cleanupTestKey(ctx, driver, key3)
	defer cleanupTestKey(ctx, driver, key4)

	value1 := []byte("prefix-value1")
	value2 := []byte("prefix-value2")
	value3 := []byte("prefix-value3")
	value4 := []byte("other-value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
		operation.Put(key3, value3),
		operation.Put(key4, value4),
	}, nil)
	require.NoError(t, err)

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get([]byte(basePrefix)),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "Get operation should return one result")

	assert.Len(t, response.Results[0].Values, 3, "Get by prefix should return three values matching the prefix")

	for _, kv := range response.Results[0].Values {
		assert.True(t, strings.HasPrefix(string(kv.Key), basePrefix),
			"Returned key %s should have prefix %s", string(kv.Key), basePrefix)
		assert.Positive(t, kv.ModRevision, "ModRevision should be greater than 0")
	}

	foundKeys := make(map[string][]byte)
	for _, kv := range response.Results[0].Values {
		foundKeys[string(kv.Key)] = kv.Value
	}

	assert.Equal(t, value1, foundKeys[string(key1)], "Should find value1 for key1")
	assert.Equal(t, value2, foundKeys[string(key2)], "Should find value2 for key2")
	assert.Equal(t, value3, foundKeys[string(key3)], "Should find value3 for key3")

	_, exists := foundKeys[string(key4)]
	assert.False(t, exists, "Key with different prefix should not be included in results")
}

func TestTCSDriver_GetNonExistentKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := []byte("/test/nonexistent/key")

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err, "Get operation should not fail for non-existent key")
	assert.True(t, response.Succeeded, "Get operation should succeed")
}

func TestTCSDriver_ErrConnect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	instances := []pool.Instance{
		{
			Name: "a",
			Dialer: &tarantool.NetDialer{
				Address:  "10.0.0.1:65534",
				User:     "client",
				Password: "secret",
				RequiredProtocolInfo: tarantool.ProtocolInfo{
					Auth:     0,
					Version:  0,
					Features: nil,
				},
			},
			Opts: tarantool.Opts{
				Timeout:       10 * time.Millisecond,
				Reconnect:     1,
				MaxReconnects: 1,
				RateLimit:     0,
				RLimitAction:  0,
				Concurrency:   0,
				SkipSchema:    false,
				Notify:        nil,
				Handle:        nil,
				Logger:        nil,
			},
		},
	}

	conn, err := pool.Connect(ctx, instances)
	require.NoError(t, err, "failed to connect to Tarantool pool")

	wrapper := pool.NewConnectorAdapter(conn, pool.RW)

	defer func() { _ = wrapper.Close() }()

	driver := tcs.New(wrapper)
	require.NotEmpty(t, driver)

	key := []byte("/test/nonexistent/key")

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.Error(t, err, "Get operation should fail with connection failed")
	assert.ErrorIs(t, err, pool.ErrNoRwInstance)
}

func TestTCSDriver_GetRoot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get([]byte("/")),
	}, nil)

	require.NoError(t, err, "Get root operation should not fail")
	assert.True(t, response.Succeeded, "Get root operation should succeed")
}

// getRevisionFromResponse extracts revision from transaction response.
// This extracts the revision from the tx.Response structure.
func getRevisionFromResponse(t *testing.T, response tx.Response, position int) int64 {
	t.Helper()

	require.Greater(t, len(response.Results), position, "expected at least %d results", position+1)
	require.NotEmpty(t, response.Results[position].Values, "expected at least %d values", position+1)

	return response.Results[position].Values[0].ModRevision
}

func TestMain(m *testing.M) {
	flag.Parse()

	tcsInstance, err := tcshelper.Start(0)
	switch {
	case errors.Is(err, tcshelper.ErrNotSupported):
		fmt.Println("TcS is not supported:", err) //nolint:forbidigo
	case err != nil:
		fmt.Println("Failed to start TCS:", err) //nolint:forbidigo
	default:
		haveTCS = true
		tcsEndpoints = tcsInstance.Endpoints()
	}

	os.Exit(func() int {
		defer func() {
			if haveTCS {
				tcsInstance.Stop()
			}
		}()

		return m.Run()
	}())
}
