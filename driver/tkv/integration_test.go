package tkv_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"

	"github.com/tarantool/go-storage/driver/tkv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
)

// skipIfNoTarantool skips the test if no Tarantool instance is available.
func skipIfNoTarantool(t *testing.T) {
	t.Helper()

	if os.Getenv("TARANTOOL_ADDR") == "" {
		t.Skip("Skipping test: TARANTOOL_ADDR environment variable not set")
	}
}

// createTestDriver creates a TKV driver for testing.
// It skips the test if no Tarantool instance is available.
func createTestDriver(ctx context.Context, t *testing.T) *tkv.Driver {
	t.Helper()

	skipIfNoTarantool(t)

	addrs := []string{}

	// Parse comma-separated addresses.
	addr := os.Getenv("TARANTOOL_ADDR")
	if addr != "" {
		// Split by comma and trim spaces.
		for _, a := range strings.Split(addr, ",") {
			addrs = append(addrs, strings.TrimSpace(a))
		}
	}

	// Create connection pool.
	instances := make([]pool.Instance, 0, len(addrs))
	for i, addr := range addrs {
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

	return tkv.New(wrapper)
}

// cleanupTestKey deletes a test key to ensure clean state.
func cleanupTestKey(ctx context.Context, driver *tkv.Driver, key []byte) {
	_, _ = driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
}

// testKey generates a unique test key to avoid conflicts between tests.
func testKey(t *testing.T, prefix string) []byte {
	t.Helper()

	return []byte("/test/" + prefix + "/" + t.Name())
}

// TestTKVDriver_Put tests basic Put operation.
func TestTKVDriver_Put(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "put")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("put-test-value")

	// Put operation.
	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Empty(t, response.Results[0].Values, "Put operation should not return any values in response")
}

// TestTKVDriver_Get tests basic Get operation.
func TestTKVDriver_Get(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "get")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("get-test-value")

	// First put a value.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	// Get operation.
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

// TestTKVDriver_Delete tests basic Delete operation.
func TestTKVDriver_Delete(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("delete-test-value")

	// First put a value.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	// Delete operation.
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

// TestTKVDriver_GetAfterDelete tests Get operation after Delete.
func TestTKVDriver_GetAfterDelete(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "get-after-delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("get-after-delete-test-value")

	// Put a value.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	// Delete the value.
	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Len(t, response.Results[0].Values, 1, "Delete operation should return one value in response")

	// Get after delete - should succeed but return empty data.
	response, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "TX should return one result")
	assert.Empty(t, response.Results[0].Values, "Get operation on deleted key should return empty values")
}

// TestTKVDriver_ValueEqualPredicate tests ValueEqual predicate functionality.
func TestTKVDriver_ValueEqualPredicate(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "value-equal")
	defer cleanupTestKey(ctx, driver, key)

	initialValue := []byte("initial")
	updatedValue := []byte("updated")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, initialValue),
	}, nil)
	require.NoError(t, err)

	// Test ValueEqual predicate - should succeed when value matches.
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

// TestTKVDriver_ValueNotEqualPredicate tests ValueNotEqual predicate functionality.
func TestTKVDriver_ValueNotEqualPredicate(t *testing.T) {
	t.Parallel()
	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "value-not-equal")
	defer cleanupTestKey(ctx, driver, key)

	initialValue := []byte("initial")

	// Setup: put initial value.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, initialValue),
	}, nil)
	require.NoError(t, err)

	// Test ValueNotEqual predicate - should succeed when value doesn't match.
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

// TestTKVDriver_VersionEqualPredicate tests VersionEqual predicate functionality.
func TestTKVDriver_VersionEqualPredicate(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "version-equal")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("version-test")

	// Put initial value and get its revision.
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

// TestTKVDriver_VersionGreaterPredicate tests VersionGreater predicate functionality.
func TestTKVDriver_VersionGreaterPredicate(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "version-greater")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("version-test")

	// Put initial value and get its revision.
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

// TestTKVDriver_MultipleKeysPut tests putting multiple keys in one operation.
func TestTKVDriver_MultipleKeysPut(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key1 := testKey(t, "multi-put1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "multi-put2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put multiple keys in one operation.
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

// TestTKVDriver_MultiplePredicates tests transaction with multiple predicates.
func TestTKVDriver_MultiplePredicates(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key1 := testKey(t, "multi-pred1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "multi-pred2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	// Setup: put initial values.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
	}, nil)
	require.NoError(t, err)

	// Transaction with multiple predicates.
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

// TestTKVDriver_MultipleOperations tests transaction with multiple operations.
func TestTKVDriver_MultipleOperations(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key1 := testKey(t, "multi-op1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "multi-op2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	// Setup: put initial values.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
	}, nil)
	require.NoError(t, err)

	// Transaction with multiple operations.
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

// TestTKVDriver_ElseOperations tests transaction with else operations.
func TestTKVDriver_ElseOperations(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key1 := testKey(t, "else-op1")
	defer cleanupTestKey(ctx, driver, key1)

	key2 := testKey(t, "else-op2")
	defer cleanupTestKey(ctx, driver, key2)

	value1 := []byte("value1")
	value2 := []byte("value2")

	// Setup: put initial values.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
	}, nil)
	require.NoError(t, err)

	// Transaction with predicates that should fail, triggering else operations.
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

// TestTKVDriver_WatchPutEvent tests watch functionality for Put events.
func TestTKVDriver_WatchPutEvent(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "watch-put")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("watch-test-value")

	// Create a context with timeout.
	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Start watching before making changes.
	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	require.NoError(t, err)

	defer stopWatch()

	// Give watcher time to register.
	time.Sleep(500 * time.Millisecond)

	// Trigger put event.
	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	// Wait for put event.
	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Prefix)
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for put event")
	}
}

// TestTKVDriver_WatchDeleteEvent tests watch functionality for Delete events.
func TestTKVDriver_WatchDeleteEvent(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := testKey(t, "watch-delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("watch-test-value")

	// Create a context with timeout.
	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Start watching before making changes.
	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	require.NoError(t, err)

	defer stopWatch()

	// Give watcher time to register.
	time.Sleep(500 * time.Millisecond)

	// Put initial value first.
	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	// Wait for initial put event.
	select {
	case <-eventCh:
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for initial put event")
	}

	// Trigger delete event.
	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)

	// Wait for delete event.
	select {
	case event := <-eventCh:
		assert.Equal(t, key, event.Prefix)
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for delete event")
	}
}

// TestTKVDriver_GetByPrefix tests Get operation with prefix to retrieve multiple values.
func TestTKVDriver_GetByPrefix(t *testing.T) {
	t.Parallel()

	skipIfNoTarantool(t)

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	// Create a common prefix for all test keys.
	basePrefix := "/test/prefix-test/" + t.Name() + "/"

	// Create multiple keys with the same prefix.
	key1 := []byte(basePrefix + "key1")
	key2 := []byte(basePrefix + "key2")
	key3 := []byte(basePrefix + "key3")
	key4 := []byte("/test/other-prefix/other-key") // Different prefix for comparison.

	defer cleanupTestKey(ctx, driver, key1)
	defer cleanupTestKey(ctx, driver, key2)
	defer cleanupTestKey(ctx, driver, key3)
	defer cleanupTestKey(ctx, driver, key4)

	value1 := []byte("prefix-value1")
	value2 := []byte("prefix-value2")
	value3 := []byte("prefix-value3")
	value4 := []byte("other-value")

	// Put multiple values with the same prefix and one with different prefix.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, value1),
		operation.Put(key2, value2),
		operation.Put(key3, value3),
		operation.Put(key4, value4),
	}, nil)
	require.NoError(t, err)

	// Get operation with prefix - should return all keys matching the prefix.
	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get([]byte(basePrefix)),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded)

	require.Len(t, response.Results, 1, "Get operation should return one result")

	// Should return 3 values matching the prefix.
	assert.Len(t, response.Results[0].Values, 3, "Get by prefix should return three values matching the prefix")

	// Verify all returned keys have the correct prefix.
	for _, kv := range response.Results[0].Values {
		assert.True(t, strings.HasPrefix(string(kv.Key), basePrefix),
			"Returned key %s should have prefix %s", string(kv.Key), basePrefix)
		assert.Positive(t, kv.ModRevision, "ModRevision should be greater than 0")
	}

	// Verify specific values are present.
	foundKeys := make(map[string][]byte)
	for _, kv := range response.Results[0].Values {
		foundKeys[string(kv.Key)] = kv.Value
	}

	assert.Equal(t, value1, foundKeys[string(key1)], "Should find value1 for key1")
	assert.Equal(t, value2, foundKeys[string(key2)], "Should find value2 for key2")
	assert.Equal(t, value3, foundKeys[string(key3)], "Should find value3 for key3")

	// Verify key with different prefix is not included.
	_, exists := foundKeys[string(key4)]
	assert.False(t, exists, "Key with different prefix should not be included in results")
}

// TestTKVDriver_GetNonExistentKey tests Get operation on non-existent key.
func TestTKVDriver_GetNonExistentKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	key := []byte("/test/nonexistent/key")

	// Test Get operation on non-existent key.
	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err, "Get operation should not fail for non-existent key")
	assert.True(t, response.Succeeded, "Get operation should succeed")
}

// TestTKVDriver_GetRoot tests getting all keys (root path).
func TestTKVDriver_GetRoot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := createTestDriver(ctx, t)

	// Test getting all keys (root path).
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
