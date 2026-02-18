package dummy_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/driver/dummy"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
)

const (
	defaultWaitTimeout = 10 * time.Second
)

func createTestDriver(_ context.Context, t *testing.T) (*dummy.Driver, func()) {
	t.Helper()

	return dummy.New(), func() {}
}

func cleanupTestKey(ctx context.Context, driver *dummy.Driver, key []byte) {
	_, _ = driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
}

func testKey(t *testing.T, prefix string) []byte {
	t.Helper()

	return []byte("/test/" + prefix + "/" + t.Name() + "/")
}

func TestDummyDriver_Put(t *testing.T) {
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

func TestDummyDriver_Get(t *testing.T) {
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

func TestDummyDriver_Delete(t *testing.T) {
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

func TestDummyDriver_GetAfterDelete(t *testing.T) {
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

func TestDummyDriver_ValueEqualPredicate(t *testing.T) {
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

func TestDummyDriver_ValueNotEqualPredicate(t *testing.T) {
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

func TestDummyDriver_VersionEqualPredicate(t *testing.T) {
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

	initialRevision := getRevisionFromResponse(t, response)

	response, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionEqual(key, initialRevision),
	}, []operation.Operation{
		operation.Put(key, []byte("updated")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Should succeed when version matches")
}

func TestDummyDriver_VersionGreaterPredicate(t *testing.T) {
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

	initialRevision := getRevisionFromResponse(t, response)

	response, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionGreater(key, initialRevision-1),
	}, []operation.Operation{
		operation.Put(key, []byte("new-value")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, response.Succeeded, "Should succeed when version is greater than specified version")
}

func TestDummyDriver_MultipleKeysPut(t *testing.T) {
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

func TestDummyDriver_MultiplePredicates(t *testing.T) {
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

func TestDummyDriver_MultipleOperations(t *testing.T) {
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

func TestDummyDriver_ElseOperations(t *testing.T) {
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

func TestDummyDriver_WatchPutEvent(t *testing.T) {
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

func TestDummyDriver_WatchDeleteEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "watch-delete")
	defer cleanupTestKey(ctx, driver, key)

	value := []byte("watch-test-value")

	watchCtx, cancel := context.WithTimeout(ctx, defaultWaitTimeout)
	defer cancel()

	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	require.NoError(t, err)

	defer stopWatch()

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

func TestDummyDriver_GetByPrefix(t *testing.T) {
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

func TestDummyDriver_GetNonExistentKey(t *testing.T) {
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

func TestDummyDriver_GetRoot(t *testing.T) {
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

func TestDummyDriver_VersionLessPredicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-less")
	defer cleanupTestKey(ctx, driver, key)

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("start")),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)

	currentRev := getRevisionFromResponse(t, resp)

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionLess(key, currentRev),
	}, []operation.Operation{
		operation.Put(key, []byte("fail")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "Should fail when version is not less than current")

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionLess(key, currentRev+1),
	}, []operation.Operation{
		operation.Put(key, []byte("success")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, resp.Succeeded, "Should succeed when version is less")
}

func TestDummyDriver_VersionNotEqualPredicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-not-equal")
	defer cleanupTestKey(ctx, driver, key)

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("data")),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)

	rev := getRevisionFromResponse(t, resp)

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionNotEqual(key, rev),
	}, []operation.Operation{
		operation.Put(key, []byte("fail")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded)

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionNotEqual(key, rev+1),
	}, []operation.Operation{
		operation.Put(key, []byte("success")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, resp.Succeeded)
}

func TestDummyDriver_ValueNotEqual_NonExistentKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "value-not-equal-missing")

	resp, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueNotEqual(key, "anything"),
	}, []operation.Operation{
		operation.Put(key, []byte("created")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, resp.Succeeded, "ValueNotEqual on missing key should succeed")
}

func TestDummyDriver_TxAtomicity_Revision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key1 := testKey(t, "atomic1")
	key2 := testKey(t, "atomic2")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, []byte("v1")),
		operation.Put(key2, []byte("v2")),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key1),
		operation.Get(key2),
	}, nil)
	require.NoError(t, err)
	require.Len(t, resp.Results, 2)

	rev1 := resp.Results[0].Values[0].ModRevision
	rev2 := resp.Results[1].Values[0].ModRevision

	assert.Equal(t, rev1, rev2, "Operations in same TX should have same revision")
	assert.Positive(t, rev1)

	// Check next TX gets higher revision.
	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, []byte("v1-updated")),
	}, nil)
	require.NoError(t, err)

	resp2, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key1),
	}, nil)
	require.NoError(t, err)

	rev3 := resp2.Results[0].Values[0].ModRevision

	assert.Greater(t, rev3, rev1, "Subsequent TX should have higher revision")
}

func TestDummyDriver_Delete_NonExistent_NoRevisionIncrement(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "delete-missing")
	refKey := testKey(t, "ref")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(refKey, []byte("initial")),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, nil, []operation.Operation{operation.Get(refKey)}, nil)
	revBefore := getRevisionFromResponse(t, resp)
	require.NoError(t, err)

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)

	key2 := testKey(t, "check")

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key2, []byte("check")),
	}, nil)
	require.NoError(t, err)

	resp2, err := driver.Execute(ctx, nil, []operation.Operation{operation.Get(key2)}, nil)
	revAfter := getRevisionFromResponse(t, resp2)
	require.NoError(t, err)

	assert.Equal(t,
		revBefore+1,
		revAfter,
		"Revision should increment by exactly 1 for the one successful Put, skipping the failed Delete")
}

func TestDummyDriver_EmptyOps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	resp, err := driver.Execute(ctx, nil, nil, nil)
	require.NoError(t, err)
	assert.True(t, resp.Succeeded)
	assert.Empty(t, resp.Results)
}

func TestDummyDriver_Watch_Recursive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	prefix := testKey(t, "watch-root")
	key := append(append([]byte{}, prefix...), []byte("child")...)

	watchCtx, cancel := context.WithTimeout(ctx, defaultWaitTimeout)
	defer cancel()

	eventCh, stop, err := driver.Watch(watchCtx, prefix)
	require.NoError(t, err)

	defer stop()

	_, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("child-value")),
	}, nil)
	require.NoError(t, err)

	select {
	case event := <-eventCh:
		assert.Equal(t, prefix, event.Prefix)
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for recursive watch event")
	}
}

func TestDummyDriver_InvalidPredicateValueType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "invalid-type")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("val")),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueEqual(key, 12345),
	}, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "Should fail when predicate value type is unsupported")
}

func TestDummyDriver_EmptyKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := []byte("")
	resp, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("root-val")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, resp.Succeeded)

	resp, err = driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Len(t, resp.Results[0].Values, 1)
	assert.Equal(t, []byte("root-val"), resp.Results[0].Values[0].Value)
}

func getRevisionFromResponse(t *testing.T, response tx.Response) int64 {
	t.Helper()

	position := 0

	require.Greater(t, len(response.Results), position, "expected at least %d results", position+1)
	require.NotEmpty(t, response.Results[position].Values, "expected at least %d values", position+1)

	return response.Results[position].Values[0].ModRevision
}

// mockPredicate is a test helper to create predicates with custom values for testing edge cases.
type mockPredicate struct {
	key       []byte
	operation predicate.Op
	target    predicate.Target
	value     any
}

func (m mockPredicate) Key() []byte {
	return m.key
}

func (m mockPredicate) Operation() predicate.Op {
	return m.operation
}

func (m mockPredicate) Target() predicate.Target {
	return m.target
}

func (m mockPredicate) Value() any {
	return m.value
}

func TestDummyDriver_VersionPredicate_InvalidType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-invalid-type")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("val")),
	}, nil)
	require.NoError(t, err)

	// Test with string instead of int64 for version.
	resp, err := driver.Execute(ctx, []predicate.Predicate{
		mockPredicate{
			key:       key,
			operation: predicate.OpEqual,
			target:    predicate.TargetVersion,
			value:     "not-an-int64",
		},
	}, []operation.Operation{
		operation.Delete(key),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "Should fail when version type is not int64")
}

func TestDummyDriver_VersionEqual_KeyDoesNotExist(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-equal-missing")

	resp, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionEqual(key, 1),
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "VersionEqual should fail when key doesn't exist")
}

func TestDummyDriver_VersionEqual_VersionMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-equal-mismatch")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("value")),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)

	currentRev := getRevisionFromResponse(t, resp)

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionEqual(key, currentRev+10),
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "VersionEqual should fail when version doesn't match")
}

func TestDummyDriver_VersionGreater_KeyDoesNotExist(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-greater-missing")

	resp, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionGreater(key, 0),
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "VersionGreater should fail when key doesn't exist")
}

func TestDummyDriver_VersionGreater_VersionNotGreater(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "version-greater-not")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("value")),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	require.NoError(t, err)

	currentRev := getRevisionFromResponse(t, resp)

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionGreater(key, currentRev),
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "VersionGreater should fail when version equals (not greater than) specified value")

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionGreater(key, currentRev+1),
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run2")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "VersionGreater should fail when version is less than specified value")
}

func TestDummyDriver_ValueEqual_ByteSliceType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "value-equal-bytes")
	value := []byte("test-bytes-value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	// Test with []byte value.
	resp, err := driver.Execute(ctx, []predicate.Predicate{
		mockPredicate{
			key:       key,
			operation: predicate.OpEqual,
			target:    predicate.TargetValue,
			value:     value,
		},
	}, []operation.Operation{
		operation.Put(key, []byte("updated")),
	}, nil)
	require.NoError(t, err)
	assert.True(t, resp.Succeeded, "ValueEqual with []byte type should succeed when values match")
}

func TestDummyDriver_ValueNotEqual_ValuesAreEqual(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "value-not-equal-fail")
	value := []byte("same-value")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	require.NoError(t, err)

	resp, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueNotEqual(key, "same-value"),
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "ValueNotEqual should fail when values are actually equal")
}

func TestDummyDriver_Predicate_UnknownOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "unknown-op")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("value")),
	}, nil)
	require.NoError(t, err)

	// Use an invalid operation value (999).
	resp, err := driver.Execute(ctx, []predicate.Predicate{
		mockPredicate{
			key:       key,
			operation: predicate.Op(999),
			target:    predicate.TargetVersion,
			value:     int64(1),
		},
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "Should fail with unknown predicate operation for TargetVersion")

	resp, err = driver.Execute(ctx, []predicate.Predicate{
		mockPredicate{
			key:       key,
			operation: predicate.Op(999),
			target:    predicate.TargetValue,
			value:     "value",
		},
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "Should fail with unknown predicate operation for TargetValue")
}

func TestDummyDriver_Predicate_UnknownTarget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	driver, done := createTestDriver(ctx, t)
	defer done()

	key := testKey(t, "unknown-target")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("value")),
	}, nil)
	require.NoError(t, err)

	// Use an invalid target value (999).
	resp, err := driver.Execute(ctx, []predicate.Predicate{
		mockPredicate{
			key:       key,
			operation: predicate.OpEqual,
			target:    predicate.Target(999),
			value:     "value",
		},
	}, []operation.Operation{
		operation.Put(key, []byte("should-not-run")),
	}, nil)
	require.NoError(t, err)
	assert.False(t, resp.Succeeded, "Should fail with unknown predicate target")
}
