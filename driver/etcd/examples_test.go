package etcd_test

// This file provides example tests for the etcd driver using LazyCluster.
// These examples demonstrate real usage with a running etcd instance.
//
// Due to inability to start multiple LazyClusters - we're using one LazyCluster
// and won't start tests in parallel here.

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	etcdclient "go.etcd.io/etcd/client/v3"
	etcdfintegration "go.etcd.io/etcd/tests/v3/framework/integration"

	"github.com/tarantool/go-storage/driver/etcd"
	testingUtils "github.com/tarantool/go-storage/internal/testing"
	etcdtestingUtils "github.com/tarantool/go-storage/internal/testing/etcd"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
)

const (
	exampleTestDialTimeout = 5 * time.Second
)

// createEtcdDriver creates an etcd driver for examples using the integration framework.
// Returns driver and cleanup function.
func createEtcdDriver(tb testing.TB) (*etcd.Driver, func()) {
	tb.Helper()

	etcdfintegration.BeforeTest(etcdtestingUtils.NewSilentTB(tb), etcdfintegration.WithoutGoLeakDetection())

	cluster := etcdtestingUtils.NewLazyCluster()

	tb.Cleanup(func() { cluster.Terminate() })

	endpoints := cluster.EndpointsGRPC()

	client, err := etcdclient.New(etcdclient.Config{
		Endpoints:   endpoints,
		DialTimeout: exampleTestDialTimeout,

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
	if err != nil {
		tb.Fatalf("Failed to create etcd client: %v", err)
	}

	tb.Cleanup(func() { _ = client.Close() })

	driver := etcd.New(client)

	return driver, func() {}
}

// ExampleDriver_Execute_simplePut demonstrates a simple Put operation with the etcd driver.
func ExampleDriver_Execute_simplePut() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/app/version")
	value := []byte("1.0.0")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	if err != nil {
		log.Printf("Put operation failed: %v", err)
		return
	}

	fmt.Println("Key", string(key), "stored with value:", string(value))

	// Output:
	// Key /config/app/version stored with value: 1.0.0
}

// ExampleDriver_Execute_simpleGet demonstrates a simple Get operation with the etcd driver.
func ExampleDriver_Execute_simpleGet() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/app/version")

	// First store a value.
	_, _ = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("1.0.0")),
	}, nil)

	// Then retrieve it.
	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	if err != nil {
		log.Printf("Get operation failed: %v", err)
		return
	}

	if response.Succeeded && len(response.Results) > 0 {
		if len(response.Results[0].Values) > 0 {
			kv := response.Results[0].Values[0]
			fmt.Printf("Retrieved key: %s, value: %s, version: %d\n",
				string(kv.Key), string(kv.Value), kv.ModRevision)
		}
	}

	// Output:
	// Retrieved key: /config/app/version, value: 1.0.0, version: 2
}

// ExampleDriver_Execute_simpleDelete demonstrates a simple Delete operation with the etcd driver.
func ExampleDriver_Execute_simpleDelete() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/app/version")

	// First store a value.
	_, _ = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, []byte("1.0.0")),
	}, nil)

	// Then delete it.
	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Delete(key),
	}, nil)
	if err != nil {
		log.Printf("Delete operation failed: %v", err)
		return
	}

	fmt.Println("Successfully deleted key:", string(key))

	// Output:
	// Successfully deleted key: /config/app/version
}

// ExampleDriver_Execute_simpleMultiPut demonstrates multiple Put operations in a single transaction with
// the etcd driver.
func ExampleDriver_Execute_simpleMultiPut() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	response, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put([]byte("/config/app/name"), []byte("MyApp")),
		operation.Put([]byte("/config/app/environment"), []byte("production")),
	}, nil)
	if err != nil {
		log.Printf("Multi-put operation failed: %v", err)
		return
	}

	fmt.Println("Successfully stored", len(response.Results), "configuration items")

	// Output:
	// Successfully stored 2 configuration items
}

// ExampleDriver_Execute_with_predicates_valueBased demonstrates value-based conditional updates using predicates.
func ExampleDriver_Execute_with_predicates_valueBased() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/app/settings")
	currentValue := []byte("old-settings")
	newValue := []byte("new-settings")

	_, _ = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, currentValue),
	}, nil)

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueEqual(key, []byte("old-settings")),
	}, []operation.Operation{
		operation.Put(key, newValue),
	}, nil)
	if err != nil {
		log.Printf("Conditional update failed: %v", err)
		return
	}

	if response.Succeeded {
		fmt.Println("Conditional update succeeded - value was updated")
	} else {
		fmt.Println("Conditional update failed - value did not match")
	}

	// Output:
	// Conditional update succeeded - value was updated
}

// ExampleDriver_Execute_with_predicates_versionBased demonstrates version-based conditional updates using predicates.
func ExampleDriver_Execute_with_predicates_versionBased() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/app/feature")
	value := []byte("enabled")

	_, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key, value),
	}, nil)
	if err != nil {
		log.Printf("Initial update failed: %v", err)
		return
	}

	getResponse, err := driver.Execute(ctx, nil, []operation.Operation{
		operation.Get(key),
	}, nil)
	if err != nil {
		log.Printf("Get operation failed: %v", err)
		return
	}

	var currentVersion int64
	if len(getResponse.Results) > 0 && len(getResponse.Results[0].Values) > 0 {
		currentVersion = getResponse.Results[0].Values[0].ModRevision
	}

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.VersionEqual(key, currentVersion),
	}, []operation.Operation{
		operation.Put(key, []byte("disabled")),
	}, nil)
	if err != nil {
		log.Printf("Version-based update failed: %v", err)
		return
	}

	if response.Succeeded {
		fmt.Println("Version-based update succeeded - no concurrent modification")
	} else {
		fmt.Println("Version-based update failed - version conflict detected")
	}

	// Output:
	// Version-based update succeeded - no concurrent modification
}

// ExampleDriver_Execute_with_predicates_multiplePredicates demonstrates multiple predicates with Else operations.
func ExampleDriver_Execute_with_predicates_multiplePredicates() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key1 := []byte("/config/database/host")
	key2 := []byte("/config/database/port")

	_, _ = driver.Execute(ctx, nil, []operation.Operation{
		operation.Put(key1, []byte("localhost")),
		operation.Put(key2, []byte("5432")),
	}, nil)

	response, err := driver.Execute(ctx, []predicate.Predicate{
		predicate.ValueEqual(key1, []byte("localhost")),
		predicate.ValueEqual(key2, []byte("5432")),
	}, []operation.Operation{
		operation.Put(key1, []byte("new-host")),
		operation.Put(key2, []byte("6432")),
	}, []operation.Operation{
		operation.Delete(key1),
		operation.Delete(key2),
	})
	if err != nil {
		log.Printf("Multi-predicate transaction failed: %v", err)
		return
	}

	if response.Succeeded {
		fmt.Println("Multi-predicate transaction succeeded - values were updated")
	} else {
		fmt.Println("Multi-predicate transaction failed - cleanup operations executed")
	}

	// Output:
	// Multi-predicate transaction succeeded - values were updated
}

// ExampleDriver_Watch_basic demonstrates basic watch on a single key.
func ExampleDriver_Watch_basic() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/app/status")

	watchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	if err != nil {
		log.Printf("Failed to start watch: %v", err)
		return
	}
	defer stopWatch()

	fmt.Println("Watching for changes on:", string(key))

	go func() {
		time.Sleep(100 * time.Millisecond)

		_, _ = driver.Execute(ctx, nil, []operation.Operation{
			operation.Put(key, []byte("running")),
		}, nil)
	}()

	select {
	case event := <-eventCh:
		fmt.Printf("Received watch event for key: %s\n", string(event.Prefix))
	case <-watchCtx.Done():
		fmt.Println("Watch context expired")
	}

	// Output:
	// Watching for changes on: /config/app/status
	// Received watch event for key: /config/app/status
}

// ExampleDriver_Watch_multipleOperations demonstrates watching for multiple operations on a key prefix.
func ExampleDriver_Watch_multipleOperations() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/database/")

	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	if err != nil {
		log.Printf("Failed to start watch: %v", err)
		return
	}
	defer stopWatch()

	fmt.Println("Watching for changes on prefix:", string(key))

	go func() {
		_, _ = driver.Execute(ctx, nil, []operation.Operation{
			operation.Put([]byte("/config/database/host"), []byte("db1")),
		}, nil)

		_, _ = driver.Execute(ctx, nil, []operation.Operation{
			operation.Put([]byte("/config/database/port"), []byte("5432")),
		}, nil)

		_, _ = driver.Execute(ctx, nil, []operation.Operation{
			operation.Delete([]byte("/config/database/host")),
		}, nil)
	}()

	eventCount := 0
	for eventCount < 3 {
		select {
		case event := <-eventCh:
			fmt.Printf("Event %d: change detected on %s\n", eventCount+1, string(event.Prefix))

			eventCount++
		case <-watchCtx.Done():
			fmt.Println("Watch context expired")
			return
		}
	}

	// Output:
	// Watching for changes on prefix: /config/database/
	// Event 1: change detected on /config/database/
	// Event 2: change detected on /config/database/
	// Event 3: change detected on /config/database/
}

// ExampleDriver_Watch_gracefulTermination demonstrates graceful watch termination with manual control.
func ExampleDriver_Watch_gracefulTermination() {
	// Create a testing context for the example.
	t := testingUtils.NewT()
	defer t.Cleanups()

	ctx := context.Background()

	driver, cleanup := createEtcdDriver(t)
	defer cleanup()

	key := []byte("/config/monitoring/metrics")

	watchCtx, cancel := context.WithCancel(ctx)

	eventCh, stopWatch, err := driver.Watch(watchCtx, key)
	if err != nil {
		log.Printf("Failed to start watch: %v", err)
		cancel()

		return
	}

	fmt.Println("Started watch with manual control")

	go func() {
		time.Sleep(100 * time.Millisecond)
		fmt.Println("Stopping watch gracefully...")
		stopWatch()
		cancel()
	}()

	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return
			}

			fmt.Printf("Received event: %s\n", string(event.Prefix))
		case <-watchCtx.Done():
			return
		}
	}

	// Output:
	// Started watch with manual control
	// Stopping watch gracefully...
}
