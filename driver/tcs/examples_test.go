package tcs_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-storage/driver/tcs"
	gsTesting "github.com/tarantool/go-storage/internal/testing"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
)

func newResponse(success bool, data [][]any) *gsTesting.MockResponse {
	return gsTesting.NewMockResponse(gsTesting.NewT(), []any{
		map[string]any{
			"data": map[string]any{
				"responses":  data,
				"is_success": success,
			},
			"revision": 1000,
		},
	})
}

func createTCSDriverExecuteSimple() *tcs.Driver {
	mock := gsTesting.NewMockDoer(gsTesting.NewT(),

		newResponse(true, [][]any{{}}),
		newResponse(true, [][]any{
			{
				map[string]any{
					"path":         []byte("/config/app/version"),
					"value":        []byte("1.0.0"),
					"mod_revision": 1000,
				},
			},
		}),
		newResponse(true, [][]any{{}}),
		newResponse(true, [][]any{{}, {}}),
	)

	// createDriver is a function to create dummy driver.
	return tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))
}

// ExampleExecuteBasicOperations demonstrates basic Execute operations with the TCS driver.
// This example shows Put, Get, and Delete operations without predicates.
func ExampleDriver_Execute_simple() {
	ctx := context.Background()

	driver := createTCSDriverExecuteSimple()

	// Example 1: Simple Put operation.
	{
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
	}

	// Example 2: Simple Get operation.
	{
		key := []byte("/config/app/version")

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
	}

	// Example 3: Simple Delete operation.
	{
		key := []byte("/config/app/version")

		_, err := driver.Execute(ctx, nil, []operation.Operation{
			operation.Delete(key),
		}, nil)
		if err != nil {
			log.Printf("Delete operation failed: %v", err)
			return
		}

		fmt.Println("Successfully deleted key:", string(key))
	}

	// Example 4: Multiple operations in single transaction.
	{
		response, err := driver.Execute(ctx, nil, []operation.Operation{
			operation.Put([]byte("/config/app/name"), []byte("MyApp")),
			operation.Put([]byte("/config/app/environment"), []byte("production")),
		}, nil)
		if err != nil {
			log.Printf("Multi-put operation failed: %v", err)
			return
		}

		fmt.Println("Successfully stored", len(response.Results), "configuration items")
	}

	// Output:
	// Key /config/app/version stored with value: 1.0.0
	// Retrieved key: /config/app/version, value: 1.0.0, version: 1000
	// Successfully deleted key: /config/app/version
	// Successfully stored 2 configuration items
}

func createTCSDriverExecuteWithPredicates() *tcs.Driver {
	mock := gsTesting.NewMockDoer(gsTesting.NewT(),

		newResponse(true, [][]any{{}}),
		newResponse(true, [][]any{{}}),
		newResponse(true, [][]any{
			{
				map[string]any{
					"path":         []byte("/config/app/feature"),
					"value":        []byte("enabled"),
					"mod_revision": 1000,
				},
			},
		}),
		newResponse(true, [][]any{{}}),
		newResponse(true, [][]any{{}, {}}),
		newResponse(true, [][]any{{}, {}}),
		newResponse(false, [][]any{{}, {}}),
	)

	// createDriver is a function to create dummy driver.
	return tcs.New(gsTesting.NewMockDoerWithWatcher(mock, nil))
}

// ExampleDriver_Execute_WithPredicates demonstrates conditional Execute operations using predicates.
// This example shows how to use value and version predicates for conditional execution.
func ExampleDriver_Execute_with_predicates() {
	ctx := context.Background()

	driver := createTCSDriverExecuteWithPredicates()

	// Example 1: Value-based conditional update.
	{
		key := []byte("/config/app/settings")
		currentValue := []byte("old-settings")
		newValue := []byte("new-settings")

		_, _ = driver.Execute(ctx, nil, []operation.Operation{
			operation.Put(key, currentValue),
		}, nil)

		response, err := driver.Execute(ctx, []predicate.Predicate{
			predicate.ValueEqual(key, "old-settings"),
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
	}

	// Example 2: Version-based conditional update.
	{
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
	}

	// Example 3: Multiple predicates with Else operations.
	{
		key1 := []byte("/config/database/host")
		key2 := []byte("/config/database/port")

		_, _ = driver.Execute(ctx, nil, []operation.Operation{
			operation.Put(key1, []byte("localhost")),
			operation.Put(key2, []byte("5432")),
		}, nil)

		response, err := driver.Execute(ctx, []predicate.Predicate{
			predicate.ValueEqual(key1, "localhost"),
			predicate.ValueEqual(key2, "5432"),
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
	}

	// Output:
	// Conditional update succeeded - value was updated
	// Version-based update succeeded - no concurrent modification
	// Multi-predicate transaction failed - cleanup operations executed
}

func createTCSDriverWatch() *tcs.Driver {
	return tcs.New(
		gsTesting.NewMockDoerWithWatcher(
			gsTesting.NewMockDoer(gsTesting.NewT(),
				newResponse(true, [][]any{{}}),
				newResponse(true, [][]any{{}}),
				newResponse(true, [][]any{{}}),
				newResponse(true, [][]any{{}}),
			),
			map[string][]tarantool.WatchEvent{
				"config.storage:/config/app/status": {
					tarantool.WatchEvent{
						Conn:  nil,
						Key:   "/config/app/status",
						Value: nil,
					},
				},
				"config.storage:/config/database/": {
					tarantool.WatchEvent{
						Conn:  nil,
						Key:   "/config/database/host",
						Value: nil,
					},
					tarantool.WatchEvent{
						Conn:  nil,
						Key:   "/config/database/port",
						Value: nil,
					},
					tarantool.WatchEvent{
						Conn:  nil,
						Key:   "/config/database/host",
						Value: nil,
					},
				},
				"config.storage:/config/monitoring/metrics": {},
			},
		),
	)
}

// ExampleWatchOperations demonstrates how to use Watch for real-time change notifications.
// This example shows watching individual keys and handling watch events.
func ExampleDriver_Watch() {
	ctx := context.Background()

	driver := createTCSDriverWatch()

	// Example 1: Basic watch on a single key.
	{
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
	}

	// Example 2: Watch with multiple operations.
	{
		key := []byte("/config/database/")

		watchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		eventCh, stopWatch, err := driver.Watch(watchCtx, key)
		if err != nil {
			log.Printf("Failed to start watch: %v", err)
			return
		}
		defer stopWatch()

		fmt.Println("Watching for changes on prefix:", string(key))

		go func() {
			time.Sleep(100 * time.Millisecond)

			_, _ = driver.Execute(ctx, nil, []operation.Operation{
				operation.Put([]byte("/config/database/host"), []byte("db1")),
			}, nil)

			time.Sleep(200 * time.Millisecond)

			_, _ = driver.Execute(ctx, nil, []operation.Operation{
				operation.Put([]byte("/config/database/port"), []byte("5432")),
			}, nil)

			time.Sleep(300 * time.Millisecond)

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
	}

	// Example 3: Graceful watch termination.
	{
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
	}

	// Output:
	// Watching for changes on: /config/app/status
	// Received watch event for key: /config/app/status
	// Watching for changes on prefix: /config/database/
	// Event 1: change detected on /config/database/
	// Event 2: change detected on /config/database/
	// Event 3: change detected on /config/database/
	// Started watch with manual control
	// Stopping watch gracefully...
}
