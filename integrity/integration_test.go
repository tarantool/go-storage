//nolint:paralleltest
package integrity_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	tcshelper "github.com/tarantool/go-tarantool/v2/test_helpers/tcs"
	etcdclient "go.etcd.io/etcd/client/v3"
	etcdfintegration "go.etcd.io/etcd/tests/v3/framework/integration"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver"
	etcddriver "github.com/tarantool/go-storage/driver/etcd"
	tcsdriver "github.com/tarantool/go-storage/driver/tcs"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/internal/testing/etcd"
)

// IntegrationStruct is a simple structure used for integration tests.
type IntegrationStruct struct {
	Name  string `yaml:"name"`
	Value int    `yaml:"value"`
}

const (
	defaultWaitTimeout = 5 * time.Second
	testDialTimeout    = 5 * time.Second
)

var (
	// TCS availability flags.
	haveTCS      bool     //nolint:gochecknoglobals
	tcsEndpoints []string //nolint:gochecknoglobals
)

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup TCS if available.
	tcsInstance, err := tcshelper.Start(0)
	switch {
	case errors.Is(err, tcshelper.ErrNotSupported):
		fmt.Println("TCS is not supported:", err) //nolint:forbidigo
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

// createEtcdTestDriver creates an etcd driver for testing using the integration framework.
func createEtcdTestDriver(_ context.Context, t *testing.T) (driver.Driver, func()) {
	t.Helper()

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

// createTcsTestDriver creates a TCS driver for testing.
// It skips the test if no Tarantool instance is available.
func createTcsTestDriver(ctx context.Context, t *testing.T) (driver.Driver, func()) {
	t.Helper()

	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
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

	return tcsdriver.New(wrapper), func() { _ = wrapper.Close() }
}

type preparationCallback func(ctx context.Context, t *testing.T) (driver.Driver, func())

var storages = map[string]preparationCallback{ //nolint:gochecknoglobals
	"tcs":  createTcsTestDriver,
	"etcd": createEtcdTestDriver,
}

func executeOnStorage(t *testing.T, testCallback func(t *testing.T, driver driver.Driver)) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	for storageName, storagePreparation := range storages {
		t.Run(storageName, func(t *testing.T) {
			driverInstance, cancel := storagePreparation(t.Context(), t)
			defer cancel()

			testCallback(t, driverInstance)
		})
	}
}

func TestTypedIntegration_GetPut(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()

		st := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](st).
			WithPrefix("/test").
			Build()

		value := IntegrationStruct{Name: "test", Value: 42}
		err := typed.Put(ctx, "my-object", value)
		require.NoError(t, err, "Put should succeed")

		// Verify we can retrieve the value.
		result, err := typed.Get(ctx, "my-object")
		require.NoError(t, err, "Get should succeed after Put")
		assert.Equal(t, "my-object", result.Name)
		assert.True(t, result.Value.IsSome())

		val, ok := result.Value.Get()
		require.True(t, ok)
		assert.Equal(t, value, val)
	})
}

// Etcd return ErrNotFound if value was deleted.
func TestTypedIntegrationEtcd_DeletePrefix(t *testing.T) {
	ctx := t.Context()

	driverInstance, cleanup := createEtcdTestDriver(ctx, t)
	defer cleanup()

	st := storage.NewStorage(driverInstance)
	typed := integrity.NewTypedBuilder[IntegrationStruct](st).
		WithPrefix("/test").
		Build()

	value := IntegrationStruct{Name: "test", Value: 42}
	err := typed.Put(ctx, "object-1", value)
	require.NoError(t, err, "Put should succeed")

	err = typed.Put(ctx, "kbject-2", value)
	require.NoError(t, err, "Put should succeed")

	err = typed.Delete(ctx, "o", integrity.WithPrefixDelete())
	require.NoError(t, err, "Delete should succeed")

	_, err = typed.Get(ctx, "object-1")
	require.ErrorIs(t, err, integrity.ErrNotFound, "Get should return not found error after Delete")

	result, err := typed.Get(ctx, "kbject-2")
	require.NoError(t, err, "Get should succeed after Put")
	assert.Equal(t, "kbject-2", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)
}

// TCS return nil if value was deleted.
func TestTypedIntegrationTCS_DeletePrefix(t *testing.T) {
	ctx := t.Context()

	driverInstance, cleanup := createTcsTestDriver(ctx, t)
	defer cleanup()

	st := storage.NewStorage(driverInstance)
	typed := integrity.NewTypedBuilder[IntegrationStruct](st).
		WithPrefix("/test").
		Build()

	value := IntegrationStruct{Name: "test", Value: 42}
	err := typed.Put(ctx, "object-1", value)
	require.NoError(t, err, "Put should succeed")

	err = typed.Put(ctx, "kbject-2", value)
	require.NoError(t, err, "Put should succeed")

	err = typed.Delete(ctx, "o", integrity.WithPrefixDelete())
	require.NoError(t, err, "Delete should succeed")

	_, err = typed.Get(ctx, "object-1")
	require.NoError(t, err, "Get should return nil after Delete")

	result, err := typed.Get(ctx, "kbject-2")
	require.NoError(t, err, "Get should succeed after Put")
	assert.Equal(t, "kbject-2", result.Name)
	assert.True(t, result.Value.IsSome())

	val, ok := result.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)
}

func TestTypedIntegration_GetPrefix(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()

		st := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](st).
			WithPrefix("/test").
			Build()

		value := IntegrationStruct{Name: "test", Value: 42}

		err := typed.Put(ctx, "object-1", value)
		require.NoError(t, err, "Put should succeed")

		err = typed.Put(ctx, "object-2", value)
		require.NoError(t, err, "Put should succeed")

		result, err := typed.Get(ctx, "", integrity.WithPrefixGet())
		require.ErrorIs(t, err, integrity.ErrMoreThanOneResult, "Get should succeed after Put")

		require.Empty(t, result.Name)
		require.Equal(t, int64(integrity.ModRevisionEmpty), result.ModRevision)
		require.NoError(t, result.Error)
		require.False(t, result.Value.IsSome())
	})
}
