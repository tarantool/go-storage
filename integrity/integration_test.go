package integrity_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
	tcshelper "github.com/tarantool/go-tarantool/v3/test_helpers/tcs"
	etcdclient "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-storage/v2/driver"
	"github.com/tarantool/go-storage/v2/driver/dummy"
	etcddriver "github.com/tarantool/go-storage/v2/driver/etcd"
	tcsdriver "github.com/tarantool/go-storage/v2/driver/tcs"
	etcdtest "github.com/tarantool/go-storage/v2/test_helpers/etcd"
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

var rsaPK2048 *rsa.PrivateKey //nolint:gochecknoglobals

// sharedEtcdCluster is a single embedded etcd reused across every etcd
// subtest in this package. Spinning up one cluster per subtest under
// `-race -count=N` blows past the standard 30-minute test timeout; sharing
// reduces total cluster boots from N × #tests to one.
//
//nolint:gochecknoglobals
var sharedEtcdCluster *etcdtest.LazyCluster

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

	rsaPK2048, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	sharedEtcdCluster = etcdtest.NewLazyCluster(etcdtest.ClusterConfig{Size: 1}) //nolint:exhaustruct

	os.Exit(func() int {
		defer func() {
			if haveTCS {
				tcsInstance.Stop()
			}

			sharedEtcdCluster.Terminate()
		}()

		return m.Run()
	}())
}

// createDummyDriver creates a dummy driver for testing.
func createDummyDriver(_ context.Context, t *testing.T) (driver.Driver, func()) {
	t.Helper()

	return dummy.New(), func() {}
}

// createEtcdTestDriver creates an etcd driver for testing using the integration framework.
// It reuses the package-wide sharedEtcdCluster; per-test data isolation is the
// caller's responsibility (tests already cleanup their own keys).
func createEtcdTestDriver(_ context.Context, t *testing.T) (driver.Driver, func()) {
	t.Helper()

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
				Allocator:     nil,
			},
		})
	}

	conn, err := pool.New(ctx, instances)
	require.NoError(t, err, "Failed to connect to Tarantool pool")

	// Wrap the pool connection to implement DoerWatcher.
	wrapper := pool.NewConnectorAdapter(conn, pool.ModeRW)

	return tcsdriver.New(wrapper), func() { _ = wrapper.Close() }
}

type preparationCallback func(ctx context.Context, t *testing.T) (driver.Driver, func())

var storages = map[string]preparationCallback{ //nolint:gochecknoglobals
	"tcs":   createTcsTestDriver,
	"etcd":  createEtcdTestDriver,
	"dummy": createDummyDriver,
}

func executeOnStorage(t *testing.T, testCallback func(t *testing.T, driver driver.Driver)) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	for storageName, storagePreparation := range storages {
		t.Run(storageName, func(t *testing.T) {
			t.Helper()

			driverInstance, cancel := storagePreparation(t.Context(), t)
			defer cancel()

			testCallback(t, driverInstance)
		})
	}
}
