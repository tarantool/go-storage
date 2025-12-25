//nolint:paralleltest
package integrity_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-option"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	tcshelper "github.com/tarantool/go-tarantool/v2/test_helpers/tcs"
	etcdclient "go.etcd.io/etcd/client/v3"
	etcdfintegration "go.etcd.io/etcd/tests/v3/framework/integration"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/driver"
	etcddriver "github.com/tarantool/go-storage/driver/etcd"
	tcsdriver "github.com/tarantool/go-storage/driver/tcs"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/internal/testing/etcd"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/watch"
)

// IntegrationStruct is a simple structure used for integration tests.
type IntegrationStruct struct {
	Name  string `yaml:"name"`
	Value int    `yaml:"value"`
}

const (
	defaultWaitTimeout = 5 * time.Second
	testDialTimeout    = 5 * time.Second
	megabyte           = 1024 * 1024
)

const (
	writers            = 5
	readers            = 5
	iterNumsConcurrent = 1000
)

const iterNumsManyRequests = 1000

var (
	// TCS availability flags.
	haveTCS      bool     //nolint:gochecknoglobals
	tcsEndpoints []string //nolint:gochecknoglobals
)

func generateRandomString(t *testing.T, length int) string {
	t.Helper()

	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	byteString := make([]byte, length)

	for i := range length {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		require.NoError(t, err)

		byteString[i] = charset[n.Int64()]
	}

	return string(byteString)
}

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
			t.Helper()

			driverInstance, cancel := storagePreparation(t.Context(), t)
			defer cancel()

			testCallback(t, driverInstance)
		})
	}
}

type FillDataOptions struct {
	Prefix  string
	Names   []string
	Records []IntegrationStruct
	Hashers []hasher.Hasher
	Signers []crypto.Signer
}

func fillData(t *testing.T, driverInstance driver.Driver, opts FillDataOptions) {
	t.Helper()

	ctx := t.Context()

	require.Len(t, opts.Names, len(opts.Records))

	hashersNames := make([]string, 0, len(opts.Hashers))
	for _, h := range opts.Hashers {
		hashersNames = append(hashersNames, h.Name())
	}

	signersNames := make([]string, 0, len(opts.Signers))
	for _, s := range opts.Signers {
		signersNames = append(signersNames, s.Name())
	}

	namerInstance := namer.NewDefaultNamer(opts.Prefix, hashersNames, signersNames)

	generator := integrity.NewGenerator[IntegrationStruct](
		namerInstance,
		marshaller.NewTypedYamlMarshaller[IntegrationStruct](),
		opts.Hashers,
		opts.Signers,
	)

	putOps := make([]operation.Operation, 0)

	for i, record := range opts.Records {
		kvs, err := generator.Generate(opts.Names[i], record)
		require.NoError(t, err)

		for _, kv := range kvs {
			putOps = append(putOps, operation.Put(kv.Key, kv.Value))
		}
	}

	_, err := driverInstance.Execute(ctx, nil, putOps, nil)
	require.NoError(t, err)
}

func cleanupTyped[T any](t *testing.T, typed *integrity.Typed[T], names ...string) {
	t.Helper()

	ctx := t.Context()

	for _, name := range names {
		err := typed.Delete(ctx, name)
		require.NoError(t, err)
	}
}

func TestTypedIntegration_GetPut(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()

		storageInstance := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
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

		cleanupTyped(t, typed, "my-object")
	})
}

func TestTypedIntegration_Delete(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		fillDataOptions := FillDataOptions{
			Prefix: "/test",
			Names:  []string{"my-object"},
			Records: []IntegrationStruct{
				{
					Name:  "test",
					Value: 42,
				},
			},
			Hashers: nil,
			Signers: nil,
		}

		fillData(t, driverInstance, fillDataOptions)

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		err := typed.Delete(ctx, "my-object")
		require.NoError(t, err)

		_, err = typed.Get(ctx, "my-object")
		require.Error(t, err)
	})
}

func TestTypedIntegration_Get_EmptyStorage(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		_, err := typed.Get(ctx, "my-object")
		require.Error(t, err)
	})
}

func TestTypedIntegration_Range(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		fillDataOptions := FillDataOptions{
			Prefix: "/test",
			Names:  []string{"object1", "object2"},
			Records: []IntegrationStruct{
				{
					Name:  "obj1",
					Value: 100,
				},
				{
					Name:  "obj2",
					Value: 200,
				},
			},
			Hashers: nil,
			Signers: nil,
		}

		fillData(t, driverInstance, fillDataOptions)

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		results, err := typed.Range(ctx, "")
		require.NoError(t, err)
		require.Len(t, results, 2)

		// TCS and Etcd return different ModRevision values,
		// so they must be normalized.
		for i := range results {
			results[i].ModRevision = 0
		}

		require.ElementsMatch(t, results, []integrity.ValidatedResult[IntegrationStruct]{
			{
				Name: "object1",
				Value: option.Some[IntegrationStruct](IntegrationStruct{
					Name:  "obj1",
					Value: 100,
				}),
				ModRevision: 0,
				Error:       nil,
			},
			{
				Name: "object2",
				Value: option.Some[IntegrationStruct](IntegrationStruct{
					Name:  "obj2",
					Value: 200,
				}),
				ModRevision: 0,
				Error:       nil,
			},
		})

		// Cleanup.
		cleanupTyped(t, typed, "object1", "object2")
	})
}

func TestTypedIntegration_Watch(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		fillDataOptions := FillDataOptions{
			Prefix: "/test",
			Names:  []string{"object1"},
			Records: []IntegrationStruct{
				{
					Name:  "obj1",
					Value: 100,
				},
			},
			Hashers: nil,
			Signers: nil,
		}

		fillData(t, driverInstance, fillDataOptions)

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		eventCh, err := typed.Watch(ctx, "object1")
		require.NoError(t, err)

		expectedEvents := []watch.Event{
			{Prefix: []byte("/test/object1")},
		}

		err = typed.Delete(ctx, "object1")
		require.NoError(t, err)

		var receivedEvents []watch.Event

		select {
		case e := <-eventCh:
			receivedEvents = append(receivedEvents, e)
		case <-time.After(100 * time.Millisecond):
		}

		require.Len(t, receivedEvents, 1)
		require.Equal(t, expectedEvents, receivedEvents)
	})
}

func TestTypedIntegration_HashVerification_SameHash(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typedSHA256Hasher := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err := typedSHA256Hasher.Put(ctx, "obj1", value)
		require.NoError(t, err)

		// Same hasher.
		result, err := typedSHA256Hasher.Get(ctx, "obj1")
		require.NoError(t, err)
		assert.Equal(t, "obj1", result.Name)
		assert.True(t, result.Value.IsSome())

		val, ok := result.Value.Get()
		require.True(t, ok)
		assert.Equal(t, value, val)

		// Cleanup.
		cleanupTyped(t, typedSHA256Hasher, "object1")
	})
}

func TestTypedIntegration_HashVerification_DifferentHash(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typedSHA256Hasher := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		typedSHA1Hasher := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithHasher(hasher.NewSHA1Hasher()).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err := typedSHA256Hasher.Put(ctx, "obj1", value)
		require.NoError(t, err)

		// Another hasher.
		_, err = typedSHA1Hasher.Get(ctx, "obj1")
		require.Error(t, err)

		// Cleanup.
		cleanupTyped(t, typedSHA256Hasher, "object1")
	})
}

func TestTypedIntegration_SignatureVerification_SameSignatureVerification(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		privateKey1, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		typed1 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey1)).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err = typed1.Put(ctx, "obj1", value)
		require.NoError(t, err)

		result, err := typed1.Get(ctx, "obj1")
		require.NoError(t, err)
		assert.Equal(t, "obj1", result.Name)
		assert.True(t, result.Value.IsSome())

		val, ok := result.Value.Get()
		require.True(t, ok)
		assert.Equal(t, value, val)

		// Cleanup.
		cleanupTyped(t, typed1, "obj1")
	})
}

func TestTypedIntegration_SignatureVerification_DifferentSignatureVerification(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		privateKey1, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		privateKey2, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		typed1 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey1)).
			Build()

		typed2 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey2)).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err = typed1.Put(ctx, "obj1", value)
		require.NoError(t, err)

		_, err = typed2.Get(ctx, "obj1")
		require.Error(t, err)

		// Cleanup.
		cleanupTyped(t, typed1, "obj1")
	})
}

func TestTypedIntegration_CorruptedData_SameSignerVerifierAndHasher(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		privateKey1, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		typed1 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey1)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err = typed1.Put(ctx, "obj1", value)
		require.NoError(t, err)

		result, err := typed1.Get(ctx, "obj1")
		require.NoError(t, err)
		assert.Equal(t, "obj1", result.Name)
		assert.True(t, result.Value.IsSome())

		val, ok := result.Value.Get()
		require.True(t, ok)
		assert.Equal(t, value, val)

		// Cleanup.
		cleanupTyped(t, typed1, "obj1")
	})
}

func TestTypedIntegration_CorruptedData_DifferentSignerVerifierAndHasher(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		privateKey1, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		privateKey2, err := rsa.GenerateKey(rand.Reader, 4096)
		require.NoError(t, err)

		typed1 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey1)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		typed2 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey2)).
			WithHasher(hasher.NewSHA1Hasher()).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err = typed1.Put(ctx, "obj1", value)
		require.NoError(t, err)

		_, err = typed2.Get(ctx, "obj1")
		require.Error(t, err)

		// Cleanup.
		cleanupTyped(t, typed1, "obj1")
	})
}

func TestTypedIntegration_MissingSignature(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		privateKey1, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		typed1 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey1)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		typed2 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err = typed2.Put(ctx, "obj1", value)
		require.NoError(t, err)

		_, err = typed1.Get(ctx, "obj1")
		require.Error(t, err)

		// Cleanup.
		cleanupTyped(t, typed2, "obj1")
	})
}

func TestTypedIntegration_MissingHash(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		privateKey1, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		typed1 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey1)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		typed2 := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey1)).
			Build()

		value := IntegrationStruct{Name: "object1", Value: 100}

		err = typed2.Put(ctx, "obj1", value)
		require.NoError(t, err)

		_, err = typed1.Get(ctx, "obj1")
		require.Error(t, err)

		// Cleanup.
		cleanupTyped(t, typed2, "obj1")
	})
}

func TestTypedIntegration_ConcurrentAccess(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		key := "shared-key"

		var waitGr sync.WaitGroup

		for range writers {
			waitGr.Add(1)

			go func() {
				defer waitGr.Done()

				for i := range iterNumsConcurrent {
					value := IntegrationStruct{
						Name:  fmt.Sprintf("object-%d", i),
						Value: i,
					}
					err := typed.Put(ctx, key, value)
					assert.NoError(t, err)
				}
			}()
		}

		for range readers {
			waitGr.Add(1)

			go func() {
				defer waitGr.Done()

				for range iterNumsConcurrent {
					_, err := typed.Get(ctx, key)
					if err != nil {
						assert.ErrorIs(t, err, integrity.ErrNotFound)
					}
				}
			}()
		}

		waitGr.Wait()

		// Cleanup.
		cleanupTyped(t, typed, key)
	})
}

func TestTypedIntegration_ManyRequests(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()

		storageInstance := storage.NewStorage(driverInstance)

		cleanupNames := make([]string, 0, iterNumsManyRequests)

		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		for iter := range iterNumsManyRequests {
			name := fmt.Sprintf("obj_%d", iter)
			value := IntegrationStruct{Name: fmt.Sprintf("object_%d", iter), Value: iter}
			err := typed.Put(ctx, name, value)
			require.NoError(t, err)

			cleanupNames = append(cleanupNames, name)
		}

		for iter := range iterNumsManyRequests {
			value := IntegrationStruct{Name: fmt.Sprintf("object_%d", iter), Value: iter}
			result, err := typed.Get(ctx, fmt.Sprintf("obj_%d", iter))
			require.NoError(t, err)

			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("obj_%d", iter), result.Name)
			assert.True(t, result.Value.IsSome())

			val, ok := result.Value.Get()
			require.True(t, ok)
			assert.Equal(t, value, val)
		}

		// Cleanup.
		cleanupTyped(t, typed, cleanupNames...)
	})
}

func TestTypedIntegration_LargeValue(t *testing.T) {
	testTypedLargeValue := func(t *testing.T, driverInstance driver.Driver, maxMemory int) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*privateKey)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		value := IntegrationStruct{Name: generateRandomString(t, maxMemory), Value: 10}

		err = typed.Put(ctx, "big-object", value)
		require.NoError(t, err)

		result, err := typed.Get(ctx, "big-object")
		require.NoError(t, err)
		assert.Equal(t, "big-object", result.Name)
		assert.True(t, result.Value.IsSome())

		val, ok := result.Value.Get()
		require.True(t, ok)
		assert.Equal(t, value, val)

		// Cleanup.
		cleanupTyped(t, typed, "big-object")
	}

	t.Run("TCS", func(t *testing.T) {
		driver, cleanup := createTcsTestDriver(t.Context(), t)
		defer cleanup()

		testTypedLargeValue(t, driver, megabyte/2)
	})
	t.Run("etcd", func(t *testing.T) {
		driver, cleanup := createEtcdTestDriver(t.Context(), t)
		defer cleanup()

		testTypedLargeValue(t, driver, megabyte)
	})
}

func TestTypedIntegration_SpecialNames(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		value := IntegrationStruct{Name: "obj", Value: 10}
		names := []string{"ȵ_ț-object", "¼-Õ-object", "$%#!-object"}

		for _, name := range names {
			err := typed.Put(ctx, name, value)
			require.NoError(t, err)
		}

		for _, name := range names {
			result, err := typed.Get(ctx, name)
			require.NoError(t, err)
			assert.Equal(t, name, result.Name)
			assert.True(t, result.Value.IsSome())

			val, ok := result.Value.Get()
			require.True(t, ok)
			assert.Equal(t, value, val)
		}

		// Cleanup.
		cleanupTyped(t, typed, names...)
	})
}
