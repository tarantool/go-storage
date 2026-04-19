//go:build integration

//nolint:paralleltest
package connect_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcshelper "github.com/tarantool/go-tarantool/v2/test_helpers/tcs"
	"go.etcd.io/etcd/client/pkg/v3/transport" //nolint:depguard
	etcdfintegration "go.etcd.io/etcd/tests/v3/framework/integration"
	etcdlazy "go.etcd.io/etcd/tests/v3/integration"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/connect"
	etcdtesting "github.com/tarantool/go-storage/internal/testing/etcd"
	"github.com/tarantool/go-storage/operation"
)

var haveTCS bool          //nolint:gochecknoglobals
var tcsEndpoints []string //nolint:gochecknoglobals

func TestMain(m *testing.M) {
	flag.Parse()

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

	code := m.Run()

	if haveTCS {
		tcsInstance.Stop()
	}

	os.Exit(code)
}

func createEtcdTestConfig(t *testing.T) connect.Config {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	etcdfintegration.BeforeTest(etcdtesting.NewSilentTB(t), etcdfintegration.WithoutGoLeakDetection())

	cluster := etcdtesting.NewLazyCluster()

	t.Cleanup(func() { cluster.Terminate() })

	return connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
	}
}

func TestNewEtcdStorage_PutAndGet(t *testing.T) {
	cfg := createEtcdTestConfig(t)

	ctx := context.Background()
	stor, cancel, err := connect.NewEtcdStorage(ctx, cfg)
	require.NoError(t, err)

	defer cancel()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("hello"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, key, kvs[0].Key)
	assert.Equal(t, []byte("hello"), kvs[0].Value)
}

func TestNewTCSStorage_PutAndGet(t *testing.T) {
	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
	}

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	ctx := context.Background()
	stor, cancel, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: tcsEndpoints,
		Username:  "client",
		Password:  "secret",
	})
	require.NoError(t, err)

	defer cancel()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, _ = stor.Tx(ctx).Then(operation.Delete(key)).Commit()

	defer func() { _, _ = stor.Tx(ctx).Then(operation.Delete(key)).Commit() }()

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("hello"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, key, kvs[0].Key)
	assert.Equal(t, []byte("hello"), kvs[0].Value)
}

func TestNewStorage_AutoDetect_Etcd(t *testing.T) {
	cfg := createEtcdTestConfig(t)

	ctx := context.Background()
	stor, cancel, err := connect.NewStorage(ctx, cfg)
	require.NoError(t, err)

	defer cancel()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("auto"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("auto"), kvs[0].Value)
}

func TestNewStorage_AutoDetect_TCS(t *testing.T) {
	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
	}

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	ctx := context.Background()

	stor, cancel, err := connect.NewStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: tcsEndpoints,
		Username:  "client",
		Password:  "secret",
	})
	require.NoError(t, err)

	defer cancel()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, _ = stor.Tx(ctx).Then(operation.Delete(key)).Commit()

	defer func() { _, _ = stor.Tx(ctx).Then(operation.Delete(key)).Commit() }()

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("auto-tcs"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("auto-tcs"), kvs[0].Value)
}

func tlsDir(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get caller information")
	}

	dir := filepath.Dir(filename)

	return filepath.Join(dir, "testdata", "tls")
}

func createEtcdTLSCluster(t *testing.T) etcdlazy.LazyCluster {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	etcdfintegration.BeforeTest(etcdtesting.NewSilentTB(t), etcdfintegration.WithoutGoLeakDetection())

	tlsDir := tlsDir(t)
	peerTLS := &transport.TLSInfo{ //nolint:exhaustruct
		CertFile:      filepath.Join(tlsDir, "localhost.crt"),
		KeyFile:       filepath.Join(tlsDir, "localhost.key"),
		TrustedCAFile: filepath.Join(tlsDir, "ca.crt"),
	}
	clientTLS := &transport.TLSInfo{ //nolint:exhaustruct
		CertFile:           filepath.Join(tlsDir, "localhost.crt"),
		KeyFile:            filepath.Join(tlsDir, "localhost.key"),
		InsecureSkipVerify: true,
	}

	cfg := etcdfintegration.ClusterConfig{ //nolint:exhaustruct
		Size:      1,
		PeerTLS:   peerTLS,
		ClientTLS: clientTLS,
		UseTCP:    true,
	}

	cluster := etcdlazy.NewLazyClusterWithConfig(cfg)

	t.Cleanup(func() { cluster.Terminate() })

	return cluster
}

func TestNewEtcdStorage_WithTLSCaFile(t *testing.T) {
	cluster := createEtcdTLSCluster(t)
	tlsDir := tlsDir(t)

	ctx := context.Background()
	stor, cleanup, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaFile: filepath.Join(tlsDir, "ca.crt"),
		},
	})
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("tls-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("tls-test"), kvs[0].Value)
}

func TestNewEtcdStorage_WithTLSCaPath(t *testing.T) {
	cluster := createEtcdTLSCluster(t)
	tlsDir := tlsDir(t)

	ctx := context.Background()
	stor, cleanup, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaPath: filepath.Join(tlsDir, "only_ca"),
		},
	})
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("tls-path-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("tls-path-test"), kvs[0].Value)
}

func TestNewEtcdStorage_WithTLSClientCert(t *testing.T) {
	cluster := createEtcdTLSCluster(t)
	tlsDir := tlsDir(t)

	ctx := context.Background()
	stor, cleanup, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CaFile:   filepath.Join(tlsDir, "ca.crt"),
			CertFile: filepath.Join(tlsDir, "localhost.crt"),
			KeyFile:  filepath.Join(tlsDir, "localhost.key"),
		},
	})
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("tls-cert-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("tls-cert-test"), kvs[0].Value)
}

func TestNewEtcdStorage_WithTLSVerifyHost(t *testing.T) {
	cluster := createEtcdTLSCluster(t)
	tlsDir := tlsDir(t)

	ctx := context.Background()
	stor, cleanup, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:     true,
			CaFile:     filepath.Join(tlsDir, "ca.crt"),
			VerifyHost: true,
		},
	})
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("verify-host-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("verify-host-test"), kvs[0].Value)
}

func TestNewEtcdStorage_WithCustomDialTimeout(t *testing.T) {
	cfg := createEtcdTestConfig(t)

	cfg.DialTimeout = 10 * time.Second

	ctx := context.Background()
	stor, cleanup, err := connect.NewEtcdStorage(ctx, cfg)
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("timeout-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("timeout-test"), kvs[0].Value)
}

func TestNewEtcdStorage_CanceledContext(t *testing.T) {
	cfg := createEtcdTestConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := connect.NewEtcdStorage(ctx, cfg)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewTCSStorage_CanceledContext(t *testing.T) {
	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: tcsEndpoints,
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_WithTLSEncryptedKeyAndPassword(t *testing.T) {
	cluster := createEtcdTLSCluster(t)
	tlsDir := tlsDir(t)

	ctx := context.Background()
	stor, cleanup, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CaFile:   filepath.Join(tlsDir, "ca.crt"),
			CertFile: filepath.Join(tlsDir, "localhost.crt"),
			KeyFile:  filepath.Join(tlsDir, "localhost-encrypted.key"),
			Password: "testpass",
		},
	})
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("encrypted-key-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("encrypted-key-test"), kvs[0].Value)
}

func TestNewEtcdStorage_WithTLSEncryptedKeyAndPasswordFile(t *testing.T) {
	cluster := createEtcdTLSCluster(t)
	tlsDir := tlsDir(t)

	ctx := context.Background()
	stor, cleanup, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:       true,
			CaFile:       filepath.Join(tlsDir, "ca.crt"),
			CertFile:     filepath.Join(tlsDir, "localhost.crt"),
			KeyFile:      filepath.Join(tlsDir, "localhost-encrypted.key"),
			PasswordFile: filepath.Join(tlsDir, "password.txt"),
		},
	})
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("password-file-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("password-file-test"), kvs[0].Value)
}
