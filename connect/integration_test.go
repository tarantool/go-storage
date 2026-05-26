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
	etcdclient "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/connect"
	"github.com/tarantool/go-storage/operation"
	etcdtest "github.com/tarantool/go-storage/test_helpers/etcd"
)

var haveTCS bool          //nolint:gochecknoglobals
var tcsEndpoints []string //nolint:gochecknoglobals

// sharedEtcdCluster is a single embedded etcd reused across every non-TLS,
// non-auth Test* in this package. The auth helper enables AuthEnable globally
// and TLS tests need TLS-configured listeners, so both keep their own
// per-test cluster (see createEtcdAuthTestConfig and createEtcdTLSCluster).
//
//nolint:gochecknoglobals
var sharedEtcdCluster *etcdtest.LazyCluster

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

// createEtcdTestConfig returns a connect.Config pointing at sharedEtcdCluster.
// Per-test data isolation is the caller's responsibility (tests namespace
// their keys with t.Name()).
func createEtcdTestConfig(t *testing.T) connect.Config {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	return connect.Config{ //nolint:exhaustruct
		Endpoints: sharedEtcdCluster.EndpointsGRPC(),
	}
}

// createEtcdAuthTestConfig spins up a dedicated etcd cluster because it
// enables AuthEnable globally, which would otherwise poison every other test
// sharing the cluster.
func createEtcdAuthTestConfig(t *testing.T) connect.Config {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	cluster := etcdtest.New(t, etcdtest.ClusterConfig{Size: 1}) //nolint:exhaustruct

	cfg := connect.Config{ //nolint:exhaustruct
		Endpoints: cluster.EndpointsGRPC(),
	}

	client, err := etcdclient.New(etcdclient.Config{ //nolint:exhaustruct
		Endpoints:   cfg.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.UserAdd(ctx, "root", "root")
	require.NoError(t, err)

	_, err = client.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)

	_, err = client.UserAdd(ctx, "client", "secret")
	require.NoError(t, err)

	_, err = client.Auth.RoleAdd(ctx, "client")
	require.NoError(t, err)

	_, err = client.Auth.RoleGrantPermission(ctx, "client", "/prefix/", etcdclient.GetPrefixRangeEnd("/prefix/"), etcdclient.PermissionType(etcdclient.PermReadWrite))
	require.NoError(t, err)

	_, err = client.Auth.UserGrantRole(ctx, "client", "client")
	require.NoError(t, err)

	_, err = client.AuthEnable(ctx)
	require.NoError(t, err)

	cfg.Username = "client"
	cfg.Password = "secret"

	return cfg
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

func createEtcdTLSCluster(t *testing.T) *etcdtest.Cluster {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

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

	return etcdtest.New(t, etcdtest.ClusterConfig{
		Size:      1,
		PeerTLS:   peerTLS,
		ClientTLS: clientTLS,
	})
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

func TestNewEtcdStorage_InvalidCredentials(t *testing.T) {
	cfg := createEtcdAuthTestConfig(t)

	cfg.Username = "invalid_user"
	cfg.Password = "invalid_pass"

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "authentication failed, invalid user ID or password")
}

func TestNewEtcdStorage_ValidCredentials(t *testing.T) {
	cfg := createEtcdAuthTestConfig(t)

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, cfg)
	require.NoError(t, err)
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

func TestNewTCSStorage_InvalidCredentials(t *testing.T) {
	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
	}

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	ctx := context.Background()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: tcsEndpoints,
		Username:  "invalid_user",
		Password:  "invalid_pass",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "supplied credentials are invalid")
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

func TestNewTCSStorage_HTTPEndpointScheme(t *testing.T) {
	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
	}

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	endpoints := make([]string, len(tcsEndpoints))
	for i, ep := range tcsEndpoints {
		endpoints[i] = "http://" + ep
	}

	ctx := context.Background()
	stor, cancel, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: endpoints,
		Username:  "client",
		Password:  "secret",
	})
	require.NoError(t, err)

	defer cancel()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, _ = stor.Tx(ctx).Then(operation.Delete(key)).Commit()

	defer func() { _, _ = stor.Tx(ctx).Then(operation.Delete(key)).Commit() }()

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("http-scheme-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("http-scheme-test"), kvs[0].Value)
}

func TestNewEtcdStorage_HTTPSEndpointScheme(t *testing.T) {
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

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("https-scheme-test"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	assert.Equal(t, []byte("https-scheme-test"), kvs[0].Value)
}
