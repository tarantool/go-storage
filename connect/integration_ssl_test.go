//go:build integration

//nolint:paralleltest
package connect_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
	"github.com/tarantool/go-tlsdialer/v2"
	"github.com/tarantool/go-tlsdialer/v2/backend/gostls"

	"github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/connect"
	"github.com/tarantool/go-storage/v2/operation"
)

// These tests exercise TLS to a live TCS and run with whatever TLS backend the
// test binary was built with: the pure-Go gostls one by default and the OpenSSL
// one with the openssl build tag.

const (
	sslTCSUser     = "client"
	sslTCSPassword = "secret"
	// The password of testdata/tls/localhost-encrypted-pkcs8.key. The PKCS#8
	// format is used because the gostls backend cannot read legacy encrypted
	// PKCS#1 keys.
	sslKeyPassword = "testpass"
)

// sslTCSConfig is the config of a TCS that listens on a TLS socket. The server
// certificate is signed by testdata/tls/ca.crt. With mutualTLS the server also
// requires a client certificate signed by the same CA.
const sslTCSConfig = `
credentials:
  users:
    replicator:
      password: "topsecret"
      roles: [replication]
    %[1]s:
      password: "%[2]s"
      privileges:
        - permissions: [execute]
          universe: true
        - permissions: [read, write]
          spaces: [config_storage, config_storage_meta]

iproto:
  advertise:
    peer:
      login: replicator

replication:
  failover: election

database:
  use_mvcc_engine: true

groups:
  group-001:
    replicasets:
      replicaset-001:
        roles: [config.storage]
        roles_cfg:
          config_storage:
            status_check_interval: 3
        instances:
          master:
            iproto:
              listen:
                - uri: "127.0.0.1:%[3]d"
                  params:
                    transport: ssl
                    ssl_cert_file: "%[4]s"
                    ssl_key_file: "%[5]s"
%[6]s
`

// startSSLTCS starts a TCS with a TLS iproto socket and returns its endpoint.
func startSSLTCS(t *testing.T, mutualTLS bool) string {
	t.Helper()

	if !haveTCS {
		t.Skip("TCS is unsupported or Tarantool isn't found")
	}

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp4", "127.0.0.1:0") //nolint:exhaustruct
	require.NoError(t, err)

	port := listener.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert
	require.NoError(t, listener.Close())

	dir := tlsDir(t)
	extra := ""

	if mutualTLS {
		extra = fmt.Sprintf(`                    ssl_ca_file: "%s"`, filepath.Join(dir, "ca.crt"))
	}

	workDir := t.TempDir()
	configFile := filepath.Join(workDir, "config.yaml")
	config := fmt.Sprintf(sslTCSConfig, sslTCSUser, sslTCSPassword, port,
		filepath.Join(dir, "localhost.crt"), filepath.Join(dir, "localhost.key"), extra)
	require.NoError(t, os.WriteFile(configFile, []byte(config), 0o600))

	// The dialer is used by the helper only to wait until the instance is up.
	readyDialer := tlsdialer.TLSDialer{ //nolint:exhaustruct
		Address:     fmt.Sprintf("127.0.0.1:%d", port),
		Auth:        tarantool.AutoAuth,
		User:        sslTCSUser,
		Password:    sslTCSPassword,
		SslCaFile:   filepath.Join(dir, "ca.crt"),
		SslCertFile: filepath.Join(dir, "localhost.crt"),
		SslKeyFile:  filepath.Join(dir, "localhost.key"),
		Backend:     gostls.New(),
	}

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{ //nolint:exhaustruct
		WorkDir:      workDir,
		ConfigFile:   configFile,
		InstanceName: "master",
		Listen:       fmt.Sprintf("127.0.0.1:%d", port),
		WaitStart:    500 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
		Dialer:       readyDialer,
	})
	require.NoError(t, err)

	t.Cleanup(func() { test_helpers.StopTarantool(inst) })

	return fmt.Sprintf("127.0.0.1:%d", port)
}

func sslTCSConfigFor(endpoint string, ssl connect.SSLConfig) connect.Config {
	ssl.Enable = true

	return connect.Config{
		Endpoints:   []string{endpoint},
		Username:    sslTCSUser,
		Password:    sslTCSPassword,
		SSL:         ssl,
		DialTimeout: 3 * time.Second,
	}
}

// requirePutGet checks that the connection actually works: a value written
// through it can be read back.
func requirePutGet(t *testing.T, cfg connect.Config) {
	t.Helper()

	ctx := context.Background()

	stor, cleanup, err := connect.NewTCSStorage(ctx, cfg)
	require.NoError(t, err)

	defer cleanup()

	prefix := "/" + t.Name() + "/test"
	key := []byte(prefix + "/value")

	_, err = stor.Tx(ctx).Then(operation.Put(key, []byte("tls"))).Commit()
	require.NoError(t, err)

	kvs, err := stor.Range(ctx, storage.WithPrefix(prefix))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	assert.Equal(t, []byte("tls"), kvs[0].Value)
}

// writeOtherCA writes a self-signed CA certificate unrelated to testdata/tls and
// returns the path to it.
func writeOtherCA(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{ //nolint:exhaustruct
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "other-test-CA"}, //nolint:exhaustruct
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "other-ca.crt")
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}) //nolint:exhaustruct
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

func TestNewTCSStorage_SSL(t *testing.T) {
	endpoint := startSSLTCS(t, false)
	dir := tlsDir(t)

	t.Run("CaFile", func(t *testing.T) {
		requirePutGet(t, sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile: filepath.Join(dir, "ca.crt"),
		}))
	})

	t.Run("Ciphers", func(t *testing.T) {
		requirePutGet(t, sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile:  filepath.Join(dir, "ca.crt"),
			Ciphers: "ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-GCM-SHA256",
		}))
	})

	t.Run("UnknownCipher", func(t *testing.T) {
		cfg := sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile:  filepath.Join(dir, "ca.crt"),
			Ciphers: "NO-SUCH-CIPHER",
		})

		_, _, err := connect.NewTCSStorage(context.Background(), cfg)
		require.Error(t, err)
	})

	t.Run("CaFileNotFound", func(t *testing.T) {
		cfg := sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile: filepath.Join(dir, "no-such-ca.crt"),
		})

		_, _, err := connect.NewTCSStorage(context.Background(), cfg)
		require.Error(t, err)
	})

	t.Run("UntrustedServer", func(t *testing.T) {
		// A CA that did not sign the server certificate.
		cfg := sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile: writeOtherCA(t),
		})

		_, _, err := connect.NewTCSStorage(context.Background(), cfg)
		require.Error(t, err)
	})

	t.Run("PlainClient", func(t *testing.T) {
		cfg := sslTCSConfigFor(endpoint, connect.SSLConfig{}) //nolint:exhaustruct

		cfg.SSL.Enable = false
		cfg.DialTimeout = time.Second

		_, _, err := connect.NewTCSStorage(context.Background(), cfg)
		require.Error(t, err)
	})
}

func TestNewTCSStorage_SSLClientCert(t *testing.T) {
	endpoint := startSSLTCS(t, true)
	dir := tlsDir(t)

	t.Run("Key", func(t *testing.T) {
		requirePutGet(t, sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile:   filepath.Join(dir, "ca.crt"),
			CertFile: filepath.Join(dir, "localhost.crt"),
			KeyFile:  filepath.Join(dir, "localhost.key"),
		}))
	})

	t.Run("EncryptedKeyPassword", func(t *testing.T) {
		requirePutGet(t, sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile:   filepath.Join(dir, "ca.crt"),
			CertFile: filepath.Join(dir, "localhost.crt"),
			KeyFile:  filepath.Join(dir, "localhost-encrypted-pkcs8.key"),
			Password: sslKeyPassword,
		}))
	})

	t.Run("EncryptedKeyPasswordFile", func(t *testing.T) {
		requirePutGet(t, sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile:       filepath.Join(dir, "ca.crt"),
			CertFile:     filepath.Join(dir, "localhost.crt"),
			KeyFile:      filepath.Join(dir, "localhost-encrypted-pkcs8.key"),
			PasswordFile: filepath.Join(dir, "password.txt"),
		}))
	})

	t.Run("EncryptedKeyWrongPassword", func(t *testing.T) {
		cfg := sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile:   filepath.Join(dir, "ca.crt"),
			CertFile: filepath.Join(dir, "localhost.crt"),
			KeyFile:  filepath.Join(dir, "localhost-encrypted-pkcs8.key"),
			Password: "wrong",
		})

		_, _, err := connect.NewTCSStorage(context.Background(), cfg)
		require.Error(t, err)
	})

	t.Run("NoClientCert", func(t *testing.T) {
		cfg := sslTCSConfigFor(endpoint, connect.SSLConfig{ //nolint:exhaustruct
			CaFile: filepath.Join(dir, "ca.crt"),
		})

		_, _, err := connect.NewTCSStorage(context.Background(), cfg)
		require.Error(t, err)
	})
}
