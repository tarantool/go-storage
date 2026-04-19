package connect_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/connect"
)

func TestNewStorage_NoEndpoints(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewStorage(ctx, connect.Config{}) //nolint:exhaustruct
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrNoEndpoint)
}

func TestNewEtcdStorage_NoEndpoints(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{}) //nolint:exhaustruct
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrNoEndpoint)
}

func TestNewEtcdStorage_InvalidSSLConfig_CertWithoutKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CertFile: "/path/to/cert.pem",
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrInvalidSSLConfig)
}

func TestNewEtcdStorage_InvalidSSLConfig_KeyWithoutCert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:  true,
			KeyFile: "/path/to/key.pem",
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrInvalidSSLConfig)
}

func TestNewEtcdStorage_SSLEnableFalse_IgnoresSSLFields(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			CertFile: "/path/to/cert.pem",
			KeyFile:  "/path/to/key.pem",
		},
	})
	require.Error(t, err)
	assert.NotErrorIs(t, err, connect.ErrInvalidSSLConfig)
}

func TestNewEtcdStorage_SSLEnableTrue_NoSSLFields(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL:         connect.SSLConfig{Enable: true}, //nolint:exhaustruct
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_DefaultDialTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
	})
	require.Error(t, err)
}

func TestNewTCSStorage_InvalidSSLConfig_CertWithoutKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:3301"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CertFile: "/path/to/cert.pem",
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrInvalidSSLConfig)
}

func TestNewTCSStorage_InvalidSSLConfig_KeyWithoutCert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:3301"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:  true,
			KeyFile: "/path/to/key.pem",
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrInvalidSSLConfig)
}

func TestNewTCSStorage_SSLEnableFalse_IgnoresSSLFields(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:3301"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			CertFile: "/path/to/cert.pem",
		},
	})
	require.Error(t, err)
	assert.NotErrorIs(t, err, connect.ErrInvalidSSLConfig)
}

func TestNewEtcdStorage_CAFileNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaFile: "/nonexistent/path/to/ca.crt",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_CAPathNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaPath: "/nonexistent/path/to/ca/",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_InvalidCertFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CertFile: "/nonexistent/path/to/cert.pem",
			KeyFile:  "/nonexistent/path/to/key.pem",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithVerifyPeer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:     true,
			VerifyPeer: true,
			CaFile:     "/nonexistent/ca.crt",
			CertFile:   "/nonexistent/cert.pem",
			KeyFile:    "/nonexistent/key.pem",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithVerifyHost(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:     true,
			VerifyHost: true,
			CaFile:     "/nonexistent/ca.crt",
			CertFile:   "/nonexistent/cert.pem",
			KeyFile:    "/nonexistent/key.pem",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithCipher(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:  true,
			Ciphers: "ECDHE-RSA-AES256-GCM-SHA384",
			CaFile:  "/nonexistent/ca.crt",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithUnknownCipher(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:  true,
			Ciphers: "FAKE-CIPHER-NAME",
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FAKE-CIPHER-NAME")
}

func TestNewEtcdStorage_SSLWithPassword(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			Password: "secret",
			CaFile:   "/nonexistent/ca.crt",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithPasswordFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:2379"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:       true,
			PasswordFile: "/nonexistent/password.txt",
			CaFile:       "/nonexistent/ca.crt",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithEmptyCADirectory(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaPath: tmpDir,
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithCADirectoryContainingSubdirs(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	subdir := filepath.Join(tmpDir, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0750))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaPath: tmpDir,
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithCADirectoryContainingInvalidCert(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	invalidCert := filepath.Join(tmpDir, "invalid.crt")
	require.NoError(t, os.WriteFile(invalidCert, []byte("not a valid certificate"), 0600))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaPath: tmpDir,
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithBothCaFileAndCaPath(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaFile: "/nonexistent/ca.crt",
			CaPath: tmpDir,
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithVerifyHostAndPeer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:     true,
			VerifyHost: true,
			VerifyPeer: true,
			CaFile:     "/nonexistent/ca.crt",
			CertFile:   "/nonexistent/cert.pem",
			KeyFile:    "/nonexistent/key.pem",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_SSLWithInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  "/nonexistent/key.pem",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_UnreachableEndpoint(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"127.0.0.1:12345"},
		DialTimeout: 1 * time.Second,
	})
	require.Error(t, err)
}

func TestNewStorage_BothFail(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, _, err := connect.NewStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"127.0.0.1:12345"},
		DialTimeout: 1 * time.Second,
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_EncryptedKeyWithWrongPassword(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller information")

	tlsDir := filepath.Join(filepath.Dir(filename), "testdata", "tls")

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CertFile: filepath.Join(tlsDir, "localhost.crt"),
			KeyFile:  filepath.Join(tlsDir, "localhost-encrypted.key"),
			Password: "wrongpassword",
		},
	})
	require.Error(t, err)
}

func TestNewEtcdStorage_EncryptedKeyWithInvalidKeyFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	invalidKey := filepath.Join(tmpDir, "invalid.key")
	require.NoError(t, os.WriteFile(invalidKey, []byte("not a valid PEM block"), 0600))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller information")

	tlsDir := filepath.Join(filepath.Dir(filename), "testdata", "tls")

	_, _, err := connect.NewEtcdStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 1 * time.Second,
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CertFile: filepath.Join(tlsDir, "localhost.crt"),
			KeyFile:  invalidKey,
			Password: "testpass",
		},
	})
	require.Error(t, err)
}

func TestNewTCSStorage_NoEndpoints(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{}) //nolint:exhaustruct
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrNoEndpoint)
}
