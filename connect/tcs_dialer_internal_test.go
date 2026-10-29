package connect

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tlsdialer/v2"
)

func TestNewDialerForAddress_SSLEnabled(t *testing.T) {
	t.Parallel()

	dialer := newDialerForAddress(Config{ //nolint:exhaustruct
		Username: "client",
		Password: "secret",
		SSL: SSLConfig{ //nolint:exhaustruct
			Enable:       true,
			CaFile:       "/path/to/ca.crt",
			CertFile:     "/path/to/cert.pem",
			KeyFile:      "/path/to/key.pem",
			Ciphers:      "ECDHE-RSA-AES256-GCM-SHA384",
			Password:     "keypass",
			PasswordFile: "/path/to/password.txt",
		},
	}, "localhost:3301")

	tlsDialer, ok := dialer.(tlsdialer.TLSDialer)
	require.True(t, ok, "expected tlsdialer.TLSDialer, got %T", dialer)

	// Every SSL setting must reach the TLS dialer, whatever the backend is.
	assert.Equal(t, "localhost:3301", tlsDialer.Address)
	assert.Equal(t, tarantool.AutoAuth, tlsDialer.Auth)
	assert.Equal(t, "client", tlsDialer.User)
	assert.Equal(t, "secret", tlsDialer.Password)
	assert.Equal(t, "/path/to/ca.crt", tlsDialer.SslCaFile)
	assert.Equal(t, "/path/to/cert.pem", tlsDialer.SslCertFile)
	assert.Equal(t, "/path/to/key.pem", tlsDialer.SslKeyFile)
	assert.Equal(t, "ECDHE-RSA-AES256-GCM-SHA384", tlsDialer.SslCiphers)
	assert.Equal(t, "keypass", tlsDialer.SslPassword)
	assert.Equal(t, "/path/to/password.txt", tlsDialer.SslPasswordFile)

	require.NotNil(t, tlsDialer.Backend)
	assert.Equal(t, wantTLSBackendPkg, reflect.TypeOf(tlsDialer.Backend).PkgPath())
}

func TestNewDialerForAddress_SSLDisabled(t *testing.T) {
	t.Parallel()

	dialer := newDialerForAddress(Config{ //nolint:exhaustruct
		Username: "client",
		Password: "secret",
		SSL:      SSLConfig{CaFile: "/path/to/ca.crt"}, //nolint:exhaustruct
	}, "localhost:3301")

	netDialer, ok := dialer.(*tarantool.NetDialer)
	require.True(t, ok, "expected *tarantool.NetDialer, got %T", dialer)
	assert.Equal(t, "localhost:3301", netDialer.Address)
	assert.Equal(t, "client", netDialer.User)
	assert.Equal(t, "secret", netDialer.Password)
}
