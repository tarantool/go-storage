//go:build !openssl && !go_storage_ssl

package connect

import (
	"github.com/tarantool/go-tlsdialer/v2"
	"github.com/tarantool/go-tlsdialer/v2/backend/gostls"
)

// newTLSBackend returns the pure-Go TLS backend, which needs neither cgo nor
// OpenSSL. Build with the openssl tag to use the OpenSSL backend instead.
func newTLSBackend() tlsdialer.Backend {
	return gostls.New()
}
