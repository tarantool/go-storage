//go:build openssl || go_storage_ssl

package connect

import (
	"github.com/tarantool/go-tlsdialer/v2"
	"github.com/tarantool/go-tlsdialer/v2/backend/openssl"
)

// newTLSBackend returns the OpenSSL TLS backend (requires cgo and OpenSSL).
//
// The go_storage_ssl tag is a deprecated synonym of openssl and selects this
// backend as well.
func newTLSBackend() tlsdialer.Backend {
	return openssl.New()
}
