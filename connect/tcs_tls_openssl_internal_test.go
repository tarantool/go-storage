//go:build openssl || go_storage_ssl

package connect

// wantTLSBackendPkg is the package of the TLS backend expected in this build.
const wantTLSBackendPkg = "github.com/tarantool/go-tlsdialer/v2/backend/openssl"
