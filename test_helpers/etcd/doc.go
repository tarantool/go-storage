// Package etcd provides a reusable, embedded single-node etcd cluster for
// integration tests.
//
// Importers are expected to alias the package to etcdtest to disambiguate it
// from sibling packages named etcd (notably driver/etcd):
//
//	import etcdtest "github.com/tarantool/go-storage/test_helpers/etcd"
//
// The helper is backed by go.etcd.io/etcd/server/v3/embed (the etcd server
// library) rather than go.etcd.io/etcd/tests/v3. The test framework drags in a
// v1 build of github.com/grpc-ecosystem/go-grpc-middleware, which in turn pins
// the pre-split google.golang.org/genproto monolith; that collides with modern
// genproto/googleapis/{api,rpc} and breaks plain module-mode builds with an
// "ambiguous import" error. embed imports none of that, so a helper built on it
// keeps the offending dependency out of consumers' module graphs.
//
// # TLS semantics
//
// When ClusterConfig.ClientTLS is set, the embedded server listens for clients
// over TLS and EndpointsGRPC/EndpointsHTTP return https:// URLs; a clientv3
// dialer must be configured with matching TLS. This differs from the historical
// tests/v3-based helper, which mapped certificates to the peer listener while
// dialing clients in plaintext. Set ClientTLS (not just PeerTLS) when you want
// the client connection itself to be encrypted.
package etcd
