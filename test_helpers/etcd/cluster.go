package etcd

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// readyTimeout bounds how long startup waits for the embedded server to
// become ready before failing.
const readyTimeout = 30 * time.Second

// startAttempts is the maximum number of (port-snapshot → start) attempts
// before startCluster gives up. freeURL snapshots an ephemeral port and
// closes the listener so embed can rebind it; under heavy parallelism the
// kernel can hand the same port to another process between close and
// rebind, surfacing as "bind: address already in use". A few retries are
// enough to make this practically nonflaky even under -race -count=100.
const startAttempts = 8

// ClusterConfig configures the embedded cluster. It is the helper's own type;
// it intentionally does not re-export any tests/v3 type.
type ClusterConfig struct {
	// Size is the number of nodes. Only 1 is supported; 0 is treated as 1.
	Size int

	// ClientTLS, when non-nil, makes the server serve clients over TLS. The
	// returned endpoints become https:// and clientv3 must dial with matching
	// TLS settings.
	ClientTLS *transport.TLSInfo

	// PeerTLS, when non-nil, secures the peer (inter-node) listener. For a
	// single node it has no functional effect but is honored for parity.
	PeerTLS *transport.TLSInfo
}

// Cluster is a running embedded etcd cluster handle.
type Cluster struct {
	etcd          *embed.Etcd
	dataDir       string // non-empty only when the helper owns the directory.
	endpointsGRPC []string
	endpointsHTTP []string
}

// New starts an embedded single-node etcd cluster and registers its shutdown
// via tb.Cleanup. It fails the test on any startup error.
func New(tb testing.TB, cfg ClusterConfig) *Cluster {
	tb.Helper()

	cluster, err := startCluster(cfg, tb.TempDir())
	if err != nil {
		tb.Fatalf("etcdtest: %v", err)
	}

	tb.Cleanup(cluster.Terminate)

	return cluster
}

// EndpointsGRPC returns the client endpoints to pass to clientv3.
func (c *Cluster) EndpointsGRPC() []string {
	if c == nil {
		return nil
	}

	return c.endpointsGRPC
}

// EndpointsHTTP returns the client endpoints as HTTP(S) URLs.
func (c *Cluster) EndpointsHTTP() []string {
	if c == nil {
		return nil
	}

	return c.endpointsHTTP
}

// Terminate stops the embedded server and, when the helper owns the data
// directory, removes it. It is safe to call multiple times.
func (c *Cluster) Terminate() {
	if c == nil {
		return
	}

	if c.etcd != nil {
		c.etcd.Close()

		c.etcd = nil
	}

	if c.dataDir != "" {
		_ = os.RemoveAll(c.dataDir)

		c.dataDir = ""
	}
}

// errMultiNode and errNotReady are returned by startCluster on the failure
// paths that don't wrap an external error. Declared as sentinels so err113
// stays satisfied and callers can errors.Is them if needed.
var (
	errMultiNode = errors.New("multi-node clusters are not supported")
	errNotReady  = errors.New("embedded etcd did not become ready")
)

// startCluster spins up an embedded etcd in dataDir. The returned Cluster's
// dataDir field is left empty (the caller — typically tb.TempDir-based —
// owns cleanup). LazyCluster overrides this so its own MkdirTemp is removed
// in Terminate.
//
// Startup is retried up to startAttempts times on "address already in use"
// because freeURL hands out a port the kernel is free to re-issue before
// embed binds it (see freeURL).
func startCluster(cfg ClusterConfig, dataDir string) (*Cluster, error) {
	if cfg.Size > 1 {
		return nil, fmt.Errorf("%w (Size=%d)", errMultiNode, cfg.Size)
	}

	var lastErr error

	for attempt := 1; attempt <= startAttempts; attempt++ {
		cluster, err := startClusterOnce(cfg, dataDir)
		if err == nil {
			return cluster, nil
		}

		if !isAddressInUse(err) {
			return nil, err
		}

		lastErr = err
	}

	return nil, fmt.Errorf("start embedded etcd after %d attempts: %w", startAttempts, lastErr)
}

func startClusterOnce(cfg ClusterConfig, dataDir string) (*Cluster, error) {
	clientScheme, peerScheme := "http", "http"
	if cfg.ClientTLS != nil {
		clientScheme = "https"
	}

	if cfg.PeerTLS != nil {
		peerScheme = "https"
	}

	clientURL, err := freeURL(clientScheme)
	if err != nil {
		return nil, fmt.Errorf("reserve client URL: %w", err)
	}

	peerURL, err := freeURL(peerScheme)
	if err != nil {
		return nil, fmt.Errorf("reserve peer URL: %w", err)
	}

	embedCfg := embed.NewConfig()

	embedCfg.Name = "etcdtest"
	embedCfg.Dir = dataDir
	embedCfg.ListenClientUrls = []url.URL{clientURL}
	embedCfg.AdvertiseClientUrls = []url.URL{clientURL}
	embedCfg.ListenPeerUrls = []url.URL{peerURL}
	embedCfg.AdvertisePeerUrls = []url.URL{peerURL}
	embedCfg.InitialCluster = embedCfg.InitialClusterFromName(embedCfg.Name)

	// Keep the test output clean: only surface fatal server problems.
	embedCfg.Logger = "zap"
	embedCfg.LogLevel = "error"
	embedCfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())

	if cfg.ClientTLS != nil {
		embedCfg.ClientTLSInfo = *cfg.ClientTLS
	}

	if cfg.PeerTLS != nil {
		embedCfg.PeerTLSInfo = *cfg.PeerTLS
	}

	etcd, err := embed.StartEtcd(embedCfg)
	if err != nil {
		return nil, fmt.Errorf("start embedded etcd: %w", err)
	}

	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(readyTimeout):
		etcd.Close()

		return nil, fmt.Errorf("%w within %s", errNotReady, readyTimeout)
	}

	return &Cluster{
		etcd:          etcd,
		dataDir:       "", // caller owns dataDir cleanup.
		endpointsGRPC: []string{clientURL.String()},
		endpointsHTTP: []string{clientURL.String()},
	}, nil
}

// isAddressInUse reports whether err is a "bind: address already in use"
// syscall error somewhere in its wrap chain. embed wraps it deeply, so a
// substring check is the most portable option here.
func isAddressInUse(err error) bool {
	return err != nil && strings.Contains(err.Error(), "address already in use")
}

// freeURL reserves a free TCP port on the loopback interface and returns a
// URL with the given scheme bound to it. The listener is closed immediately;
// embed re-binds the address on startup.
func freeURL(scheme string) (url.URL, error) {
	// A bare net.Listen on a fixed-shape loopback address keeps the helper API
	// (tb-only or context-free) and avoids forcing every caller to thread a
	// context just to snapshot a free port.
	listener, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx
	if err != nil {
		return url.URL{}, fmt.Errorf("reserve a free port: %w", err)
	}

	addr := listener.Addr().String()

	err = listener.Close()
	if err != nil {
		return url.URL{}, fmt.Errorf("release reserved port: %w", err)
	}

	parsed, err := url.Parse(fmt.Sprintf("%s://%s", scheme, addr))
	if err != nil {
		return url.URL{}, fmt.Errorf("build URL: %w", err)
	}

	return *parsed, nil
}
