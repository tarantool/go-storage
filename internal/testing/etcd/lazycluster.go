package etcd

import (
	"net/http"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	etcdintegration "go.etcd.io/etcd/tests/v3/framework/integration"
	etcdlazy "go.etcd.io/etcd/tests/v3/integration"
)

// LazyCluster is an alias for etcdlazy.LazyCluster.
type LazyCluster = etcdlazy.LazyCluster

type lazyCluster struct {
	cfg       etcdintegration.ClusterConfig
	cluster   *etcdintegration.Cluster
	transport *http.Transport
	once      sync.Once
	tb        testutil.TB
	closer    func()
}

var _ LazyCluster = (*lazyCluster)(nil)

// NewLazyCluster returns a new test cluster handler that gets created on the
// first call to GetEndpoints() or GetTransport().
// This implementation uses a no-op TB to skip logging.
func NewLazyCluster() LazyCluster {
	return NewLazyClusterWithConfig(etcdintegration.ClusterConfig{Size: 1}) //nolint:exhaustruct
}

// NewLazyClusterWithConfig returns a new test cluster handler that gets created
// on the first call to GetEndpoints() or GetTransport().
// This implementation uses a no-op TB to skip logging.
func NewLazyClusterWithConfig(cfg etcdintegration.ClusterConfig) LazyCluster {
	tb, closer := NewNoopTB("lazy_cluster")

	return &lazyCluster{
		cfg:       cfg,
		cluster:   nil,
		transport: nil,
		once:      sync.Once{},
		tb:        tb,
		closer:    closer,
	}
}

func (c *lazyCluster) Terminate() {
	if c == nil {
		return
	}

	if c.cluster != nil {
		c.cluster.Terminate(nil)

		c.cluster = nil
	}

	if c.closer != nil {
		c.closer()
	}
}

func (c *lazyCluster) EndpointsHTTP() []string {
	return []string{c.Cluster().Members[0].URL()}
}

func (c *lazyCluster) EndpointsGRPC() []string {
	return c.Cluster().Client(0).Endpoints()
}

func (c *lazyCluster) Cluster() *etcdintegration.Cluster {
	c.mustLazyInit()

	return c.cluster
}

func (c *lazyCluster) Transport() *http.Transport {
	c.mustLazyInit()

	return c.transport
}

func (c *lazyCluster) TB() testutil.TB {
	return c.tb
}

func (c *lazyCluster) mustLazyInit() {
	c.once.Do(func() {
		var err error

		c.transport, err = transport.NewTransport(transport.TLSInfo{}, time.Second) //nolint:exhaustruct
		if err != nil {
			// Since we cannot log, we panic as there's no other way to report the error.
			panic(err)
		}

		c.cluster = etcdintegration.NewCluster(c.tb, &c.cfg)
	})
}
