package etcd

import (
	"fmt"
	"os"
	"sync"
)

// LazyCluster lazily starts a single embedded etcd cluster on first endpoint
// access and is intended to be shared across many tests in a suite. Typical
// use is to instantiate it in TestMain and tear it down before os.Exit:
//
//	var shared *etcdtest.LazyCluster
//
//	func TestMain(m *testing.M) {
//	    shared = etcdtest.NewLazyCluster(etcdtest.ClusterConfig{Size: 1})
//	    code := m.Run()
//	    shared.Terminate()
//	    os.Exit(code)
//	}
//
// Unlike New, LazyCluster does not register cleanup via testing.TB — its
// lifetime spans many tests, so the caller must Terminate it explicitly.
// Startup failures panic, since there is no TB to report through.
//
// Tests sharing a LazyCluster see each other's keys; isolate writes with
// per-test prefixes or explicit cleanup.
type LazyCluster struct {
	cfg ClusterConfig

	once    sync.Once
	cluster *Cluster
	initErr error
}

// NewLazyCluster returns a handle to a cluster that is not started until
// EndpointsGRPC or EndpointsHTTP is first called.
func NewLazyCluster(cfg ClusterConfig) *LazyCluster {
	return &LazyCluster{cfg: cfg} //nolint:exhaustruct
}

// EndpointsGRPC starts the cluster on first call and returns clientv3
// endpoints.
func (c *LazyCluster) EndpointsGRPC() []string {
	return c.must().EndpointsGRPC()
}

// EndpointsHTTP starts the cluster on first call and returns HTTP(S) endpoints.
func (c *LazyCluster) EndpointsHTTP() []string {
	return c.must().EndpointsHTTP()
}

// Terminate stops the embedded server (if started) and removes the helper-
// owned data directory. It is safe to call multiple times and on a never-
// initialized LazyCluster.
func (c *LazyCluster) Terminate() {
	if c == nil || c.cluster == nil {
		return
	}

	c.cluster.Terminate()

	c.cluster = nil
}

func (c *LazyCluster) must() *Cluster {
	c.once.Do(func() {
		dataDir, err := os.MkdirTemp("", "etcdtest-lazy-*")
		if err != nil {
			c.initErr = fmt.Errorf("etcdtest: create temp dir: %w", err)

			return
		}

		cluster, err := startCluster(c.cfg, dataDir)
		if err != nil {
			_ = os.RemoveAll(dataDir)
			c.initErr = fmt.Errorf("etcdtest: %w", err)

			return
		}

		// LazyCluster owns the temp dir lifetime; record it on the cluster
		// so Terminate removes it.
		cluster.dataDir = dataDir
		c.cluster = cluster
	})

	if c.initErr != nil {
		panic(c.initErr)
	}

	return c.cluster
}
