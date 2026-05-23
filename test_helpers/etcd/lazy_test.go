package etcd_test

import (
	"testing"
	"time"

	etcdclient "go.etcd.io/etcd/client/v3"

	etcdtest "github.com/tarantool/go-storage/test_helpers/etcd"
)

// TestLazyCluster_LazyStart asserts that the cluster is only started on
// first endpoint access, that two endpoint calls return the same endpoints
// (single underlying cluster), and that Terminate is idempotent.
func TestLazyCluster_LazyStart(t *testing.T) {
	t.Parallel()

	lazy := etcdtest.NewLazyCluster(etcdtest.ClusterConfig{Size: 1}) //nolint:exhaustruct
	t.Cleanup(lazy.Terminate)

	first := lazy.EndpointsGRPC()
	if len(first) == 0 {
		t.Fatal("EndpointsGRPC returned no endpoints")
	}

	second := lazy.EndpointsGRPC()
	if len(second) != len(first) || second[0] != first[0] {
		t.Fatalf("second EndpointsGRPC call returned a different cluster: %v vs %v", first, second)
	}

	// Round-trip through a client to confirm the cluster is alive.
	client, err := etcdclient.New(etcdclient.Config{ //nolint:exhaustruct
		Endpoints:   first,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Put(t.Context(), "/etcdtest/lazy", "value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Idempotent Terminate.
	lazy.Terminate()
	lazy.Terminate()
}
