package etcd_test

import (
	"testing"
	"time"

	etcdclient "go.etcd.io/etcd/client/v3"

	etcdtest "github.com/tarantool/go-storage/v2/test_helpers/etcd"
)

func TestNew_StartsAndServesClients(t *testing.T) {
	t.Parallel()

	cluster := etcdtest.New(t, etcdtest.ClusterConfig{Size: 1}) //nolint:exhaustruct

	endpoints := cluster.EndpointsGRPC()
	if len(endpoints) == 0 {
		t.Fatal("EndpointsGRPC returned no endpoints")
	}

	client, err := etcdclient.New(etcdclient.Config{ //nolint:exhaustruct
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	ctx := t.Context()

	_, err = client.Put(ctx, "/etcdtest/key", "value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	resp, err := client.Get(ctx, "/etcdtest/key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "value" {
		t.Fatalf("unexpected Get response: %+v", resp.Kvs)
	}
}

// TestTerminate_Idempotent ensures Terminate can be called repeatedly.
func TestTerminate_Idempotent(t *testing.T) {
	t.Parallel()

	cluster := etcdtest.New(t, etcdtest.ClusterConfig{Size: 1}) //nolint:exhaustruct
	cluster.Terminate()
	cluster.Terminate()
}
