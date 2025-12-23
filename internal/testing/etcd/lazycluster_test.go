package etcd_test

import (
	"testing"

	"github.com/tarantool/go-storage/internal/testing/etcd"
)

func TestNewLazyCluster(t *testing.T) {
	t.Parallel()

	lcInstance := etcd.NewLazyCluster()
	if lcInstance == nil {
		t.Fatal("NewLazyCluster returned nil")
	}

	// Terminate should not panic even if cluster not initialized.
	lcInstance.Terminate()
}
