//go:build integration && go_storage_ssl

package connect_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/connect"
)

func TestNewTCSStorage_SSLEnabled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:3301"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable:   true,
			CaFile:   "/path/to/ca.crt",
			CertFile: "/path/to/cert.pem",
			KeyFile:  "/path/to/key.pem",
		},
	})
	require.Error(t, err)
}
