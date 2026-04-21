//go:build integration && !go_storage_ssl

package connect_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/connect"
)

func TestNewTCSStorage_SSLDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, _, err := connect.NewTCSStorage(ctx, connect.Config{ //nolint:exhaustruct
		Endpoints: []string{"localhost:3301"},
		SSL: connect.SSLConfig{ //nolint:exhaustruct
			Enable: true,
			CaFile: "/path/to/ca.crt",
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, connect.ErrSSLDisabled)
}
