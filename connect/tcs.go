package connect

import (
	"context"
	"fmt"
	"strings"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"

	tcsdriver "github.com/tarantool/go-storage/driver/tcs"
)

func createTCSConnection(ctx context.Context, cfg Config) (tcsdriver.DoerWatcher, CleanupFunc, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, nil, ErrNoEndpoint
	}

	err := validateSSLConfig(cfg.SSL)
	if err != nil {
		return nil, nil, err
	}

	instances := make([]pool.Instance, 0, len(cfg.Endpoints))

	for idx, addr := range cfg.Endpoints {
		addr = strings.TrimPrefix(addr, "http://")
		addr = strings.TrimPrefix(addr, "https://")

		instDialer, dialerErr := newDialerForAddress(cfg, addr)
		if dialerErr != nil {
			return nil, nil, dialerErr
		}

		instances = append(instances, pool.Instance{
			Name:   fmt.Sprintf("instance_%d", idx),
			Dialer: instDialer,
			Opts:   tarantool.Opts{}, //nolint:exhaustruct
		})
	}

	// Probe the connections to ensure that we have at least one working connection.
	//
	// Go-tarantool pool doesn't return connection failures, just logs it.
	// Check go-tarantool/v2@v2.4.1/pool/connection_pool.go:200,
	// So we need to check it ourselves.
	probeCtx, cancel := context.WithTimeout(ctx, cfg.dialTimeout())
	probeConn, err := tarantool.Connect(probeCtx, instances[0].Dialer, tarantool.Opts{ //nolint:exhaustruct
		Timeout: cfg.dialTimeout(),
	})

	cancel()

	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", errFailedTarantool, err)
	}

	_ = probeConn.Close()

	conn, connErr := pool.Connect(ctx, instances)
	if connErr != nil {
		return nil, nil, fmt.Errorf("%w: %w", errFailedTarantool, connErr)
	}

	wrapper := pool.NewConnectorAdapter(conn, pool.RW)

	return wrapper, func() { _ = wrapper.Close() }, nil
}
