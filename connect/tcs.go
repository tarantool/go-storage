package connect

import (
	"context"
	"fmt"

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

	conn, connErr := pool.Connect(ctx, instances)
	if connErr != nil {
		return nil, nil, fmt.Errorf("%w: %w", errFailedTarantool, connErr)
	}

	wrapper := pool.NewConnectorAdapter(conn, pool.RW)

	err = probeTCSConnection(ctx, wrapper)
	if err != nil {
		_ = wrapper.Close()

		return nil, nil, err
	}

	return wrapper, func() { _ = wrapper.Close() }, nil
}

func probeTCSConnection(ctx context.Context, conn tcsdriver.DoerWatcher) error {
	req := tarantool.NewCallRequest("config.storage.get").
		Args([]any{"/"}).
		Context(ctx)

	_, err := conn.Do(req).GetResponse()
	if err != nil {
		return fmt.Errorf("%w: %w", errFailedTarantoolProbe, err)
	}

	return nil
}
