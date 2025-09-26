// Package tcs provides a Tarantool Cartridge storage driver implementation.
// It enables using Tarantool as a distributed key-value storage backend.
package tcs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"

	"github.com/tarantool/go-storage/driver"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/utils"
	"github.com/tarantool/go-storage/watch"
)

// Driver is a Tarantool implementation of the storage driver interface.
// It uses TCS as the underlying key-value storage backend.
type Driver struct {
	conn *pool.ConnectionPool // Tarantool connection pool.
}

var (
	_ driver.Driver = &Driver{} //nolint:exhaustruct
)

// New creates a new Tarantool driver instance.
// It establishes connections to Tarantool instances using the provided addresses.
func New(ctx context.Context, addrs []string) (*Driver, error) {
	instances := make([]pool.Instance, 0, len(addrs))
	for i, addr := range addrs {
		instances = append(instances, pool.Instance{
			Name: fmt.Sprintf("instance-%d", i),
			Dialer: &tarantool.NetDialer{
				Address:  addr,
				User:     "user",
				Password: "password",
				RequiredProtocolInfo: tarantool.ProtocolInfo{
					Auth:     tarantool.AutoAuth,
					Version:  tarantool.ProtocolVersion(0),
					Features: nil,
				},
			},
			Opts: tarantool.Opts{
				Timeout:       0,
				Reconnect:     0,
				MaxReconnects: 0,
				RateLimit:     0,
				RLimitAction:  tarantool.RLimitAction(0),
				Concurrency:   0,
				SkipSchema:    false,
				Notify:        nil,
				Handle:        nil,
				Logger:        nil,
			},
		})
	}

	conn, err := pool.Connect(ctx, instances)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to tarantool pool: %w", err)
	}

	return &Driver{conn: conn}, nil
}

// Execute executes a transactional operation with conditional logic.
// It processes predicates to determine whether to execute thenOps or elseOps.
func (d Driver) Execute(
	ctx context.Context,
	predicates []predicate.Predicate,
	thenOps []operation.Operation,
	elseOps []operation.Operation,
) (tx.Response, error) {
	predicateToLua := func(pred predicate.Predicate) (string, error) {
		targets := [...]string{
			predicate.TargetValue:   "value",
			predicate.TargetVersion: "revision",
		}
		operators := [...]string{
			predicate.OpEqual:    "==",
			predicate.OpNotEqual: "!=",
			predicate.OpGreater:  ">",
			predicate.OpLess:     "<",
		}
		target, op := targets[pred.Target()], operators[pred.Operation()]
		switch v := pred.Value().(type) {
		case string:
			return fmt.Sprintf("{'%s', '%s', '%s'}", target, op, v), nil
		case int:
			return fmt.Sprintf("{'%s', '%s', %d}", target, op, v), nil
		default:
			return "", fmt.Errorf("unexpected type of comparison value in predicate")
		}
	}

	opToLua := func(op operation.Operation) string {
		ops := [...][]string{
			operation.TypeGet:    {"get", string(op.Key())},
			operation.TypePut:    {"put", string(op.Key()), string(op.Value())},
			operation.TypeDelete: {"delete", string(op.Key())},
		}
		return fmt.Sprintf("{ '%s' }", strings.Join(ops[op.Type()], "', '"))
	}

	luaPredicates, err := utils.TryTransformSlice(predicates, predicateToLua)
	if err != nil {
		return tx.Response{}, err
	}

	txnArg := fmt.Sprintf(
		`{ predicates={ %s }, on_success = { %s }, on_failure = { %s } }`,
		strings.Join(luaPredicates, ","),
		strings.Join(utils.TransformSlice(thenOps, opToLua), ","),
		strings.Join(utils.TransformSlice(elseOps, opToLua), ","),
	)

	req := tarantool.NewCallRequest("config.storage.txn").Args([]any{txnArg}).Context(ctx)

	var result []any
	if err := d.conn.Do(req, pool.RW).GetTyped(&result); err != nil {
		return tx.Response{}, fmt.Errorf("failed to fetch data from tarantool: %w", err)
	}
	if len(result) != 1 {
		return tx.Response{}, fmt.Errorf("unexpected response from tarantool: %q", result)
	}

	type txnResponse struct {
		data struct {
			is_success bool
			responses  []any
		}
		revision int
	}
	var resp txnResponse
	if err := mapstructure.Decode(result[0], &resp); err != nil {
		return tx.Response{}, fmt.Errorf("failed to map response from tarantool: %q", result[0])
	}

	toTxResponse := func(_ any) tx.RequestResponse {
		panic("implement me")
	}

	return tx.Response{
		Succeeded: resp.data.is_success,
		Results:   utils.TransformSlice(resp.data.responses, toTxResponse),
	}, nil
}

// Watch monitors changes to a specific key and returns a stream of events.
// It supports optional watch configuration through the opts parameter.
func (d Driver) Watch(ctx context.Context, key []byte, _ ...watch.Option) <-chan watch.Event {
	ch := make(chan watch.Event)

	callback := func(ev tarantool.WatchEvent) {
		// WatchEvent only contains revision number, so separate request is needed to get data.
		if ev.Value != nil {
			// NOTE: At the moment watcher is only used to catch the fact that value has been
			// changed and it doesn't matter if the value obtained with the subsequent request
			// has revision number other than revision that triggered this event, so for now
			// no consistency checking is implemented.
			value, err := d.get(ctx, string(key))
			if err == nil {
				ch <- watch.Event{
					Key:   key,
					Value: value,
				}
			}
		}
	}

	// To watch for config storage key "config.storage:" prefix should be used.
	watcher, err := d.conn.NewWatcher("config.storage:"+string(key), callback, pool.ANY)
	if err != nil {
		close(ch)
		return ch
	}

	go func() {
		defer close(ch)
		defer watcher.Unregister()
		for {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (d Driver) get(ctx context.Context, key string) ([]byte, error) {
	req := tarantool.NewCallRequest("config.storage.get").Args([]any{key}).Context(ctx)

	var result []any
	if err := d.conn.Do(req, pool.ANY).GetTyped(&result); err != nil {
		return nil, fmt.Errorf("failed to fetch data from tarantool: %w", err)
	}
	if len(result) != 1 {
		return nil, fmt.Errorf("unexpected response from tarantool: %q", result)
	}

	// getResponse is a response of a config.storage.get request.
	type getResponse struct {
		Data []struct {
			Path        string
			Value       string
			ModRevision int64 `mapstructure:"mod_revision"`
		}
	}

	var resp getResponse
	if err := mapstructure.Decode(result[0], &resp); err != nil {
		return nil, fmt.Errorf("failed to map response from tarantool: %q", result[0])
	}

	switch {
	case len(resp.Data) == 0:
		return nil, fmt.Errorf("a configuration data not found in tarantool for key %q", key)
	case len(resp.Data) > 1:
		// It should not happen, but we need to be sure to avoid a null pointer
		// dereference.
		return nil, fmt.Errorf("too many responses (%v) from tarantool for key %q", resp, key)
	}

	return []byte(resp.Data[0].Value), nil
}
