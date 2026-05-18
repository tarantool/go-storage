package tcs

import (
	"context"
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
)

type lockEntry struct {
	Path        string
	Value       string
	ModRevision int64
}

// putWithTTL writes path with the given TTL and returns the cluster revision
// assigned to the write — the same value reappears as mod_revision in
// config.storage.get results, providing the ordering for "smallest revision
// wins".
func putWithTTL(ctx context.Context, conn DoerWatcher, path, value string, ttlSec int) (int64, error) {
	req := tarantool.NewCallRequest("config.storage.put").
		Args([]any{path, value, map[string]any{"ttl": ttlSec}}).
		Context(ctx)

	// Response shape: [ { revision: N } ].
	var result []struct {
		Revision int64 `msgpack:"revision"`
	}

	err := conn.Do(req).GetTyped(&result)
	if err != nil {
		return 0, fmt.Errorf("tcs locker: putWithTTL %s: %w", path, err)
	}

	if len(result) != 1 {
		return 0, fmt.Errorf("tcs locker: putWithTTL %s: %w: expected 1 element, got %d",
			path, ErrUnexpectedResponse, len(result))
	}

	return result[0].Revision, nil
}

func keepalivePath(ctx context.Context, conn DoerWatcher, path string, ttlSec int) error {
	req := tarantool.NewCallRequest("config.storage.keepalive").
		Args([]any{path, ttlSec}).
		Context(ctx)

	var result []any

	err := conn.Do(req).GetTyped(&result)
	if err != nil {
		return fmt.Errorf("tcs locker: keepalive %s: %w", path, err)
	}

	return nil
}

// getPrefix returns entries under prefix. Response shape:
//
//	[ { data: [ {path, value, mod_revision}, ... ], revision: N } ]
func getPrefix(ctx context.Context, conn DoerWatcher, prefix string) ([]lockEntry, error) {
	req := tarantool.NewCallRequest("config.storage.get").
		Args([]any{prefix}).
		Context(ctx)

	var raw []struct {
		Data []struct {
			Path        string `msgpack:"path"`
			Value       string `msgpack:"value"`
			ModRevision int64  `msgpack:"mod_revision"`
		} `msgpack:"data"`
		Revision int64 `msgpack:"revision"`
	}

	err := conn.Do(req).GetTyped(&raw)
	if err != nil {
		return nil, fmt.Errorf("tcs locker: getPrefix %s: %w", prefix, err)
	}

	if len(raw) == 0 {
		return nil, nil
	}

	entries := make([]lockEntry, 0, len(raw[0].Data))
	for _, r := range raw[0].Data {
		entries = append(entries, lockEntry{
			Path:        r.Path,
			Value:       r.Value,
			ModRevision: r.ModRevision,
		})
	}

	return entries, nil
}

func deletePath(ctx context.Context, conn DoerWatcher, path string) error {
	req := tarantool.NewCallRequest("config.storage.delete").
		Args([]any{path}).
		Context(ctx)

	var result []any

	err := conn.Do(req).GetTyped(&result)
	if err != nil {
		return fmt.Errorf("tcs locker: delete %s: %w", path, err)
	}

	return nil
}

// infoFeatures reports whether the server exposes the ttl and keepalive
// features required by the locker.
func infoFeatures(ctx context.Context, conn DoerWatcher) (bool, bool, error) {
	req := tarantool.NewCallRequest("config.storage.info").
		Args([]any{}).
		Context(ctx)

	// Response shape: [ { features = { ttl, keepalive, ... } } ].
	var result []struct {
		Features map[string]any `msgpack:"features"`
	}

	err := conn.Do(req).GetTyped(&result)
	if err != nil {
		return false, false, fmt.Errorf("tcs locker: info: %w", err)
	}

	if len(result) == 0 {
		return false, false, fmt.Errorf("tcs locker: info: %w: empty response", ErrUnexpectedResponse)
	}

	features := result[0].Features

	var ttl, keepalive bool

	if v, ok := features["ttl"]; ok {
		if b, ok2 := v.(bool); ok2 {
			ttl = b
		}
	}

	if v, ok := features["keepalive"]; ok {
		if b, ok2 := v.(bool); ok2 {
			keepalive = b
		}
	}

	return ttl, keepalive, nil
}
