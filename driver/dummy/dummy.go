// Package dummy provides a base in-memory implementation
// of the storage driver interface for demonstration and tests.
package dummy

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
)

type watcherPrefix struct {
	id    uint64
	chans map[uint64]chan watch.Event
}

const (
	eventChannelSize = 100
)

// dummyStorage is a thread-safe structure that holds the
// key-value storage and watch channels.
type dummyStorage struct {
	storage          map[string]kv.KeyValue
	watchChanStorage map[string]watcherPrefix
	modRevision      int64
	mu               sync.RWMutex
}

type Driver struct {
	data dummyStorage
}

func New() *Driver {
	return &Driver{
		data: dummyStorage{
			storage:          make(map[string]kv.KeyValue),
			watchChanStorage: make(map[string]watcherPrefix),
			modRevision:      1,
			mu:               sync.RWMutex{},
		},
	}
}

func (d *Driver) Execute(
	ctx context.Context,
	predicates []predicate.Predicate,
	thenOps []operation.Operation,
	elseOps []operation.Operation,
) (tx.Response, error) {
	// We use a mutex to ensure that the execution of
	// operations is atomic and thread-safe.
	d.data.mu.Lock()
	defer d.data.mu.Unlock()

	ops := elseOps

	success := d.checkPredicates(predicates)
	if success {
		ops = thenOps
	}

	opsResults := d.executeOps(ops)

	return tx.Response{
		Succeeded: success,
		Results:   opsResults,
	}, nil
}

func (d *Driver) Watch(ctx context.Context, key []byte, _ ...watch.Option) (<-chan watch.Event, func(), error) {
	ch, cancel := d.addWatcher(ctx, string(key))
	return ch, cancel, nil
}

func (d *Driver) get(key string) (kv.KeyValue, bool) {
	val, ok := d.data.storage[key]

	return val, ok
}

func (d *Driver) put(key string, value []byte) {
	d.data.storage[key] = kv.KeyValue{
		Key:         []byte(key),
		Value:       value,
		ModRevision: d.data.modRevision,
	}

	d.notifyWatchers(key)
}

func (d *Driver) delete(key string) (kv.KeyValue, bool) {
	prevKv, ok := d.data.storage[key]
	delete(d.data.storage, key)

	if ok {
		d.notifyWatchers(key)
	}

	return prevKv, ok
}

func isPrefix(str string) bool {
	return str == "" || str[len(str)-1] == '/'
}

// checkPredicates checks if the given predicates are satisfied by
// the current state of the storage.
func (d *Driver) checkPredicates(predicates []predicate.Predicate) bool {
	for _, pred := range predicates {
		val, exists := d.data.storage[string(pred.Key())]

		switch pred.Target() {
		case predicate.TargetVersion:
			version, ok := pred.Value().(int64)
			if !ok {
				return false
			}

			switch pred.Operation() {
			case predicate.OpEqual:
				if !exists || val.ModRevision != version {
					return false
				}
			case predicate.OpNotEqual:
				if exists && val.ModRevision == version {
					return false
				}
			case predicate.OpGreater:
				if !exists || val.ModRevision <= version {
					return false
				}
			case predicate.OpLess:
				if !exists || val.ModRevision >= version {
					return false
				}
			default:
				return false
			}
		case predicate.TargetValue:
			var value []byte

			switch v := pred.Value().(type) {
			case []byte:
				value = v
			case string:
				value = []byte(v)
			default:
				return false
			}

			switch pred.Operation() { //nolint:exhaustive
			case predicate.OpEqual:
				if !exists || !bytes.Equal(val.Value, value) {
					return false
				}
			case predicate.OpNotEqual:
				if exists && bytes.Equal(val.Value, value) {
					return false
				}
			default:
				return false
			}
		default:
			return false
		}
	}

	return true
}

func (d *Driver) getAllByPrefix(prefix string) []kv.KeyValue {
	var prefixValues []kv.KeyValue

	for k, v := range d.data.storage {
		if strings.HasPrefix(k, prefix) {
			prefixValues = append(prefixValues, v)
		}
	}

	sort.Slice(prefixValues, func(i, j int) bool {
		return bytes.Compare(prefixValues[i].Key, prefixValues[j].Key) < 0
	})

	return prefixValues
}

func (d *Driver) executeOps(ops []operation.Operation) []tx.RequestResponse {
	result := make([]tx.RequestResponse, 0, len(ops))
	mutable := false

	for _, eop := range ops {
		switch eop.Type() {
		case operation.TypePut:
			d.put(string(eop.Key()), eop.Value())

			mutable = true

			result = append(result, tx.RequestResponse{
				Values: nil,
			})
		case operation.TypeDelete:
			var values []kv.KeyValue

			if isPrefix(string(eop.Key())) {
				prefixValues := d.getAllByPrefix(string(eop.Key()))
				for _, pv := range prefixValues {
					d.delete(string(pv.Key))
				}

				values = prefixValues
				if len(prefixValues) > 0 {
					mutable = true
				}
			} else {
				val, ok := d.delete(string(eop.Key()))
				if ok {
					values = []kv.KeyValue{val}
					mutable = true
				}
			}

			result = append(result, tx.RequestResponse{
				Values: values,
			})
		case operation.TypeGet:
			var values []kv.KeyValue
			if isPrefix(string(eop.Key())) {
				values = d.getAllByPrefix(string(eop.Key()))
			} else if val, ok := d.get(string(eop.Key())); ok {
				values = []kv.KeyValue{val}
			}

			result = append(result, tx.RequestResponse{
				Values: values,
			})
		}
	}

	if mutable {
		d.data.modRevision++
	}

	return result
}

func (d *Driver) addWatcher(ctx context.Context, key string) (chan watch.Event, func()) {
	d.data.mu.Lock()
	defer d.data.mu.Unlock()

	if _, exists := d.data.watchChanStorage[key]; !exists {
		d.data.watchChanStorage[key] = watcherPrefix{
			id:    0,
			chans: make(map[uint64]chan watch.Event),
		}
	}

	watcher := d.data.watchChanStorage[key]
	watcher.id++

	wid := watcher.id
	wch := make(chan watch.Event, eventChannelSize)

	watcher.chans[wid] = wch
	d.data.watchChanStorage[key] = watcher

	var (
		isStoppedOnce = sync.Once{}
		isStopped     = make(chan struct{})
	)

	go func() {
		defer func() {
			d.data.mu.Lock()
			defer d.data.mu.Unlock()

			delete(d.data.watchChanStorage[key].chans, wid)
			close(wch)
		}()

		select {
		case <-ctx.Done():
		case <-isStopped:
		}
	}()

	return wch, func() { isStoppedOnce.Do(func() { close(isStopped) }) }
}

// notifyWatchers sends a watch event to all watchers
// whose prefix matches the given key.
func (d *Driver) notifyWatchers(key string) {
	for prefix, watchers := range d.data.watchChanStorage {
		if strings.HasPrefix(key, prefix) && isPrefix(prefix) || key == prefix {
			for _, ch := range watchers.chans {
				select {
				case ch <- watch.Event{
					Prefix: []byte(prefix),
				}:
				default:
				}
			}
		}
	}
}
