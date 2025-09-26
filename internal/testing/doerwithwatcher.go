// Package testing provides a mock implementation of the tarantool.Doer and other interfaces.
// It is used for testing purposes.
package testing

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v2"
)

// MockDoerWithWatcher is a mock implementation of the tarantool.DoerWithWatcher interface.
type MockDoerWithWatcher struct {
	mockDoer *MockDoer

	events map[string][]tarantool.WatchEvent
}

// NewMockDoerWithWatcher returns a new mock doer with watcher.
func NewMockDoerWithWatcher(doer *MockDoer, events map[string][]tarantool.WatchEvent) *MockDoerWithWatcher {
	return &MockDoerWithWatcher{
		mockDoer: doer,
		events:   events,
	}
}

const (
	delayBeforeFirstEvent = 100 * time.Millisecond
)

// Do returns a new future.
func (m *MockDoerWithWatcher) Do(req tarantool.Request) *tarantool.Future {
	return m.mockDoer.Do(req)
}

// NewWatcher returns a new watcher.
func (m *MockDoerWithWatcher) NewWatcher(path string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
	eventList, ok := m.events[path]
	if !ok {
		panic(fmt.Sprintf("event list %s not found", path))
	}

	dummy := &dummyWatcher{
		cb:        callback,
		isStopped: make(chan struct{}),

		eventListPos: 0,
		eventList:    eventList,
	}

	go func() {
		time.Sleep(delayBeforeFirstEvent)
		dummy.Start()
	}()

	return dummy, nil
}

type dummyWatcher struct {
	cb        tarantool.WatchCallback
	isStopped chan struct{}

	eventListPos int
	eventList    []tarantool.WatchEvent
}

const (
	delayBetweenEvents = 10 * time.Millisecond
)

func (w *dummyWatcher) Start() {
	go func() {
		for {
			select {
			case <-w.isStopped:
				return
			default:
			}

			if w.eventListPos >= len(w.eventList) {
				return
			}

			w.cb(w.eventList[w.eventListPos])

			w.eventListPos++

			time.Sleep(delayBetweenEvents)
		}
	}()
}

// Unregister stops the watcher.
func (w *dummyWatcher) Unregister() {
	close(w.isStopped)
}
