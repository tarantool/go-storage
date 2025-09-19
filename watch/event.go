// Package watch provides change notification functionality for storage operations.
// It enables real-time monitoring of key changes through event streams.
package watch

import (
	"github.com/tarantool/go-storage/kv"
)

// EventType represents the type of watch event.
type EventType int

const (
	// EventPut indicates a key was created or updated.
	EventPut EventType = iota
	// EventDelete indicates a key was deleted.
	EventDelete
)

func (t EventType) String() string {
	switch t {
	case EventPut:
		return "Put"
	case EventDelete:
		return "Delete"
	default:
		return "Unknown"
	}
}

// Event represents a change notification from the watch stream.
type Event struct {
	// Type indicates whether this is a put or delete event.
	Type EventType
	// Key is the key that was changed.
	Key []byte
	// Value contains the new value for put events, nil for delete events.
	Value []byte
	// Rev is the revision number of the event.
	Rev int64
}

// AsKeyValue converts the Event to a KeyValue structure.
// For delete events, the Value field will be nil.
func (e *Event) AsKeyValue() kv.KeyValue {
	return kv.KeyValue{
		Key:            e.Key,
		Value:          e.Value,
		CreateRevision: 0,
		ModRevision:    0,
		Version:        0,
	}
}
