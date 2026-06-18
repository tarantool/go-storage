// Package watch provides change notification functionality for storage operations.
// It enables real-time monitoring of key changes through event streams.
package watch

// Event represents a change notification from the watch stream.
type Event struct {
	// Key is the key (or key prefix) that changed.
	Key []byte
}
