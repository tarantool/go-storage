// Package watch provides change notification functionality for storage operations.
// It enables real-time monitoring of key changes through event streams.
package watch

// Event represents a change notification from the watch stream.
type Event struct {
	// Prefix indicates key/prefix of what was changed.
	Prefix []byte
}
