package watch

// watchOptions contains configuration options for watch operations.
type watchOptions struct {
	Prefix string // Prefix filter for watch operations.
}

// Option is a function that configures watch operation options.
type Option func(*watchOptions)
