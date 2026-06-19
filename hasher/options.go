package hasher

// Mode selects how a Hasher encodes the digest it produces and which encodings
// it accepts when verifying a stored digest.
type Mode int

const (
	// ModeAuto produces the raw digest bytes and, on verification, accepts a
	// stored digest in either raw or lower-case hex form. It is the default.
	ModeAuto Mode = iota
	// ModeHex produces a lower-case hex digest and accepts only hex on
	// verification.
	ModeHex
	// ModeBin produces the raw digest bytes and accepts only raw bytes on
	// verification.
	ModeBin
)

// Option configures a Hasher constructor.
type Option func(*config)

type config struct {
	mode Mode
}

func newConfig(opts ...Option) config {
	var cfg config // mode defaults to ModeAuto (the zero value).

	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

// WithMode sets the encoding mode of the hasher. See ModeAuto, ModeHex and
// ModeBin. When omitted, ModeAuto is used.
func WithMode(m Mode) Option {
	return func(c *config) { c.mode = m }
}
