package crypto

// Mode selects how a signer encodes the signature it produces and which
// encodings a verifier accepts.
type Mode int

const (
	// ModeAuto emits raw signature bytes and, on verification, accepts a
	// signature in either raw or lower-case hex form. It is the default.
	ModeAuto Mode = iota
	// ModeHex emits a lower-case hex signature and accepts only hex on
	// verification.
	ModeHex
	// ModeBin emits raw signature bytes and accepts only raw bytes on
	// verification.
	ModeBin
)

// Option configures an RSA-PSS signer/verifier constructor.
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

// WithMode sets the encoding mode of the signer/verifier. See ModeAuto,
// ModeHex and ModeBin. When omitted, ModeAuto is used.
func WithMode(m Mode) Option {
	return func(c *config) { c.mode = m }
}
