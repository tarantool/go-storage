// Package crypto implements crypto interfaces.
package crypto

// Signer implements high-level API for package signing.
type Signer interface {
	// Name returns name of the crypto algorithm, used by signer.
	Name() string
	// Sign returns signature for passed data.
	Sign(data []byte) ([]byte, error)
}

// Verifier is an interface implementing a generic signature
// verification algorithm.
type Verifier interface {
	// Name returns name of the crypto algorithm, used by verifier.
	Name() string
	// Verify checks data and signature mapping.
	Verify(data []byte, signature []byte) error
}

// SignerVerifier common interface.
type SignerVerifier interface {
	Signer
	Verifier
}
