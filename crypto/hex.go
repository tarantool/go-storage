package crypto

import (
	"crypto/rsa"
	"encoding/hex"
	"fmt"
)

// hexSignerVerifier decorates a SignerVerifier so that Sign emits a hex-encoded
// signature and Verify accepts a hex-encoded one. Name is passed through
// unchanged, so the on-disk key layout is preserved; only the stored signature
// payload becomes hex.
type hexSignerVerifier struct {
	inner SignerVerifier
}

// hexVerifier decorates a Verifier so that Verify accepts a hex-encoded signature.
type hexVerifier struct {
	inner Verifier
}

// NewHexSignerVerifier wraps sv so that Sign emits a hex-encoded signature and
// Verify accepts a hex-encoded one.
func NewHexSignerVerifier(sv SignerVerifier) SignerVerifier {
	return hexSignerVerifier{inner: sv}
}

// NewHexVerifier wraps v so that Verify accepts a hex-encoded signature.
func NewHexVerifier(v Verifier) Verifier {
	return hexVerifier{inner: v}
}

// NewHexRSAPSSSignerVerifier creates an RSA-PSS signer/verifier whose
// signatures are hex-encoded.
func NewHexRSAPSSSignerVerifier(privKey rsa.PrivateKey) SignerVerifier {
	return NewHexSignerVerifier(NewRSAPSSSignerVerifier(privKey))
}

// NewHexRSAPSSVerifier creates an RSA-PSS verifier that accepts hex-encoded
// signatures.
func NewHexRSAPSSVerifier(pubKey rsa.PublicKey) Verifier {
	return NewHexVerifier(NewRSAPSSVerifier(pubKey))
}

// Name implements Signer/Verifier interface.
func (h hexSignerVerifier) Name() string {
	return h.inner.Name()
}

// Sign implements Signer interface, returning the inner signature hex-encoded.
func (h hexSignerVerifier) Sign(data []byte) ([]byte, error) {
	sig, err := h.inner.Sign(data)
	if err != nil {
		return nil, fmt.Errorf("hex signer: %w", err)
	}

	dst := make([]byte, hex.EncodedLen(len(sig)))
	hex.Encode(dst, sig)

	return dst, nil
}

// Verify implements Verifier interface, decoding a hex signature before verifying.
func (h hexSignerVerifier) Verify(data []byte, signature []byte) error {
	return verifyHex(h.inner, data, signature)
}

// Name implements Verifier interface.
func (h hexVerifier) Name() string {
	return h.inner.Name()
}

// Verify implements Verifier interface, decoding a hex signature before verifying.
func (h hexVerifier) Verify(data []byte, signature []byte) error {
	return verifyHex(h.inner, data, signature)
}

func verifyHex(verifier Verifier, data []byte, signature []byte) error {
	raw := make([]byte, hex.DecodedLen(len(signature)))

	n, err := hex.Decode(raw, signature)
	if err != nil {
		return fmt.Errorf("failed to decode hex signature: %w", err)
	}

	err = verifier.Verify(data, raw[:n])
	if err != nil {
		return fmt.Errorf("hex verifier: %w", err)
	}

	return nil
}
