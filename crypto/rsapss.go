package crypto

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"

	"github.com/tarantool/go-storage/hasher"
)

var (
	ErrEmptyPrivateKey = errors.New("trying to sign without private key")
)

func zero[T any]() (out T) { return } //nolint:nonamedreturns

// RSAPSS represents RSA PSS algo for signing/verification
// (with SHA256 as digest calculation function).
type RSAPSS struct {
	publicKey  rsa.PublicKey
	privateKey rsa.PrivateKey
	hash       crypto.Hash
	hasher     hasher.Hasher
}

// NewRSAPSSSignerVerifier creates new RSAPSS object that can both sign and verify.
// The public key is derived from the private key.
func NewRSAPSSSignerVerifier(privKey rsa.PrivateKey) SignerVerifier {
	return RSAPSS{
		publicKey:  privKey.PublicKey,
		privateKey: privKey,
		hash:       crypto.SHA256,
		hasher:     hasher.NewSHA256Hasher(),
	}
}

// NewRSAPSSVerifier creates new RSAPSS object that can only verify signatures.
func NewRSAPSSVerifier(pubKey rsa.PublicKey) Verifier {
	return RSAPSS{
		publicKey:  pubKey,
		privateKey: zero[rsa.PrivateKey](),
		hash:       crypto.SHA256,
		hasher:     hasher.NewSHA256Hasher(),
	}
}

// Name implements SignerVerifier interface.
func (r RSAPSS) Name() string {
	return "rsapss"
}

// Sign generates SHA-256 digest and signs it using RSASSA-PSS.
func (r RSAPSS) Sign(data []byte) ([]byte, error) {
	if r.privateKey.N == nil {
		return nil, ErrEmptyPrivateKey
	}

	digest, err := r.hasher.Hash(data)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to get hash: %w", err)
	}

	signature, err := rsa.SignPSS(rand.Reader, &r.privateKey, r.hash, digest, &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       r.hash,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	return signature, nil
}

// Verify compares data with signature.
func (r RSAPSS) Verify(data []byte, signature []byte) error {
	digest, err := r.hasher.Hash(data)
	if err != nil {
		return fmt.Errorf("failed to get hash: %w", err)
	}

	err = rsa.VerifyPSS(&r.publicKey, r.hash, digest, signature, &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       r.hash,
	})
	if err != nil {
		return fmt.Errorf("failed to verify: %w", err)
	}

	return nil
}
