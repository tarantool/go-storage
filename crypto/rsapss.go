package crypto

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"fmt"

	"github.com/tarantool/go-storage/hasher"
)

// RSAPSS represents RSA PSS algo for signing/verification
// (with SHA256 as digest calculation function).
type RSAPSS struct {
	publicKey  rsa.PublicKey
	privateKey rsa.PrivateKey
	hash       crypto.Hash
	hasher     hasher.Hasher
}

// NewRSAPSS creates new RSAPSS object.
func NewRSAPSS(privKey rsa.PrivateKey, pubKey rsa.PublicKey) RSAPSS {
	return RSAPSS{
		publicKey:  pubKey,
		privateKey: privKey,
		hash:       crypto.SHA256,
		hasher:     hasher.NewSHA256Hasher(),
	}
}

// Name implements SignerVerifier interface.
func (r RSAPSS) Name() string {
	return "RSASSA-PSS"
}

// Sign generates SHA-256 digest and signs it using RSASSA-PSS.
func (r RSAPSS) Sign(data []byte) ([]byte, error) {
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
