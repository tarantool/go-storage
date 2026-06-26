package crypto

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/tarantool/go-storage/v2/hasher"
)

var (
	ErrEmptyPrivateKey = errors.New("crypto: cannot sign without a private key")
)

// AlgoRSAPSS is the algorithm name returned by the RSA-PSS signer/verifier's
// Name method. Use it with integrity codec location options (e.g.
// WithSignatureLocation) instead of retyping the bare string.
const AlgoRSAPSS = "rsapss"

func zero[T any]() (out T) { return } //nolint:nonamedreturns

// RSAPSS represents RSA PSS algo for signing/verification
// (with SHA256 as digest calculation function).
//
// The encoding of produced signatures and the encodings accepted on
// verification are controlled by the Mode (see ModeAuto, ModeHex, ModeBin). By
// default (ModeAuto) signatures are emitted as raw bytes and Verify accepts a
// signature in either raw or hex form.
type RSAPSS struct {
	publicKey  rsa.PublicKey
	privateKey rsa.PrivateKey
	hash       crypto.Hash
	hasher     hasher.Hasher
	mode       Mode
}

// NewRSAPSS creates new RSAPSS object that can both sign and verify.
// The public key is derived from the private key.
func NewRSAPSS(privKey rsa.PrivateKey, opts ...Option) SignerVerifier {
	return RSAPSS{
		publicKey:  privKey.PublicKey,
		privateKey: privKey,
		hash:       crypto.SHA256,
		hasher:     hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeBin)),
		mode:       newConfig(opts...).mode,
	}
}

// NewRSAPSSVerifier creates new RSAPSS object that can only verify signatures.
func NewRSAPSSVerifier(pubKey rsa.PublicKey, opts ...Option) Verifier {
	return RSAPSS{
		publicKey:  pubKey,
		privateKey: zero[rsa.PrivateKey](),
		hash:       crypto.SHA256,
		hasher:     hasher.NewSHA256Hasher(hasher.WithMode(hasher.ModeBin)),
		mode:       newConfig(opts...).mode,
	}
}

// Name implements SignerVerifier interface.
func (r RSAPSS) Name() string {
	return AlgoRSAPSS
}

// Sign generates SHA-256 digest and signs it using RSASSA-PSS. The returned
// signature is encoded according to the signer's Mode (raw bytes for ModeAuto
// and ModeBin, hex for ModeHex).
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

	return r.encode(signature), nil
}

// Verify compares data with signature. The accepted signature encoding depends
// on the verifier's Mode: ModeHex expects hex, ModeBin expects raw bytes and
// ModeAuto accepts either.
func (r RSAPSS) Verify(data []byte, signature []byte) error {
	sig, err := r.decode(signature)
	if err != nil {
		return err
	}

	digest, err := r.hasher.Hash(data)
	if err != nil {
		return fmt.Errorf("failed to get hash: %w", err)
	}

	err = rsa.VerifyPSS(&r.publicKey, r.hash, digest, sig, &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       r.hash,
	})
	if err != nil {
		return fmt.Errorf("failed to verify: %w", err)
	}

	return nil
}

// sigBytes is the raw RSA-PSS signature length in bytes for the configured key,
// or 0 when no public modulus is set (verification will fail downstream).
func (r RSAPSS) sigBytes() int {
	if r.publicKey.N == nil {
		return 0
	}

	return r.publicKey.Size()
}

// encode encodes the signature according to the receiver's Mode: hex for
// ModeHex, raw bytes for ModeAuto and ModeBin.
func (r RSAPSS) encode(sig []byte) []byte {
	if r.mode != ModeHex {
		return sig
	}

	dst := make([]byte, hex.EncodedLen(len(sig)))
	hex.Encode(dst, sig)

	return dst
}

// decode decodes the signature according to the receiver's Mode. ModeBin
// returns the bytes unchanged, ModeHex hex-decodes them, and ModeAuto detects
// the encoding from the length: a raw signature is exactly sigBytes long, its
// hex form exactly twice that.
func (r RSAPSS) decode(sig []byte) ([]byte, error) {
	if r.mode == ModeBin {
		return sig, nil
	}

	if r.mode == ModeHex {
		return hexDecode(sig)
	}

	// ModeAuto: a raw signature is exactly sigBytes long.
	if len(sig) == r.sigBytes() {
		return sig, nil
	}

	// Otherwise try hex; accept it only if it decodes to the raw length.
	raw, err := hexDecode(sig)
	if err == nil && len(raw) == r.sigBytes() {
		return raw, nil
	}

	// Fall back to treating the input as raw bytes; VerifyPSS will reject it if
	// the length is wrong.
	return sig, nil
}

func hexDecode(sig []byte) ([]byte, error) {
	raw := make([]byte, hex.DecodedLen(len(sig)))

	n, err := hex.Decode(raw, sig)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex signature: %w", err)
	}

	return raw[:n], nil
}
