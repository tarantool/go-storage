package crypto_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/crypto"
)

// TestRsaSign_DefaultsToRaw pins that, by default (ModeAuto), Sign emits raw
// signature bytes and Verify accepts them.
func TestRsaSign_DefaultsToRaw(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSS(*privateKey)

	data := []byte("payload")

	sig, err := signerVerifier.Sign(data)
	require.NoError(t, err)
	require.Len(t, sig, 256, "default (auto) RSA-PSS signature over a 2048-bit key is 256 raw bytes")

	require.NoError(t, signerVerifier.Verify(data, sig))
}

// TestRsaSign_Hex pins that ModeHex emits a hex-encoded signature and that a
// hex signer/verifier round-trips.
func TestRsaSign_Hex(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSS(*privateKey, crypto.WithMode(crypto.ModeHex))

	data := []byte("payload")

	sig, err := signerVerifier.Sign(data)
	require.NoError(t, err)

	// Signature is a valid hex string of the raw 256-byte RSA-PSS signature.
	raw, err := hex.DecodeString(string(sig))
	require.NoError(t, err, "ModeHex signature must be hex-encoded")
	require.Len(t, raw, 256)

	require.NoError(t, signerVerifier.Verify(data, sig))
}

// TestRsaSign_Bin pins that ModeBin emits raw signature bytes and round-trips.
func TestRsaSign_Bin(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signer := crypto.NewRSAPSS(*privateKey, crypto.WithMode(crypto.ModeBin))
	verifier := crypto.NewRSAPSSVerifier(privateKey.PublicKey, crypto.WithMode(crypto.ModeBin))

	data := []byte("payload")

	sig, err := signer.Sign(data)
	require.NoError(t, err)
	require.Len(t, sig, 256, "raw RSA-PSS signature over a 2048-bit key is 256 bytes")

	require.NoError(t, verifier.Verify(data, sig))
}

// TestRsaVerify_AutoAcceptsBoth pins that a ModeAuto verifier accepts a
// signature in either encoding — raw bytes or hex — regardless of how it was
// produced. This is the migration-friendly read path.
func TestRsaVerify_AutoAcceptsBoth(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	data := []byte("payload")

	hexSig, err := crypto.NewRSAPSS(*privateKey, crypto.WithMode(crypto.ModeHex)).Sign(data)
	require.NoError(t, err)

	binSig, err := crypto.NewRSAPSS(*privateKey, crypto.WithMode(crypto.ModeBin)).Sign(data)
	require.NoError(t, err)

	autoVerifier := crypto.NewRSAPSSVerifier(privateKey.PublicKey) // ModeAuto.

	require.NoError(t, autoVerifier.Verify(data, hexSig), "auto verifier must accept hex")
	require.NoError(t, autoVerifier.Verify(data, binSig), "auto verifier must accept raw")
}

// TestRsaVerify_HexRejectsRaw pins that a ModeHex verifier rejects a raw
// signature instead of silently accepting it.
func TestRsaVerify_HexRejectsRaw(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	data := []byte("payload")

	binSig, err := crypto.NewRSAPSS(*privateKey, crypto.WithMode(crypto.ModeBin)).Sign(data)
	require.NoError(t, err)

	hexVerifier := crypto.NewRSAPSSVerifier(privateKey.PublicKey, crypto.WithMode(crypto.ModeHex))

	require.Error(t, hexVerifier.Verify(data, binSig))
}

// TestRsaVerify_BinRejectsHex pins that a ModeBin verifier rejects a hex
// signature — the raw verifier treats the hex bytes as the signature itself.
func TestRsaVerify_BinRejectsHex(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	data := []byte("payload")

	hexSig, err := crypto.NewRSAPSS(*privateKey, crypto.WithMode(crypto.ModeHex)).Sign(data)
	require.NoError(t, err)

	binVerifier := crypto.NewRSAPSSVerifier(privateKey.PublicKey, crypto.WithMode(crypto.ModeBin))

	require.Error(t, binVerifier.Verify(data, hexSig))
}

// TestRsaVerify_HexRejectsNonHex pins that a ModeHex verifier rejects a non-hex
// signature with a clear error instead of treating it as raw.
func TestRsaVerify_HexRejectsNonHex(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	verifier := crypto.NewRSAPSSVerifier(privateKey.PublicKey, crypto.WithMode(crypto.ModeHex))

	err = verifier.Verify([]byte("payload"), []byte("zz-not-hex"))
	require.ErrorContains(t, err, "failed to decode hex signature")
}
