package crypto_test

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/crypto"
)

func TestRsaWithoutKeys(t *testing.T) {
	t.Parallel()

	rsapss := crypto.NewRSAPSS(rsa.PrivateKey{}) //nolint:exhaustruct
	require.NotNil(t, rsapss, "rsapss must be returned")

	data := []byte("abc")

	sig, err := rsapss.Sign(data)
	require.ErrorIs(t, err, crypto.ErrEmptyPrivateKey)
	require.Nil(t, sig, "signature must be nil")

	err = rsapss.Verify(data, sig)
	require.ErrorContains(t, err, "failed to verify: crypto/rsa: missing public modulus")
}

func TestRsaOnlyPrivateKey(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signer := crypto.NewRSAPSS(*privateKey)
	require.NotNil(t, signer, "signer must be returned")

	verifier := crypto.NewRSAPSSVerifier(rsa.PublicKey{}) //nolint:exhaustruct
	require.NotNil(t, verifier, "verifier must be returned")

	data := []byte("abc")

	sig, err := signer.Sign(data)
	require.NoError(t, err, "Sign must be successful")
	require.NotNil(t, sig, "signature must be returned")

	err = verifier.Verify(data, sig)
	require.ErrorContains(t, err, "failed to verify: crypto/rsa: missing public modulus")
}

func TestRsaOnlyPublicKey(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	verifier := crypto.NewRSAPSSVerifier(privateKey.PublicKey)
	require.NotNil(t, verifier, "verifier must be returned")

	data := []byte("abc")

	// Sign should fail because private key is missing.
	sv, ok := verifier.(crypto.SignerVerifier)
	require.True(t, ok, "verifier must be a SignerVerifier")

	sig, err := sv.Sign(data)
	require.ErrorIs(t, err, crypto.ErrEmptyPrivateKey)
	require.Nil(t, sig, "signature must be nil")

	// Create signer+verifier with private key.
	signerVerifier := crypto.NewRSAPSS(*privateKey)
	require.NotNil(t, signerVerifier, "signerVerifier must be returned")

	sign, err := signerVerifier.Sign(data)
	require.NoError(t, err)
	require.NotNil(t, sign)

	err = signerVerifier.Verify(data, sign)
	require.NoError(t, err, "Verify must be successful")
}

func TestRsaSignVerify(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSS(*privateKey)
	require.NotNil(t, signerVerifier, "signerVerifier must be returned")

	data := []byte("abc")

	sig, err := signerVerifier.Sign(data)
	require.NoError(t, err, "Sign must be successful")
	require.NotNil(t, sig, "signature must be returned")

	err = signerVerifier.Verify(data, sig)
	require.NoError(t, err, "Verify must be successful")
}

// TestRsaSign_EmptyData_NoExplosion pins that Sign tolerates empty/nil input —
// before the hasher fix it surfaced "failed to get hash: data is nil" because
// Sign calls Hash internally over the payload bytes.
func TestRsaSign_EmptyData_NoExplosion(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signer := crypto.NewRSAPSS(*privateKey)

	for _, data := range [][]byte{nil, {}} {
		sig, err := signer.Sign(data)
		require.NoError(t, err)
		require.NotEmpty(t, sig)
	}
}

// TestRsaVerify_EmptyData_NoExplosion mirrors the Sign-side check: Verify must
// be able to recompute the digest over an empty/nil payload without erroring
// out at the hash step.
func TestRsaVerify_EmptyData_NoExplosion(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSS(*privateKey)

	sig, err := signerVerifier.Sign(nil)
	require.NoError(t, err)

	for _, data := range [][]byte{nil, {}} {
		require.NoError(t, signerVerifier.Verify(data, sig))
	}
}

// TestRsaVerify_EmptySignature pins that Verify rejects an empty or nil
// signature with a clean error rather than panicking. RSA-PSS signatures over
// 2048-bit keys are 256 bytes wide; an empty/nil blob is structurally invalid
// and must surface as a verification failure, not a crash.
func TestRsaVerify_EmptySignature(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSS(*privateKey)

	for _, sig := range [][]byte{nil, {}} {
		err := signerVerifier.Verify([]byte("payload"), sig)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to verify")
	}
}

// TestRsaSignVerify_EmptyData pins that Sign/Verify accept empty/nil payloads.
// Storage backends commonly round-trip empty values as nil, so a verifier that
// rejected nil input would fail to validate a legitimately-empty stored value.
func TestRsaSignVerify_EmptyData(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSS(*privateKey)

	for _, data := range [][]byte{nil, {}} {
		sig, err := signerVerifier.Sign(data)
		require.NoError(t, err, "Sign must accept empty/nil input")
		require.NotNil(t, sig, "signature must be returned")

		// Verifying against the matching empty/nil input must succeed.
		require.NoError(t, signerVerifier.Verify(data, sig))

		// Verifying against the other empty representation must also succeed:
		// nil and []byte{} hash to the same digest, so signatures are
		// interchangeable across the two.
		other := []byte{}
		if data != nil {
			other = nil
		}

		require.NoError(t, signerVerifier.Verify(other, sig),
			"nil and []byte{} must produce interchangeable signatures")
	}
}

func TestRSAPSS_Name(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSS(*privateKey)
	require.Equal(t, "rsapss", signerVerifier.Name())
}
