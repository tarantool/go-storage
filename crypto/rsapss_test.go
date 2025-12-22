package crypto_test

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/crypto"
)

func TestRsaWithoutKeys(t *testing.T) {
	t.Parallel()

	rsapss := crypto.NewRSAPSSSignerVerifier(rsa.PrivateKey{}) //nolint:exhaustruct
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

	signer := crypto.NewRSAPSSSignerVerifier(*privateKey)
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
	signerVerifier := crypto.NewRSAPSSSignerVerifier(*privateKey)
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

	signerVerifier := crypto.NewRSAPSSSignerVerifier(*privateKey)
	require.NotNil(t, signerVerifier, "signerVerifier must be returned")

	data := []byte("abc")

	sig, err := signerVerifier.Sign(data)
	require.NoError(t, err, "Sign must be successful")
	require.NotNil(t, sig, "signature must be returned")

	err = signerVerifier.Verify(data, sig)
	require.NoError(t, err, "Verify must be successful")
}

func TestRSAPSS_Name(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewRSAPSSSignerVerifier(*privateKey)
	require.Equal(t, "rsapss", signerVerifier.Name())
}
