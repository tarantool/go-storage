package crypto_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/crypto"
)

func TestHexSignerVerifierRoundTrip(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signerVerifier := crypto.NewHexRSAPSSSignerVerifier(*privateKey)
	require.NotNil(t, signerVerifier)

	data := []byte("abc")

	sig, err := signerVerifier.Sign(data)
	require.NoError(t, err)

	// Signature is a valid hex string.
	_, err = hex.DecodeString(string(sig))
	require.NoError(t, err, "signature must be hex-encoded")

	require.NoError(t, signerVerifier.Verify(data, sig))
}

func TestHexVerifierAcceptsHexSignature(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	signer := crypto.NewHexRSAPSSSignerVerifier(*privateKey)
	verifier := crypto.NewHexRSAPSSVerifier(privateKey.PublicKey)

	data := []byte("payload")

	sig, err := signer.Sign(data)
	require.NoError(t, err)

	require.NoError(t, verifier.Verify(data, sig))
}

func TestHexVerifierRejectsNonHex(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	verifier := crypto.NewHexRSAPSSVerifier(privateKey.PublicKey)

	err = verifier.Verify([]byte("payload"), []byte("zz-not-hex"))
	require.ErrorContains(t, err, "failed to decode hex signature")
}

func TestHexSignerVerifierName(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Name is passed through unchanged so the on-disk key layout is preserved.
	require.Equal(t, "rsapss", crypto.NewHexRSAPSSSignerVerifier(*privateKey).Name())
	require.Equal(t, "rsapss", crypto.NewHexRSAPSSVerifier(privateKey.PublicKey).Name())
}
