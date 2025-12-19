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

	rsapss := crypto.NewRSAPSS(rsa.PrivateKey{}, rsa.PublicKey{}) //nolint:exhaustruct
	require.NotNil(t, rsapss, "rsapss must be returned")

	data := []byte("abc")

	sig, err := rsapss.Sign(data)
	require.ErrorContains(t, err, "failed to sign: crypto/rsa: missing public modulus")
	require.Nil(t, sig, "signature must be nil")

	err = rsapss.Verify(data, sig)
	require.ErrorContains(t, err, "failed to verify: crypto/rsa: missing public modulus")
}

func TestRsaOnlyPrivateKey(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	rsapss := crypto.NewRSAPSS(*privateKey, rsa.PublicKey{}) //nolint:exhaustruct
	require.NotNil(t, rsapss, "rsapss must be returned")

	data := []byte("abc")

	sig, err := rsapss.Sign(data)
	require.NoError(t, err, "Sign must be successful")
	require.NotNil(t, sig, "signature must be returned")

	err = rsapss.Verify(data, sig)
	require.ErrorContains(t, err, "failed to verify: crypto/rsa: missing public modulus")
}

func TestRsaOnlyPublicKey(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	rsapss := crypto.NewRSAPSS(rsa.PrivateKey{}, privateKey.PublicKey) //nolint:exhaustruct
	require.NotNil(t, rsapss, "rsapss must be returned")

	data := []byte("abc")

	sig, err := rsapss.Sign(data)
	require.ErrorContains(t, err, "failed to sign: crypto/rsa: missing public modulus")
	require.Nil(t, sig, "signature must be nil")

	// Re-create to have a valid sign.
	rsapss = crypto.NewRSAPSS(*privateKey, privateKey.PublicKey)
	require.NotNil(t, rsapss, "rsapss must be returned")

	sign, err := rsapss.Sign(data)
	require.NoError(t, err)
	require.NotNil(t, sign)

	err = rsapss.Verify(data, sign)
	require.NoError(t, err, "Verify must be successful")
}

func TestRsaSignVerify(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	rsapss := crypto.NewRSAPSS(*privateKey, privateKey.PublicKey)
	require.NotNil(t, rsapss, "rsapss must be returned")

	data := []byte("abc")

	sig, err := rsapss.Sign(data)
	require.NoError(t, err, "Sign must be successful")
	require.NotNil(t, sig, "signature must be returned")

	err = rsapss.Verify(data, sig)
	require.NoError(t, err, "Verify must be successful")
}

func TestRSAPSS_Name(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	rsapss := crypto.NewRSAPSS(*privateKey, privateKey.PublicKey)
	require.Equal(t, "RSASSA-PSS", rsapss.Name())
}
