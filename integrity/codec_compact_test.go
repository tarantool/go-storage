package integrity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storage "github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/driver/dummy"
	"github.com/tarantool/go-storage/v2/hasher"
	"github.com/tarantool/go-storage/v2/integrity"
	"github.com/tarantool/go-storage/v2/operation"
)

const valueKey = "/objects/foo"

// WithSingleSigCompact counts the union of signers and verifiers, so a codec
// that only signs or only verifies can use it: the side that has no signature
// neither emits nor parses signature keys.
func TestCodecBuilder_WithSingleSigCompact_OneSide(t *testing.T) {
	t.Parallel()

	rsaPSS := newTestRSAPSS(t)

	type builder = integrity.CodecBuilder[codecTestStruct]

	tests := []struct {
		name  string
		apply func(b builder) builder
	}{
		{"signer only", func(b builder) builder { return b.WithSigner(rsaPSS) }},
		{"verifier only", func(b builder) builder { return b.WithVerifier(rsaPSS) }},
		{"signer and verifier", func(b builder) builder { return b.WithSignerVerifier(rsaPSS) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := integrity.NewCodecBuilder[codecTestStruct]().
				WithObjectLocation("objects").
				WithHasher(hasher.NewSHA256Hasher()).
				WithSingleHashCompact().
				WithSingleSigCompact()

			codec, err := tt.apply(base).Build()
			require.NoError(t, err)

			keys, err := codec.FullKeys("foo")
			require.NoError(t, err)
			assert.Equal(t, []string{valueKey, "/hashes/objects/foo", "/sig/objects/foo"}, keys)
		})
	}
}

// A signing codec and a verifying codec, both with the compact sig layout,
// must agree on the keys: what the first one writes the second one reads.
func TestCodec_WithSingleSigCompact_SignerToVerifier(t *testing.T) {
	t.Parallel()

	rsaPSS := newTestRSAPSS(t)

	build := func(b integrity.CodecBuilder[codecTestStruct]) *integrity.Codec[codecTestStruct] {
		codec, err := b.WithObjectLocation("objects").
			WithHasher(hasher.NewSHA256Hasher()).
			WithSingleHashCompact().
			WithSingleSigCompact().
			Build()
		require.NoError(t, err)

		return codec
	}

	signing := build(integrity.NewCodecBuilder[codecTestStruct]().WithSigner(rsaPSS))
	verifying := build(integrity.NewCodecBuilder[codecTestStruct]().WithVerifier(rsaPSS))

	stor := storage.NewStorage(dummy.New())
	ctx := context.Background()
	value := codecTestStruct{Name: "foo", Value: 42}

	require.NoError(t, signing.Bind(stor).Put(ctx, "foo", value))

	// The signature is stored under the compact key.
	sigs, err := stor.Range(ctx, storage.WithPrefix("/sig/"))
	require.NoError(t, err)
	require.Len(t, sigs, 1)
	assert.Equal(t, "/sig/objects/foo", string(sigs[0].Key))

	got, err := verifying.Bind(stor).Get(ctx, "foo")
	require.NoError(t, err)

	val, ok := got.Value.Get()
	require.True(t, ok)
	assert.Equal(t, value, val)

	// A tampered value is refused by the verifying codec.
	_, err = stor.Tx(ctx).Then(operation.Put([]byte(valueKey), []byte("name: foo\nvalue: 43\n"))).Commit()
	require.NoError(t, err)

	_, err = verifying.Bind(stor).Get(ctx, "foo")
	require.Error(t, err)
}

func TestCodecBuilder_WithSingleSigCompact_Cardinality(t *testing.T) {
	t.Parallel()

	t.Run("no signer and no verifier", func(t *testing.T) {
		t.Parallel()

		_, err := integrity.NewCodecBuilder[codecTestStruct]().
			WithObjectLocation("objects").
			WithSingleSigCompact().
			Build()
		require.ErrorIs(t, err, integrity.ErrSingleSigCompactCardinality)
		assert.ErrorContains(t, err, "signers=0 verifiers=0 union=0")
	})

	t.Run("signer and verifier with different names", func(t *testing.T) {
		t.Parallel()

		_, err := integrity.NewCodecBuilder[codecTestStruct]().
			WithObjectLocation("objects").
			WithSigner(newMockSigner("rsa")).
			WithVerifier(&mockVerifier{name: "ed25519", verifyErr: nil}).
			WithSingleSigCompact().
			Build()
		require.ErrorIs(t, err, integrity.ErrSingleSigCompactCardinality)
		assert.ErrorContains(t, err, "signers=1 verifiers=1 union=2")
	})
}
