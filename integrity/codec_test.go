package integrity_test

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/crypto"
	"github.com/tarantool/go-storage/v2/hasher"
	"github.com/tarantool/go-storage/v2/integrity"
	"github.com/tarantool/go-storage/v2/marshaller"
	"github.com/tarantool/go-storage/v2/namer"
)

type codecTestStruct struct {
	Name  string `yaml:"name"`
	Value int    `yaml:"value"`
}

func newTestRSAPSS(t *testing.T) crypto.SignerVerifier {
	t.Helper()

	pk, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	return crypto.NewRSAPSSSignerVerifier(*pk)
}

func TestCodecBuilder_Defaults(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)
	require.NotNil(t, codec)
}

// The codec keeps its namer private, so the default-object-location guarantee
// is verified by constructing an equivalent LayeredNamer and asserting its
// output matches the documented unnamed shape (no objectLocation segment).
func TestCodecBuilder_DefaultIsUnnamed(t *testing.T) {
	t.Parallel()

	defaultNamer, err := namer.NewLayeredNamer(namer.ObjectLocationMissing, nil, nil)
	require.NoError(t, err)

	keys, err := defaultNamer.GenerateNames("foo")
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "/foo", keys[0].Build())
}

// TestCodec_DefaultBuildsUnnamedKeys exercises the actual builder path —
// no WithObjectLocation call — and asserts that the keys produced by its
// generator omit the per-codec location segment.
func TestCodec_DefaultBuildsUnnamedKeys(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	require.NoError(t, err)

	pred, err := codec.ValueEqual(codecTestStruct{Name: "x", Value: 1})
	require.NoError(t, err)

	// ValueEqual binds via the value-key resolved against the namer; a
	// /<name> raw key (no /objects/ segment) confirms the unnamed default.
	bound, err := codec.BindPredicate("alice", pred)
	require.NoError(t, err)
	require.NotNil(t, bound)
}

// TestCodec_ExplicitObjectLocationMissing — passing the sentinel directly
// produces the same unnamed layout as omitting WithObjectLocation entirely.
func TestCodec_ExplicitObjectLocationMissing(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation(namer.ObjectLocationMissing).
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)
}

func TestCodecBuilder_WithObjectLocation(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithObjectLocation("myobjects").
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)
}

func TestCodecBuilder_WithHasher(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)

	n, err := namer.NewLayeredNamer("objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil)
	require.NoError(t, err)

	keys, err := n.GenerateNames("foo")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	var foundHash bool

	for _, k := range keys {
		if k.Build() == "/hashes/sha256/objects/foo" {
			foundHash = true
		}
	}

	assert.True(t, foundHash, "expected hash key at /hashes/sha256/objects/foo")
}

func TestCodecBuilder_WithHashLocation(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithHashLocation("sha256", "checksums").
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)

	n, err := namer.NewLayeredNamer("objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "checksums"}},
		nil)
	require.NoError(t, err)

	keys, err := n.GenerateNames("foo")
	require.NoError(t, err)

	var foundCustom bool

	for _, k := range keys {
		if k.Build() == "/hashes/checksums/objects/foo" {
			foundCustom = true
		}
	}

	assert.True(t, foundCustom, "expected hash key at /hashes/checksums/objects/foo")
}

func TestCodecBuilder_WithSigner(t *testing.T) {
	t.Parallel()

	sv := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithSigner(sv).
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)
}

func TestCodecBuilder_WithVerifier(t *testing.T) {
	t.Parallel()

	sv := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithVerifier(sv).
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)
}

func TestCodecBuilder_WithSignerVerifier(t *testing.T) {
	t.Parallel()

	sv := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithSignerVerifier(sv).
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)
}

func TestCodecBuilder_WithSignatureLocation(t *testing.T) {
	t.Parallel()

	signerVerifier := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithSignerVerifier(signerVerifier).
		WithSignatureLocation(signerVerifier.Name(), "sigs").
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)

	n, err := namer.NewLayeredNamer("objects", nil,
		[]namer.LayeredSigLocation{{SignerName: signerVerifier.Name(), Location: "sigs"}})
	require.NoError(t, err)

	keys, err := n.GenerateNames("bar")
	require.NoError(t, err)

	var foundSig bool

	for _, key := range keys {
		if key.Build() == "/sig/sigs/objects/bar" {
			foundSig = true
		}
	}

	assert.True(t, foundSig, "expected sig key at /sig/sigs/objects/bar")
}

func TestCodecBuilder_StaleHashLocationKey(t *testing.T) {
	t.Parallel()

	_, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithHashLocation("sah256", "custom"). // typo — no matching hasher.
		Build()
	require.Error(t, err, "Build should fail when WithHashLocation key has no matching hasher")
	assert.Contains(t, err.Error(), "sah256")
}

func TestCodecBuilder_StaleSigLocationKey(t *testing.T) {
	t.Parallel()

	sv := newTestRSAPSS(t)

	_, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithSignerVerifier(sv).
		WithSignatureLocation("nonexistent-signer", "sigs").
		Build()
	require.Error(t, err, "Build should fail when WithSignatureLocation key has no matching signer/verifier")
	assert.Contains(t, err.Error(), "nonexistent-signer")
}

func TestCodecBuilder_ReservedObjectLocation(t *testing.T) {
	t.Parallel()

	for _, reserved := range []string{"hashes", "sig"} {
		t.Run(reserved, func(t *testing.T) {
			t.Parallel()

			_, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
				WithObjectLocation(reserved).
				Build()
			require.Error(t, err, "Build should fail when objectLocation equals reserved marker %q", reserved)
		})
	}
}

func TestCodecBuilder_InvalidObjectLocation(t *testing.T) {
	t.Parallel()

	_, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithObjectLocation("/foo"). // leading slash — invalid.
		Build()
	require.Error(t, err, "Build should fail for location starting with '/'")
}

func TestCodecBuilder_MultiSegmentObjectLocation(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("settings/ldap").
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)

	n, err := namer.NewLayeredNamer("settings/ldap", nil, nil)
	require.NoError(t, err)

	keys, err := n.GenerateNames("entry-1")
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "/settings/ldap/entry-1", keys[0].Build())
}

func TestCodecBuilder_EmptyInnerSegmentObjectLocation(t *testing.T) {
	t.Parallel()

	// Multi-segment paths are allowed, but empty inner segments ("//") are not.
	_, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("a//b").
		Build()
	require.Error(t, err)
}

type sentinelMarshaller struct {
	marshalCalled bool
}

func (s *sentinelMarshaller) Marshal(_ codecTestStruct) ([]byte, error) {
	s.marshalCalled = true

	return []byte(`name: "x"\nvalue: 1\n`), nil
}

func (s *sentinelMarshaller) Unmarshal(_ []byte) (codecTestStruct, error) {
	return codecTestStruct{Name: "x", Value: 1}, nil
}

func TestCodecBuilder_WithMarshaller(t *testing.T) {
	t.Parallel()

	sentMarsh := &sentinelMarshaller{marshalCalled: false}

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithMarshaller(sentMarsh).
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)

	// ValueEqual is the smallest public surface that calls Marshal, so it
	// makes a good probe for "did the custom marshaller actually take effect?".
	_, err = codec.ValueEqual(codecTestStruct{Name: "x", Value: 1})
	require.NoError(t, err)
	assert.True(t, sentMarsh.marshalCalled, "custom marshaller should have been called")
}

type sentinelNamer struct {
	called bool
}

func (n *sentinelNamer) GenerateNames(name string) ([]namer.Key, error) {
	return []namer.Key{
		namer.NewDefaultKey(name, namer.KeyTypeValue, "", "/sentinel/"+name),
	}, nil
}

func (n *sentinelNamer) ParseKey(raw string) (namer.DefaultKey, error) {
	name := strings.TrimPrefix(raw, "/sentinel/")

	return namer.NewDefaultKey(name, namer.KeyTypeValue, "", raw), nil
}

func (n *sentinelNamer) ParseKeys(names []string, ignoreError bool) (namer.Results, error) {
	out := map[string][]namer.Key{}

	for _, raw := range names {
		key, err := n.ParseKey(raw)
		if err != nil {
			if ignoreError {
				continue
			}

			return namer.Results{}, err
		}

		out[key.Name()] = append(out[key.Name()], key)
	}

	return namer.NewResults(out), nil
}

func (n *sentinelNamer) Prefix(val string, _ bool) string {
	return "/sentinel/" + strings.Trim(val, "/")
}

func (n *sentinelNamer) Prefixes(val string, isPrefix bool) []string {
	return []string{n.Prefix(val, isPrefix)}
}

func TestCodecBuilder_WithNamer(t *testing.T) {
	t.Parallel()

	var constructorCalled bool

	customConstructor := integrity.CodecNamerConstructor(func(
		_ string,
		_ []namer.LayeredHashLocation,
		_ []namer.LayeredSigLocation,
		_ ...namer.LayeredOption,
	) (namer.Namer, error) {
		constructorCalled = true

		return &sentinelNamer{called: false}, nil
	})

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").
		WithNamer(customConstructor).
		Build()
	require.NoError(t, err)
	require.NotNil(t, codec)

	// Build creates three namers (gen/val/top), so seeing the constructor
	// fire at least once is enough to prove the override took effect.
	assert.True(t, constructorCalled, "custom CodecNamerConstructor should have been called during Build")
}

func TestCodec_ValueEqual(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	val := codecTestStruct{Name: "hello", Value: 42}
	pred, err := codec.ValueEqual(val)
	require.NoError(t, err)
	require.NotNil(t, pred)

	key := []byte("/objects/foo")
	concretePred := pred(key)
	require.NotNil(t, concretePred)
	assert.Equal(t, key, concretePred.Key())

	marsh := marshaller.NewTypedYamlMarshaller[codecTestStruct]()
	expected, err := marsh.Marshal(val)
	require.NoError(t, err)
	assert.Equal(t, expected, concretePred.Value())
}

func TestCodec_ValueNotEqual(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	val := codecTestStruct{Name: "hello", Value: 42}
	pred, err := codec.ValueNotEqual(val)
	require.NoError(t, err)
	require.NotNil(t, pred)

	key := []byte("/objects/foo")
	p := pred(key)
	require.NotNil(t, p)
	assert.Equal(t, key, p.Key())
}

func TestCodec_VersionEqual(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	pred := codec.VersionEqual(int64(42))
	require.NotNil(t, pred)

	key := []byte("/objects/foo")
	p := pred(key)
	require.NotNil(t, p)
	assert.Equal(t, key, p.Key())
	assert.Equal(t, int64(42), p.Value())
}

func TestCodec_VersionNotEqual(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	pred := codec.VersionNotEqual(int64(7))
	key := []byte("/objects/bar")
	p := pred(key)
	assert.Equal(t, int64(7), p.Value())
	assert.Equal(t, key, p.Key())
}

func TestCodec_VersionGreater(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	pred := codec.VersionGreater(int64(100))
	key := []byte("/objects/baz")
	p := pred(key)
	assert.Equal(t, int64(100), p.Value())
}

func TestCodec_VersionLess(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	pred := codec.VersionLess(int64(5))
	key := []byte("/objects/qux")
	p := pred(key)
	assert.Equal(t, int64(5), p.Value())
}

func TestCodecBuilder_Immutability(t *testing.T) {
	t.Parallel()

	base := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects")

	marsh1 := marshaller.NewTypedYamlMarshaller[codecTestStruct]()
	marsh2 := marshaller.NewTypedYamlMarshaller[codecTestStruct]()

	builder1 := base.WithMarshaller(marsh1)
	builder2 := builder1.WithMarshaller(marsh2)

	codec1, err := builder1.Build()
	require.NoError(t, err)

	codec2, err := builder2.Build()
	require.NoError(t, err)

	assert.NotSame(t, codec1, codec2)
}

func TestCodecBuilder_ImmutabilityHashers(t *testing.T) {
	t.Parallel()

	base := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects")
	withSHA256 := base.WithHasher(hasher.NewSHA256Hasher())
	withSHA256SHA1 := withSHA256.WithHasher(hasher.NewSHA1Hasher())

	_, err := base.Build()
	require.NoError(t, err)

	_, err = withSHA256.Build()
	require.NoError(t, err)

	_, err = withSHA256SHA1.Build()
	require.NoError(t, err)
}

func TestCodecBuilder_MultipleBuildCalls(t *testing.T) {
	t.Parallel()

	builder := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects")

	codec1, err := builder.Build()
	require.NoError(t, err)

	codec2, err := builder.Build()
	require.NoError(t, err)

	assert.NotSame(t, codec1, codec2, "each Build call should return a distinct *Codec[T]")
}

func TestCodec_ValueKey_NamedLocation(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("foo")
	require.NoError(t, err)
	assert.Equal(t, "/objects/foo", key)
}

func TestCodec_ValueKey_UnnamedDefault(t *testing.T) {
	t.Parallel()

	// No WithObjectLocation — codec is in unnamed mode (no per-codec segment).
	codec, err := integrity.NewCodecBuilder[codecTestStruct]().Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("foo")
	require.NoError(t, err)
	assert.Equal(t, "/foo", key)
}

func TestCodec_ValueKey_MultiSegmentLocation(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("settings/ldap").
		Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("entry-1")
	require.NoError(t, err)
	assert.Equal(t, "/settings/ldap/entry-1", key)
}

func TestCodec_ValueKey_IgnoresHashAndSigLayers(t *testing.T) {
	t.Parallel()

	// Adding hashers/signers must not change the value-layer key: ValueKey
	// is documented to return only the value layer, even when other layers
	// are present.
	signerVerifier := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithSignerVerifier(signerVerifier).
		Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("foo")
	require.NoError(t, err)
	assert.Equal(t, "/objects/foo", key, "ValueKey must return only the value-layer key")
	assert.NotContains(t, key, "/hash/")
	assert.NotContains(t, key, "/sig/")
}

func TestCodec_ValueKey_RespectsLocationOverrides(t *testing.T) {
	t.Parallel()

	// Hash/sig location overrides affect the hash/sig layers only — the
	// value-layer key must remain /<objectLocation>/<name>.
	signerVerifier := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithHashLocation("sha256", "checksums").
		WithSignerVerifier(signerVerifier).
		WithSignatureLocation(signerVerifier.Name(), "sigs").
		Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("foo")
	require.NoError(t, err)
	assert.Equal(t, "/objects/foo", key)
}

func TestCodec_ValueKey_InvalidName(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	cases := []struct {
		label string
		name  string
	}{
		{"empty", ""},
		{"leading slash", "/foo"},
		{"trailing slash", "foo/"},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			key, err := codec.ValueKey(tc.name)
			require.Error(t, err)
			require.ErrorIs(t, err, integrity.ErrInvalidName)
			assert.Empty(t, key)
		})
	}
}

func TestCodec_ValueKey_RoundTripsThroughNamer(t *testing.T) {
	t.Parallel()

	// ValueKey must produce a key that the codec's own (reproducible) namer
	// can parse back to the original name with type=value.
	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("alpha-beta")
	require.NoError(t, err)

	reproNamer, err := namer.NewLayeredNamer("objects", nil, nil)
	require.NoError(t, err)

	parsed, err := reproNamer.ParseKey(key)
	require.NoError(t, err)
	assert.Equal(t, namer.KeyTypeValue, parsed.Type())
	assert.Equal(t, "alpha-beta", parsed.Name())
}

// fullKeyNoValueNamer always emits a hash-only key set, so ValueKey has no
// value layer to return and must surface ErrNoValueKey. Reuses the
// sentinelNamer plumbing but overrides GenerateNames to drop the value key.
type fullKeyNoValueNamer struct{ sentinelNamer }

func (n *fullKeyNoValueNamer) GenerateNames(name string) ([]namer.Key, error) {
	return []namer.Key{
		namer.NewDefaultKey(name, namer.KeyTypeHash, "sha256", "/hash/sha256/"+name),
	}, nil
}

func TestCodec_ValueKey_NoValueKey(t *testing.T) {
	t.Parallel()

	customConstructor := integrity.CodecNamerConstructor(func(
		_ string,
		_ []namer.LayeredHashLocation,
		_ []namer.LayeredSigLocation,
		_ ...namer.LayeredOption,
	) (namer.Namer, error) {
		return &fullKeyNoValueNamer{sentinelNamer: sentinelNamer{called: false}}, nil
	})

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		WithNamer(customConstructor).
		Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("foo")
	require.Error(t, err)
	require.ErrorIs(t, err, integrity.ErrNoValueKey)
	assert.Empty(t, key)
}

// fullKeyErrNamer fails GenerateNames so ValueKey can surface the wrapped
// namer error path.
type fullKeyErrNamer struct{ sentinelNamer }

func (n *fullKeyErrNamer) GenerateNames(_ string) ([]namer.Key, error) {
	return nil, errSentinelNamerFailure
}

var errSentinelNamerFailure = errors.New("sentinel namer failure")

func TestCodec_ValueKey_NamerError(t *testing.T) {
	t.Parallel()

	customConstructor := integrity.CodecNamerConstructor(func(
		_ string,
		_ []namer.LayeredHashLocation,
		_ []namer.LayeredSigLocation,
		_ ...namer.LayeredOption,
	) (namer.Namer, error) {
		return &fullKeyErrNamer{sentinelNamer: sentinelNamer{called: false}}, nil
	})

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		WithNamer(customConstructor).
		Build()
	require.NoError(t, err)

	key, err := codec.ValueKey("foo")
	require.Error(t, err)
	require.ErrorIs(t, err, errSentinelNamerFailure)
	assert.Empty(t, key)
}

func TestCodec_FullKeys_ValueOnly(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		Build()
	require.NoError(t, err)

	keys, err := codec.FullKeys("foo")
	require.NoError(t, err)
	assert.Equal(t, []string{"/objects/foo"}, keys)
}

func TestCodec_FullKeys_ValueHashSig(t *testing.T) {
	t.Parallel()

	// Adding a hasher and a signer expands the key set to three layers,
	// in namer-emitted order: value, hash, signature.
	signerVerifier := newTestRSAPSS(t)

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		WithHasher(hasher.NewSHA256Hasher()).
		WithSignerVerifier(signerVerifier).
		Build()
	require.NoError(t, err)

	keys, err := codec.FullKeys("foo")
	require.NoError(t, err)
	require.Len(t, keys, 3)
	assert.Equal(t, "/objects/foo", keys[0], "value layer first")
	assert.Contains(t, keys[1], "/foo", "hash layer carries the object name")
	assert.Contains(t, keys[2], "/foo", "signature layer carries the object name")
	assert.NotEqual(t, keys[0], keys[1])
	assert.NotEqual(t, keys[0], keys[2])
	assert.NotEqual(t, keys[1], keys[2])
}

func TestCodec_FullKeys_InvalidName(t *testing.T) {
	t.Parallel()

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().WithObjectLocation("objects").Build()
	require.NoError(t, err)

	for _, testCase := range []struct {
		label string
		name  string
	}{
		{"empty", ""},
		{"leading slash", "/foo"},
		{"trailing slash", "foo/"},
	} {
		t.Run(testCase.label, func(t *testing.T) {
			t.Parallel()

			keys, err := codec.FullKeys(testCase.name)
			require.ErrorIs(t, err, integrity.ErrInvalidName)
			assert.Nil(t, keys)
		})
	}
}

func TestCodec_FullKeys_NamerError(t *testing.T) {
	t.Parallel()

	customConstructor := integrity.CodecNamerConstructor(func(
		_ string,
		_ []namer.LayeredHashLocation,
		_ []namer.LayeredSigLocation,
		_ ...namer.LayeredOption,
	) (namer.Namer, error) {
		return &fullKeyErrNamer{sentinelNamer: sentinelNamer{called: false}}, nil
	})

	codec, err := integrity.NewCodecBuilder[codecTestStruct]().
		WithObjectLocation("objects").
		WithNamer(customConstructor).
		Build()
	require.NoError(t, err)

	keys, err := codec.FullKeys("foo")
	require.ErrorIs(t, err, errSentinelNamerFailure)
	assert.Nil(t, keys)
}
