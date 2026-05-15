package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/namer"
)

func newLegacyNamer(
	t *testing.T,
	objectLocation string,
	hashLocs []namer.LayeredHashLocation,
	sigLocs []namer.LayeredSigLocation,
	extra ...namer.LayeredOption,
) namer.Namer {
	t.Helper()

	opts := append([]namer.LayeredOption{namer.LegacyHashSigLayout()}, extra...)

	n, err := namer.NewLayeredNamer(objectLocation, hashLocs, sigLocs, opts...)
	require.NoError(t, err)

	return n
}

func TestLegacyLayout_GenerateNames(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "config",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	keys, err := nmr.GenerateNames("all")
	require.NoError(t, err)
	require.Len(t, keys, 3)

	assert.Equal(t, "/config/all", keys[0].Build())
	assert.Equal(t, namer.KeyTypeValue, keys[0].Type())
	assert.Equal(t, "all", keys[0].Name())

	assert.Equal(t, "/hashes/sha256/all", keys[1].Build())
	assert.Equal(t, namer.KeyTypeHash, keys[1].Type())
	assert.Equal(t, "sha256", keys[1].Property())

	assert.Equal(t, "/sig/ed25519/all", keys[2].Build())
	assert.Equal(t, namer.KeyTypeSignature, keys[2].Type())
	assert.Equal(t, "ed25519", keys[2].Property())
}

func TestLegacyLayout_MultiSegmentObjectLocation(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "settings/ldap",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
	)

	keys, err := nmr.GenerateNames("alice")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	assert.Equal(t, "/settings/ldap/alice", keys[0].Build())
	assert.Equal(t, "/hashes/sha256/alice", keys[1].Build())
}

func TestLegacyLayout_CompactSingleHash(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "config",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
		namer.CompactSingleHash(),
	)

	keys, err := nmr.GenerateNames("all")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	assert.Equal(t, "/config/all", keys[0].Build())
	assert.Equal(t, "/hashes/all", keys[1].Build())
}

func TestLegacyLayout_CompactSingleSig(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "config", nil,
		[]namer.LayeredSigLocation{{SignerName: "rsa", Location: "rsa"}},
		namer.CompactSingleSig(),
	)

	keys, err := nmr.GenerateNames("all")
	require.NoError(t, err)
	require.Len(t, keys, 2)

	assert.Equal(t, "/config/all", keys[0].Build())
	assert.Equal(t, "/sig/all", keys[1].Build())
}

func TestLegacyLayout_ParseKey(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "config",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	cases := []struct {
		raw      string
		typ      namer.KeyType
		name     string
		property string
	}{
		{"/config/all", namer.KeyTypeValue, "all", ""},
		{"/hashes/sha256/all", namer.KeyTypeHash, "all", "sha256"},
		{"/sig/ed25519/all", namer.KeyTypeSignature, "all", "ed25519"},
	}

	for _, tt := range cases {
		t.Run(tt.raw, func(t *testing.T) {
			t.Parallel()

			k, err := nmr.ParseKey(tt.raw)
			require.NoError(t, err)
			assert.Equal(t, tt.typ, k.Type())
			assert.Equal(t, tt.name, k.Name())
			assert.Equal(t, tt.property, k.Property())
		})
	}
}

func TestLegacyLayout_ParseKey_NamesMayContainSlashes(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "config",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
	)

	// Legacy hash keys have shape /hashes/<hashLoc>/<name>; the parser treats
	// every segment after <hashLoc> as the (possibly slash-containing) name.
	k, err := nmr.ParseKey("/hashes/sha256/sub/all")
	require.NoError(t, err)
	assert.Equal(t, namer.KeyTypeHash, k.Type())
	assert.Equal(t, "sub/all", k.Name())
	assert.Equal(t, "sha256", k.Property())
}

func TestLegacyLayout_RoundTrip(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "config",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	keys, err := nmr.GenerateNames("alice")
	require.NoError(t, err)

	for _, k := range keys {
		parsed, err := nmr.ParseKey(k.Build())
		require.NoError(t, err, "round-trip parse %q", k.Build())
		assert.Equal(t, k.Type(), parsed.Type())
		assert.Equal(t, k.Name(), parsed.Name())
		assert.Equal(t, k.Property(), parsed.Property())
	}
}

func TestLegacyLayout_Prefixes(t *testing.T) {
	t.Parallel()

	nmr := newLegacyNamer(t, "config",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	got := nmr.Prefixes("", false)
	assert.Equal(t, []string{
		"/config/",
		"/hashes/sha256/",
		"/sig/ed25519/",
	}, got)

	got = nmr.Prefixes("alice", true)
	assert.Equal(t, []string{
		"/config/alice/",
		"/hashes/sha256/alice/",
		"/sig/ed25519/alice/",
	}, got)
}

func TestLegacyLayout_UnnamedIsNoop(t *testing.T) {
	t.Parallel()

	// In unnamed mode the layout already omits objectLocation; the legacy
	// option has no effect.
	legacy := newLegacyNamer(t, namer.ObjectLocationMissing,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	plain, err := namer.NewLayeredNamer(namer.ObjectLocationMissing,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)
	require.NoError(t, err)

	legacyKeys, err := legacy.GenerateNames("alice")
	require.NoError(t, err)

	plainKeys, err := plain.GenerateNames("alice")
	require.NoError(t, err)

	require.Len(t, legacyKeys, len(plainKeys))

	for i := range legacyKeys {
		assert.Equal(t, plainKeys[i].Build(), legacyKeys[i].Build())
	}
}
