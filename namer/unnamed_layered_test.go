package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/namer"
)

func newUnnamedNamer(
	t *testing.T,
	hashLocs []namer.LayeredHashLocation,
	sigLocs []namer.LayeredSigLocation,
	opts ...namer.LayeredOption,
) namer.Namer {
	t.Helper()

	n, err := namer.NewLayeredNamer(namer.ObjectLocationMissing, hashLocs, sigLocs, opts...)
	require.NoError(t, err)

	return n
}

func TestUnnamedLayeredNamer_GenerateNames(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	keys, err := nmr.GenerateNames("alice")
	require.NoError(t, err)
	require.Len(t, keys, 3)

	assert.Equal(t, "/alice", keys[0].Build())
	assert.Equal(t, namer.KeyTypeValue, keys[0].Type())
	assert.Equal(t, "alice", keys[0].Name())

	assert.Equal(t, "/hashes/sha256/alice", keys[1].Build())
	assert.Equal(t, namer.KeyTypeHash, keys[1].Type())
	assert.Equal(t, "sha256", keys[1].Property())

	assert.Equal(t, "/sig/ed25519/alice", keys[2].Build())
	assert.Equal(t, namer.KeyTypeSignature, keys[2].Type())
	assert.Equal(t, "ed25519", keys[2].Property())
}

func TestUnnamedLayeredNamer_GenerateNames_RejectsReservedFirstSegment(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	cases := []string{"hashes", "sig", "hashes/foo", "sig/bar/baz"}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := nmr.GenerateNames(name)
			require.Error(t, err)

			var nameErr namer.InvalidNameError

			require.ErrorAs(t, err, &nameErr)
		})
	}
}

func TestUnnamedLayeredNamer_GenerateNames_AcceptsNonReservedNames(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t, nil, nil)

	for _, name := range []string{"hashish", "signal", "alice/sub", "x"} {
		keys, err := nmr.GenerateNames(name)
		require.NoError(t, err, "name=%q", name)
		require.NotEmpty(t, keys)
	}
}

func TestUnnamedLayeredNamer_ParseKey_Value(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t, nil, nil)

	parsed, err := nmr.ParseKey("/alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", parsed.Name())
	assert.Equal(t, namer.KeyTypeValue, parsed.Type())

	parsed, err = nmr.ParseKey("/foo/bar/baz")
	require.NoError(t, err)
	assert.Equal(t, "foo/bar/baz", parsed.Name())
	assert.Equal(t, namer.KeyTypeValue, parsed.Type())
}

func TestUnnamedLayeredNamer_ParseKey_HashAndSig(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	parsed, err := nmr.ParseKey("/hashes/sha256/alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", parsed.Name())
	assert.Equal(t, namer.KeyTypeHash, parsed.Type())
	assert.Equal(t, "sha256", parsed.Property())

	parsed, err = nmr.ParseKey("/sig/ed25519/alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", parsed.Name())
	assert.Equal(t, namer.KeyTypeSignature, parsed.Type())
	assert.Equal(t, "ed25519", parsed.Property())
}

func TestUnnamedLayeredNamer_ParseKey_Errors(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	cases := []string{
		"",
		"/",
		"/hashes/",
		"/hashes/sha256/",
		"/hashes/unknown/alice",
		"/sig/",
		"/sig/unknown/alice",
		"/alice/",
	}

	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			_, err := nmr.ParseKey(raw)
			require.Error(t, err)
		})
	}
}

func TestUnnamedLayeredNamer_RoundTrip(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	for _, name := range []string{"alice", "users/bob", "deeply/nested/name"} {
		keys, err := nmr.GenerateNames(name)
		require.NoError(t, err, name)

		for _, key := range keys {
			parsed, err := nmr.ParseKey(key.Build())
			require.NoError(t, err, "raw=%q", key.Build())
			assert.Equal(t, key.Name(), parsed.Name(), "raw=%q", key.Build())
			assert.Equal(t, key.Type(), parsed.Type(), "raw=%q", key.Build())
			assert.Equal(t, key.Property(), parsed.Property(), "raw=%q", key.Build())
		}
	}
}

func TestUnnamedLayeredNamer_Prefix(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t, nil, nil)

	assert.Equal(t, "/", nmr.Prefix("", true))
	assert.Equal(t, "/", nmr.Prefix("", false))
	assert.Equal(t, "/alice", nmr.Prefix("alice", false))
	assert.Equal(t, "/alice/", nmr.Prefix("alice", true))
	assert.Equal(t, "/alice", nmr.Prefix("/alice/", false))
}

func TestUnnamedLayeredNamer_Compact(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		namer.CompactSingleHash(),
		namer.CompactSingleSig(),
	)

	keys, err := nmr.GenerateNames("alice")
	require.NoError(t, err)
	require.Len(t, keys, 3)

	assert.Equal(t, "/alice", keys[0].Build())
	assert.Equal(t, "/hashes/alice", keys[1].Build())
	assert.Equal(t, "/sig/alice", keys[2].Build())

	for _, key := range keys {
		parsed, err := nmr.ParseKey(key.Build())
		require.NoError(t, err, "raw=%q", key.Build())
		assert.Equal(t, key.Name(), parsed.Name())
		assert.Equal(t, key.Type(), parsed.Type())
	}
}

func TestUnnamedLayeredNamer_Compact_ParseErrors(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		namer.CompactSingleHash(),
		namer.CompactSingleSig(),
	)

	// Compact + unnamed: hash/<name> must not be empty or end with '/'.
	cases := []string{"/hashes/", "/hashes/alice/", "/sig/", "/sig/alice/"}

	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			_, err := nmr.ParseKey(raw)
			require.Error(t, err)
		})
	}
}

func TestUnnamedLayeredNamer_Prefixes(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)

	// Unnamed-mode roots: value at "/", hash at "/hashes/<loc>/", sig at "/sig/<loc>/".
	prefixes := nmr.Prefixes("", true)
	assert.Equal(t, []string{"/", "/hashes/sha256/", "/sig/ed25519/"}, prefixes)

	prefixes = nmr.Prefixes("alice", false)
	assert.Equal(t, []string{"/alice", "/hashes/sha256/alice", "/sig/ed25519/alice"}, prefixes)

	prefixes = nmr.Prefixes("alice", true)
	assert.Equal(t, []string{"/alice/", "/hashes/sha256/alice/", "/sig/ed25519/alice/"}, prefixes)
}

func TestUnnamedLayeredNamer_Compact_Prefixes(t *testing.T) {
	t.Parallel()

	nmr := newUnnamedNamer(t,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		namer.CompactSingleHash(),
		namer.CompactSingleSig(),
	)

	// Compact + unnamed: hashLocation / sigLocation segments are also dropped.
	prefixes := nmr.Prefixes("", true)
	assert.Equal(t, []string{"/", "/hashes/", "/sig/"}, prefixes)
}
