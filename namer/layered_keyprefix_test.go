package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/namer"
)

func newPrefixedLayeredNamer(t *testing.T, prefix string, opts ...namer.LayeredOption) namer.Namer {
	t.Helper()

	allOpts := append([]namer.LayeredOption{namer.WithKeyPrefix(prefix)}, opts...)

	layeredN, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		allOpts...,
	)
	require.NoError(t, err)

	return layeredN
}

func TestLayeredNamer_WithKeyPrefix_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prefix  string
		wantErr error
	}{
		{"empty prefix is allowed", "", nil},
		{"missing leading slash", "a", namer.ErrKeyPrefixNoLeadingSlash},
		{"trailing slash rejected", "/a/", namer.ErrKeyPrefixTrailingSlash},
		{"single segment ok", "/a", nil},
		{"multi-segment ok", "/a/b", nil},
		{"just root slash is trailing slash", "/", namer.ErrKeyPrefixTrailingSlash},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := namer.NewLayeredNamer(
				"objects",
				nil,
				nil,
				namer.WithKeyPrefix(tt.prefix),
			)
			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestLayeredNamer_WithKeyPrefix_GenerateNames(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p")

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)
	require.Len(t, keys, 3)

	expected := []string{
		"/p/objects/alice",
		"/p/hashes/sha256/objects/alice",
		"/p/sig/ed25519/objects/alice",
	}
	for i, want := range expected {
		assert.Equal(t, want, keys[i].Build(), "key[%d].Build()", i)
		assert.Equal(t, "alice", keys[i].Name(), "key[%d].Name()", i)
	}
}

func TestLayeredNamer_WithKeyPrefix_MultiSegment(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/tenant/acme")

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	assert.Equal(t, "/tenant/acme/objects/alice", keys[0].Build())
	assert.Equal(t, "/tenant/acme/hashes/sha256/objects/alice", keys[1].Build())
	assert.Equal(t, "/tenant/acme/sig/ed25519/objects/alice", keys[2].Build())
}

func TestLayeredNamer_WithKeyPrefix_ParseRoundTrip(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p")

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	for _, key := range keys {
		parsed, perr := layeredN.ParseKey(key.Build())
		require.NoError(t, perr, "ParseKey(%q)", key.Build())
		assert.Equal(t, key.Name(), parsed.Name())
		assert.Equal(t, key.Type(), parsed.Type())
		assert.Equal(t, key.Property(), parsed.Property())
		assert.Equal(t, key.Build(), parsed.Build())
	}
}

func TestLayeredNamer_WithKeyPrefix_ParseKey_MissingPrefix(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p")

	_, err := layeredN.ParseKey("/objects/alice")
	require.ErrorIs(t, err, namer.ErrKeyPrefixMissing)
}

func TestLayeredNamer_WithKeyPrefix_Prefixes(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p")

	got := layeredN.Prefixes("", false)
	expected := []string{
		"/p/objects/",
		"/p/hashes/sha256/objects/",
		"/p/sig/ed25519/objects/",
	}
	assert.Equal(t, expected, got)

	gotWithVal := layeredN.Prefixes("alice", true)
	expectedWithVal := []string{
		"/p/objects/alice/",
		"/p/hashes/sha256/objects/alice/",
		"/p/sig/ed25519/objects/alice/",
	}
	assert.Equal(t, expectedWithVal, gotWithVal)
}

func TestLayeredNamer_WithKeyPrefix_Prefix(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p")

	assert.Equal(t, "/p/objects/", layeredN.Prefix("", false))
	assert.Equal(t, "/p/objects/alice", layeredN.Prefix("alice", false))
	assert.Equal(t, "/p/objects/alice/", layeredN.Prefix("alice", true))
}

func TestLayeredNamer_WithKeyPrefix_UnnamedMode(t *testing.T) {
	t.Parallel()

	layeredN, err := namer.NewLayeredNamer(
		namer.ObjectLocationMissing,
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
		namer.WithKeyPrefix("/p"),
	)
	require.NoError(t, err)

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	assert.Equal(t, "/p/alice", keys[0].Build())
	assert.Equal(t, "/p/hashes/sha256/alice", keys[1].Build())

	for _, key := range keys {
		parsed, perr := layeredN.ParseKey(key.Build())
		require.NoError(t, perr, "ParseKey(%q)", key.Build())
		assert.Equal(t, key.Build(), parsed.Build())
	}
}

func TestLayeredNamer_WithKeyPrefix_Legacy(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p", namer.LegacyHashSigLayout())

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	assert.Equal(t, "/p/objects/alice", keys[0].Build())
	assert.Equal(t, "/p/hashes/sha256/alice", keys[1].Build())
	assert.Equal(t, "/p/sig/ed25519/alice", keys[2].Build())
}

func TestLayeredNamer_WithKeyPrefix_Compact(t *testing.T) {
	t.Parallel()

	layeredN, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		namer.WithKeyPrefix("/p"),
		namer.CompactSingleHash(),
		namer.CompactSingleSig(),
	)
	require.NoError(t, err)

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	assert.Equal(t, "/p/objects/alice", keys[0].Build())
	assert.Equal(t, "/p/hashes/objects/alice", keys[1].Build())
	assert.Equal(t, "/p/sig/objects/alice", keys[2].Build())

	for _, key := range keys {
		parsed, perr := layeredN.ParseKey(key.Build())
		require.NoError(t, perr, "ParseKey(%q)", key.Build())
		assert.Equal(t, key.Build(), parsed.Build())
		assert.Equal(t, key.Type(), parsed.Type())
	}
}

func TestLayeredNamer_WithKeyPrefix_EmptyIsNoop(t *testing.T) {
	t.Parallel()

	withEmpty, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
		namer.WithKeyPrefix(""),
	)
	require.NoError(t, err)

	without, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		nil,
	)
	require.NoError(t, err)

	keysA, err := withEmpty.GenerateNames("alice")
	require.NoError(t, err)

	keysB, err := without.GenerateNames("alice")
	require.NoError(t, err)

	require.Len(t, keysA, len(keysB))

	for i := range keysA {
		assert.Equal(t, keysB[i].Build(), keysA[i].Build())
	}
}

// TestLayeredNamer_WithKeyPrefix_ParseKey_SiblingPrefixRejected guards against
// a subtle confused-prefix bug: prefix "/p" must not falsely match keys under
// a sibling prefix such as "/pp/..." just because "/p" is a string prefix of
// "/pp". The check must require the configured prefix to be followed by '/'
// (or the end of the key).
func TestLayeredNamer_WithKeyPrefix_ParseKey_SiblingPrefixRejected(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p")

	cases := []string{
		"/pp/objects/alice",
		"/p2/objects/alice",
		"/papa/objects/alice",
	}

	for _, raw := range cases {
		_, err := layeredN.ParseKey(raw)
		require.ErrorIs(t, err, namer.ErrKeyPrefixMissing, "ParseKey(%q)", raw)
	}
}

// TestLayeredNamer_WithKeyPrefix_OverlapsObjectLocation checks that a keyPrefix
// textually overlapping the objectLocation does not corrupt the residual
// ParseKey validates: the prefix is stripped up to a '/' boundary, so the
// following objectLocation segment is matched independently and the round-trip
// stays exact.
func TestLayeredNamer_WithKeyPrefix_OverlapsObjectLocation(t *testing.T) {
	t.Parallel()

	prefixes := []string{
		"/objects",         // prefix equals the objectLocation.
		"/a/objects/b",     // prefix contains objectLocation as an interior segment.
		"/objects/objects", // repeated objectLocation segments.
	}

	for _, prefix := range prefixes {
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()

			layeredN := newPrefixedLayeredNamer(t, prefix)

			keys, err := layeredN.GenerateNames("alice")
			require.NoError(t, err)
			require.Len(t, keys, 3)

			assert.Equal(t, prefix+"/objects/alice", keys[0].Build())

			for _, key := range keys {
				parsed, perr := layeredN.ParseKey(key.Build())
				require.NoError(t, perr, "ParseKey(%q)", key.Build())
				assert.Equal(t, "alice", parsed.Name(), "name for %q", key.Build())
				assert.Equal(t, key.Type(), parsed.Type())
				assert.Equal(t, key.Property(), parsed.Property())
				assert.Equal(t, key.Build(), parsed.Build())
			}
		})
	}
}

func TestLayeredNamer_WithKeyPrefix_MultipleHashersAndSigners(t *testing.T) {
	t.Parallel()

	layeredN, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{
			{HasherName: "sha256", Location: "sha256"},
			{HasherName: "blake3", Location: "blake3"},
		},
		[]namer.LayeredSigLocation{
			{SignerName: "ed25519", Location: "ed25519"},
			{SignerName: "rsa", Location: "rsa"},
		},
		namer.WithKeyPrefix("/p"),
	)
	require.NoError(t, err)

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)
	require.Len(t, keys, 5)

	expected := []string{
		"/p/objects/alice",
		"/p/hashes/sha256/objects/alice",
		"/p/hashes/blake3/objects/alice",
		"/p/sig/ed25519/objects/alice",
		"/p/sig/rsa/objects/alice",
	}
	for i, want := range expected {
		assert.Equal(t, want, keys[i].Build(), "key[%d].Build()", i)
	}

	for _, key := range keys {
		parsed, perr := layeredN.ParseKey(key.Build())
		require.NoError(t, perr, "ParseKey(%q)", key.Build())
		assert.Equal(t, key.Build(), parsed.Build())
		assert.Equal(t, key.Type(), parsed.Type())
		assert.Equal(t, key.Property(), parsed.Property())
	}
}

func TestLayeredNamer_WithKeyPrefix_ParseKeys_GroupsByName(t *testing.T) {
	t.Parallel()

	layeredN := newPrefixedLayeredNamer(t, "/p")

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	raws := make([]string, len(keys))
	for i, key := range keys {
		raws[i] = key.Build()
	}

	results, err := layeredN.ParseKeys(raws, false)
	require.NoError(t, err)

	assert.Equal(t, 1, results.Len())

	got, ok := results.Select("alice")
	require.True(t, ok)
	assert.Len(t, got, 3)
}
