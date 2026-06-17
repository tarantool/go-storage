package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/namer"
)

// Compile-time check that NewLayeredNamer returns a Namer; the concrete type
// is unexported, so the assertion has to go through the constructor.
var _ namer.Namer = func() namer.Namer {
	layeredN, err := namer.NewLayeredNamer("objects", nil, nil)
	if err != nil {
		panic(err)
	}

	return layeredN
}()

func TestLayeredNamer_Constructor_Validation(t *testing.T) {
	t.Parallel()

	type args struct {
		objectLocation string
		hashLocations  []namer.LayeredHashLocation
		sigLocations   []namer.LayeredSigLocation
		opts           []namer.LayeredOption
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			// ObjectLocationMissing ("") selects unnamed mode and is
			// accepted at construction time.
			name: "empty objectLocation (unnamed mode)",
			args: args{
				objectLocation: namer.ObjectLocationMissing,
				hashLocations:  nil,
				sigLocations:   nil,
				opts:           nil,
			},
			wantErr: false,
		},
		{
			name:    "objectLocation with leading slash",
			args:    args{objectLocation: "/objects", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: true,
		},
		{
			name:    "objectLocation with trailing slash",
			args:    args{objectLocation: "objects/", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: true,
		},
		{
			name:    "objectLocation with inner slash is valid (multi-segment)",
			args:    args{objectLocation: "obj/ects", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: false,
		},
		{
			name:    "objectLocation with empty inner segment",
			args:    args{objectLocation: "a//b", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: true,
		},
		{
			name:    "objectLocation reserved: hashes",
			args:    args{objectLocation: "hashes", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: true,
		},
		{
			name:    "objectLocation reserved: sig",
			args:    args{objectLocation: "sig", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: true,
		},
		{
			name:    "objectLocation with reserved first segment: hashes/foo",
			args:    args{objectLocation: "hashes/foo", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: true,
		},
		{
			name:    "objectLocation with reserved first segment: sig/foo",
			args:    args{objectLocation: "sig/foo", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: true,
		},
		{
			name:    "objectLocation with non-reserved segments and inner /",
			args:    args{objectLocation: "settings/ldap", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: false,
		},
		{
			name: "hash Location with leading slash",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: "/hashes"}},
				sigLocations:   nil,
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "hash Location with trailing slash",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: "hashes/"}},
				sigLocations:   nil,
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "hash Location with inner slash",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: "hash/sha256"}},
				sigLocations:   nil,
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "hash Location empty",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: ""}},
				sigLocations:   nil,
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "hash HasherName empty",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "", Location: "sha256"}},
				sigLocations:   nil,
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "sig Location with leading slash",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "ed25519", Location: "/sigs"}},
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "sig Location with trailing slash",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "ed25519", Location: "sigs/"}},
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "sig Location with inner slash",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "ed25519", Location: "sig/ed25519"}},
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "sig Location empty",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "ed25519", Location: ""}},
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "sig SignerName empty",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "", Location: "ed25519"}},
				opts:           nil,
			},
			wantErr: true,
		},
		{
			name: "duplicate hash entries share location",
			args: args{
				objectLocation: "objects",
				hashLocations: []namer.LayeredHashLocation{
					{HasherName: "sha256", Location: "shared"},
					{HasherName: "sha512", Location: "shared"},
				},
				sigLocations: nil,
				opts:         nil,
			},
			wantErr: true,
		},
		{
			name: "duplicate sig entries share location",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations: []namer.LayeredSigLocation{
					{SignerName: "ed25519", Location: "shared"},
					{SignerName: "rsa", Location: "shared"},
				},
				opts: nil,
			},
			wantErr: true,
		},
		{
			name: "valid: object and hash share location (different namespaces)",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: "objects"}},
				sigLocations:   nil,
				opts:           nil,
			},
			wantErr: false,
		},
		{
			name: "valid: hash and sig share location (different markers)",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: "common"}},
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "ed25519", Location: "common"}},
				opts:           nil,
			},
			wantErr: false,
		},
		{
			name:    "valid: no hash/sig",
			args:    args{objectLocation: "objects", hashLocations: nil, sigLocations: nil, opts: nil},
			wantErr: false,
		},
		{
			name: "valid: with hash and sig",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
				opts:           nil,
			},
			wantErr: false,
		},
		{
			name: "compact hash: zero entries fails",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   nil,
				opts:           []namer.LayeredOption{namer.CompactSingleHash()},
			},
			wantErr: true,
		},
		{
			name: "compact hash: two entries fails",
			args: args{
				objectLocation: "objects",
				hashLocations: []namer.LayeredHashLocation{
					{HasherName: "sha256", Location: "sha256"},
					{HasherName: "sha512", Location: "sha512"},
				},
				sigLocations: nil,
				opts:         []namer.LayeredOption{namer.CompactSingleHash()},
			},
			wantErr: true,
		},
		{
			name: "compact hash: one entry succeeds",
			args: args{
				objectLocation: "objects",
				hashLocations:  []namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
				sigLocations:   nil,
				opts:           []namer.LayeredOption{namer.CompactSingleHash()},
			},
			wantErr: false,
		},
		{
			name: "compact sig: zero entries fails",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   nil,
				opts:           []namer.LayeredOption{namer.CompactSingleSig()},
			},
			wantErr: true,
		},
		{
			name: "compact sig: two entries fails",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations: []namer.LayeredSigLocation{
					{SignerName: "ed25519", Location: "ed25519"},
					{SignerName: "rsa", Location: "rsa"},
				},
				opts: []namer.LayeredOption{namer.CompactSingleSig()},
			},
			wantErr: true,
		},
		{
			name: "compact sig: one entry succeeds",
			args: args{
				objectLocation: "objects",
				hashLocations:  nil,
				sigLocations:   []namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
				opts:           []namer.LayeredOption{namer.CompactSingleSig()},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := namer.NewLayeredNamer(
				tt.args.objectLocation,
				tt.args.hashLocations,
				tt.args.sigLocations,
				tt.args.opts...,
			)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLayeredNamer_Constructor_CompactErrors_Sentinels(t *testing.T) {
	t.Parallel()

	t.Run("compact hash cardinality error is sentinel", func(t *testing.T) {
		t.Parallel()

		_, err := namer.NewLayeredNamer("objects", nil, nil, namer.CompactSingleHash())
		require.Error(t, err)
		assert.ErrorIs(t, err, namer.ErrCompactSingleHashCardinality)
	})

	t.Run("compact sig cardinality error is sentinel", func(t *testing.T) {
		t.Parallel()

		_, err := namer.NewLayeredNamer("objects", nil, nil, namer.CompactSingleSig())
		require.Error(t, err)
		assert.ErrorIs(t, err, namer.ErrCompactSingleSigCardinality)
	})

	t.Run("reserved objectLocation error is sentinel", func(t *testing.T) {
		t.Parallel()

		_, err := namer.NewLayeredNamer("hashes", nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, namer.ErrObjectLocationReserved)
	})
}

func newTestLayeredNamer(t *testing.T) namer.Namer {
	t.Helper()

	layeredN, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
	)
	require.NoError(t, err)

	return layeredN
}

func TestLayeredNamer_GenerateNames_Success(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)
	require.Len(t, keys, 3)

	expected := []struct {
		build    string
		keyType  namer.KeyType
		property string
		name     string
	}{
		{"/objects/alice", namer.KeyTypeValue, "", "alice"},
		{"/hashes/sha256/objects/alice", namer.KeyTypeHash, "sha256", "alice"},
		{"/sig/ed25519/objects/alice", namer.KeyTypeSignature, "ed25519", "alice"},
	}

	for i, exp := range expected {
		assert.Equal(t, exp.build, keys[i].Build(), "key[%d].Build()", i)
		assert.Equal(t, exp.keyType, keys[i].Type(), "key[%d].Type()", i)
		assert.Equal(t, exp.property, keys[i].Property(), "key[%d].Property()", i)
		assert.Equal(t, exp.name, keys[i].Name(), "key[%d].Name()", i)
	}
}

func TestLayeredNamer_GenerateNames_EmptyName(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	_, err := layeredN.GenerateNames("")
	require.Error(t, err)

	var nameErr namer.InvalidNameError
	require.ErrorAs(t, err, &nameErr)
}

func TestLayeredNamer_GenerateNames_LeadingSlash(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	keys, err := layeredN.GenerateNames("/alice")
	require.NoError(t, err)
	require.Len(t, keys, 3)

	assert.Equal(t, "/objects/alice", keys[0].Build())
	assert.Equal(t, "alice", keys[0].Name())
}

func TestLayeredNamer_GenerateNames_Compact(t *testing.T) {
	t.Parallel()

	t.Run("compact hash drops hashLocation segment", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(
			"objects",
			[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
			nil,
			namer.CompactSingleHash(),
		)
		require.NoError(t, err)

		keys, err := layeredN.GenerateNames("alice")
		require.NoError(t, err)
		require.Len(t, keys, 2)

		assert.Equal(t, "/objects/alice", keys[0].Build())
		assert.Equal(t, "/hashes/objects/alice", keys[1].Build())
		assert.Equal(t, namer.KeyTypeHash, keys[1].Type())
		assert.Equal(t, "sha256", keys[1].Property())
	})

	t.Run("compact sig drops sigLocation segment", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(
			"objects",
			nil,
			[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
			namer.CompactSingleSig(),
		)
		require.NoError(t, err)

		keys, err := layeredN.GenerateNames("alice")
		require.NoError(t, err)
		require.Len(t, keys, 2)

		assert.Equal(t, "/objects/alice", keys[0].Build())
		assert.Equal(t, "/sig/objects/alice", keys[1].Build())
		assert.Equal(t, namer.KeyTypeSignature, keys[1].Type())
		assert.Equal(t, "ed25519", keys[1].Property())
	})

	t.Run("both compact flags", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(
			"objects",
			[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
			[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
			namer.CompactSingleHash(),
			namer.CompactSingleSig(),
		)
		require.NoError(t, err)

		keys, err := layeredN.GenerateNames("alice")
		require.NoError(t, err)
		require.Len(t, keys, 3)

		assert.Equal(t, "/objects/alice", keys[0].Build())
		assert.Equal(t, "/hashes/objects/alice", keys[1].Build())
		assert.Equal(t, "/sig/objects/alice", keys[2].Build())
	})
}

func TestLayeredNamer_ParseKey_RoundTrip(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	for _, nkey := range keys {
		t.Run(nkey.Build(), func(t *testing.T) {
			t.Parallel()

			parsed, err := layeredN.ParseKey(nkey.Build())
			require.NoError(t, err)

			assert.Equal(t, nkey.Name(), parsed.Name())
			assert.Equal(t, nkey.Type(), parsed.Type())
			assert.Equal(t, nkey.Property(), parsed.Property())
			assert.Equal(t, nkey.Build(), parsed.Build())
		})
	}
}

func TestLayeredNamer_ParseKey_RoundTrip_Compact(t *testing.T) {
	t.Parallel()

	layeredN, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		namer.CompactSingleHash(),
		namer.CompactSingleSig(),
	)
	require.NoError(t, err)

	keys, err := layeredN.GenerateNames("alice")
	require.NoError(t, err)

	for _, nkey := range keys {
		t.Run(nkey.Build(), func(t *testing.T) {
			t.Parallel()

			parsed, err := layeredN.ParseKey(nkey.Build())
			require.NoError(t, err)

			assert.Equal(t, nkey.Name(), parsed.Name())
			assert.Equal(t, nkey.Type(), parsed.Type())
			assert.Equal(t, nkey.Property(), parsed.Property())
			assert.Equal(t, nkey.Build(), parsed.Build())
		})
	}
}

func TestLayeredNamer_ParseKey_Errors(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "unknown location segment",
			raw:  "/unknown/alice",
		},
		{
			name: "empty name part (trailing slash after location)",
			raw:  "/objects/",
		},
		{
			name: "name part has trailing slash",
			raw:  "/objects/alice/",
		},
		{
			name: "missing name segment (only location)",
			raw:  "/objects",
		},
		{
			name: "completely empty",
			raw:  "",
		},
		{
			name: "hash key with unknown hashLocation",
			raw:  "/hashes/unknown/objects/alice",
		},
		{
			name: "hash key with mismatched objectLocation",
			raw:  "/hashes/sha256/wrong/alice",
		},
		{
			name: "hash key missing objectLocation segment",
			raw:  "/hashes/sha256/alice",
		},
		{
			name: "hash key missing name",
			raw:  "/hashes/sha256/objects",
		},
		{
			name: "hash key missing hashLocation",
			raw:  "/hash",
		},
		{
			name: "sig key with unknown sigLocation",
			raw:  "/sig/unknown/objects/alice",
		},
		{
			name: "sig key with mismatched objectLocation",
			raw:  "/sig/ed25519/wrong/alice",
		},
		{
			name: "sig key missing name",
			raw:  "/sig/ed25519/objects",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := layeredN.ParseKey(tt.raw)
			require.Error(t, err)
		})
	}
}

func TestLayeredNamer_ParseKeys_Grouping(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	t.Run("multiple names grouped correctly", func(t *testing.T) {
		t.Parallel()

		input := []string{
			"/objects/alice",
			"/hashes/sha256/objects/alice",
			"/sig/ed25519/objects/alice",
			"/objects/bob",
			"/hashes/sha256/objects/bob",
			"/sig/ed25519/objects/bob",
		}

		results, err := layeredN.ParseKeys(input, false)
		require.NoError(t, err)
		assert.Equal(t, 2, results.Len())

		aliceKeys, ok := results.Select("alice")
		require.True(t, ok)
		assert.Len(t, aliceKeys, 3)

		bobKeys, ok := results.Select("bob")
		require.True(t, ok)
		assert.Len(t, bobKeys, 3)
	})

	t.Run("single name produces isSingle=true", func(t *testing.T) {
		t.Parallel()

		input := []string{
			"/objects/alice",
			"/hashes/sha256/objects/alice",
			"/sig/ed25519/objects/alice",
		}

		results, err := layeredN.ParseKeys(input, false)
		require.NoError(t, err)

		keys, ok := results.SelectSingle()
		require.True(t, ok)
		assert.Len(t, keys, 3)
	})

	t.Run("ignoreError=true skips invalid keys", func(t *testing.T) {
		t.Parallel()

		input := []string{
			"/unknown/alice",
			"/objects/alice",
		}

		results, err := layeredN.ParseKeys(input, true)
		require.NoError(t, err)
		assert.Equal(t, 1, results.Len())

		_, ok := results.Select("alice")
		assert.True(t, ok)
	})

	t.Run("ignoreError=false returns first error", func(t *testing.T) {
		t.Parallel()

		input := []string{
			"/unknown/alice",
			"/objects/alice",
		}

		_, err := layeredN.ParseKeys(input, false)
		require.Error(t, err)
	})
}

func TestLayeredNamer_Prefix(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	tests := []struct {
		name     string
		val      string
		isPrefix bool
		expected string
	}{
		{
			name:     "empty val, isPrefix=false",
			val:      "",
			isPrefix: false,
			expected: "/objects/",
		},
		{
			name:     "empty val, isPrefix=true",
			val:      "",
			isPrefix: true,
			expected: "/objects/",
		},
		{
			name:     "simple val, isPrefix=false",
			val:      "alice",
			isPrefix: false,
			expected: "/objects/alice",
		},
		{
			name:     "simple val, isPrefix=true",
			val:      "users",
			isPrefix: true,
			expected: "/objects/users/",
		},
		{
			name:     "val with leading and trailing slash trimmed",
			val:      "/alice/",
			isPrefix: false,
			expected: "/objects/alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := layeredN.Prefix(tt.val, tt.isPrefix)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestLayeredNamer_Prefixes(t *testing.T) {
	t.Parallel()

	layeredN := newTestLayeredNamer(t)

	tests := []struct {
		name     string
		val      string
		isPrefix bool
		expected []string
	}{
		{
			name:     "empty val emits one prefix per category root",
			val:      "",
			isPrefix: true,
			expected: []string{
				"/objects/",
				"/hashes/sha256/objects/",
				"/sig/ed25519/objects/",
			},
		},
		{
			name:     "non-empty val with isPrefix true",
			val:      "alice",
			isPrefix: true,
			expected: []string{
				"/objects/alice/",
				"/hashes/sha256/objects/alice/",
				"/sig/ed25519/objects/alice/",
			},
		},
		{
			name:     "non-empty val with isPrefix false",
			val:      "alice",
			isPrefix: false,
			expected: []string{
				"/objects/alice",
				"/hashes/sha256/objects/alice",
				"/sig/ed25519/objects/alice",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, layeredN.Prefixes(tt.val, tt.isPrefix))
		})
	}
}

func TestLayeredNamer_Prefixes_Compact(t *testing.T) {
	t.Parallel()

	layeredN, err := namer.NewLayeredNamer(
		"objects",
		[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
		[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		namer.CompactSingleHash(),
		namer.CompactSingleSig(),
	)
	require.NoError(t, err)

	expected := []string{
		"/objects/",
		"/hashes/objects/",
		"/sig/objects/",
	}
	assert.Equal(t, expected, layeredN.Prefixes("", true))
}

// TestLayeredNamer_MultiSegmentObjectLocation covers the round-trip of value,
// hash, and sig keys when objectLocation is itself a multi-segment path
// (e.g. "settings/ldap"). The full and compact layouts are both exercised.
func TestLayeredNamer_MultiSegmentObjectLocation(t *testing.T) {
	t.Parallel()

	const objLoc = "settings/ldap"

	t.Run("value key generate and parse", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(objLoc, nil, nil)
		require.NoError(t, err)

		keys, err := layeredN.GenerateNames("entry-1")
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.Equal(t, "/settings/ldap/entry-1", keys[0].Build())

		parsed, err := layeredN.ParseKey("/settings/ldap/entry-1")
		require.NoError(t, err)
		assert.Equal(t, namer.KeyTypeValue, parsed.Type())
		assert.Equal(t, "entry-1", parsed.Name())
	})

	t.Run("full hash and sig", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(
			objLoc,
			[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
			[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
		)
		require.NoError(t, err)

		keys, err := layeredN.GenerateNames("entry-1")
		require.NoError(t, err)
		require.Len(t, keys, 3)
		assert.Equal(t, "/settings/ldap/entry-1", keys[0].Build())
		assert.Equal(t, "/hashes/sha256/settings/ldap/entry-1", keys[1].Build())
		assert.Equal(t, "/sig/ed25519/settings/ldap/entry-1", keys[2].Build())

		// Round-trip each key.
		for _, k := range keys {
			parsed, err := layeredN.ParseKey(k.Build())
			require.NoError(t, err, "ParseKey(%q)", k.Build())
			assert.Equal(t, k.Type(), parsed.Type())
			assert.Equal(t, k.Name(), parsed.Name())
			assert.Equal(t, k.Property(), parsed.Property())
		}
	})

	t.Run("compact hash and sig", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(
			objLoc,
			[]namer.LayeredHashLocation{{HasherName: "sha256", Location: "sha256"}},
			[]namer.LayeredSigLocation{{SignerName: "ed25519", Location: "ed25519"}},
			namer.CompactSingleHash(),
			namer.CompactSingleSig(),
		)
		require.NoError(t, err)

		keys, err := layeredN.GenerateNames("entry-1")
		require.NoError(t, err)
		require.Len(t, keys, 3)
		assert.Equal(t, "/settings/ldap/entry-1", keys[0].Build())
		assert.Equal(t, "/hashes/settings/ldap/entry-1", keys[1].Build())
		assert.Equal(t, "/sig/settings/ldap/entry-1", keys[2].Build())

		for _, k := range keys {
			parsed, err := layeredN.ParseKey(k.Build())
			require.NoError(t, err, "ParseKey(%q)", k.Build())
			assert.Equal(t, k.Type(), parsed.Type())
			assert.Equal(t, k.Name(), parsed.Name())
		}
	})

	t.Run("parse rejects mismatching prefix", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(objLoc, nil, nil)
		require.NoError(t, err)

		// Right first segment, wrong second segment — must not be misread as
		// a value at "/settings/ldap/...".
		_, err = layeredN.ParseKey("/settings/oauth/entry-1")
		require.Error(t, err)

		// Truncated — first segment matches, but the full prefix does not.
		_, err = layeredN.ParseKey("/settings")
		require.Error(t, err)
	})

	t.Run("Prefix produces correct walk root", func(t *testing.T) {
		t.Parallel()

		layeredN, err := namer.NewLayeredNamer(objLoc, nil, nil)
		require.NoError(t, err)

		assert.Equal(t, "/settings/ldap/", layeredN.Prefix("", false))
		assert.Equal(t, "/settings/ldap/alice", layeredN.Prefix("alice", false))
		assert.Equal(t, "/settings/ldap/users/", layeredN.Prefix("users", true))
	})
}
