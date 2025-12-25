package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/namer"
)

const (
	storagePrefix = "/storage"
)

func TestDefaultNamer_GenerateNames_Invalid_Empty(t *testing.T) {
	t.Parallel()

	dn := namer.NewDefaultNamer("/storage", nil, nil)
	_, err := dn.GenerateNames("")
	assert.Error(t, err)
}

func TestDefaultNamer_GenerateNames_Invalid_Prefix(t *testing.T) {
	t.Parallel()

	dn := namer.NewDefaultNamer("/storage", nil, nil)
	_, err := dn.GenerateNames("123/")
	assert.Error(t, err)
}

func TestDefaultNamer_GenerateNames_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefix    string
		name      string
		hashNames []string
		sigNames  []string
		expected  []namer.Key
	}{
		{
			"/storage",
			"all",
			[]string{"sha256"},
			[]string{"RSAPSS"},
			[]namer.Key{
				namer.NewDefaultKey(
					"all",
					namer.KeyTypeValue,
					"",
					"/storage/all",
				),
				namer.NewDefaultKey(
					"all",
					namer.KeyTypeHash,
					"sha256",
					"/storage/hash/sha256/all",
				),
				namer.NewDefaultKey(
					"all",
					namer.KeyTypeSignature,
					"RSAPSS",
					"/storage/sig/RSAPSS/all",
				),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(tt.prefix, tt.hashNames, tt.sigNames)

			result, err := dn.GenerateNames(tt.name)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDefaultNamer_ParseKey_Success(t *testing.T) {
	t.Parallel()

	prefix := storagePrefix

	tests := []struct {
		input    string
		name     string
		tp       namer.KeyType
		property string
	}{
		{
			input:    "/storage/all",
			name:     "all",
			tp:       namer.KeyTypeValue,
			property: "",
		},
		{
			input:    "/storage/hash/sha256/all",
			name:     "all",
			tp:       namer.KeyTypeHash,
			property: "sha256",
		},
		{
			input:    "/storage/sig/rsa/all",
			name:     "all",
			tp:       namer.KeyTypeSignature,
			property: "rsa",
		},
	}

	for _, tt := range tests {
		t.Run("case:"+tt.input, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(prefix, nil, nil)

			key, err := dn.ParseKey(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.name, key.Name())
			assert.Equal(t, tt.tp, key.Type())
			assert.Equal(t, tt.property, key.Property())
			assert.Equal(t, tt.input, key.Build())
		})
	}
}

func TestDefaultNamer_ParseKey_Failed(t *testing.T) {
	t.Parallel()

	prefix := storagePrefix

	tests := []string{
		"/not-storage/value",
		"/storage/hash",
		"/storage/hash/sha256",
		"/storage/hash//all",
		"/storage/hash/sha256/",
		"/storage/hash/sha256/suffix/",
		"/storage/sig",
		"/storage/sig/rsa",
		"/storage/sig//all",
		"/storage/sig/rsa/",
		"/storage/sig/rsa/suffix/",
		"/storage/value-suffix/",
	}

	for _, tt := range tests {
		t.Run("case:"+tt, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(prefix, nil, nil)
			_, err := dn.ParseKey(tt)
			require.Error(t, err)
		})
	}
}

func TestDefaultNamer_ParseKeys_Success(t *testing.T) {
	t.Parallel()

	prefix := storagePrefix

	tests := []struct {
		name       string
		input      []string
		countMap   map[string]int
		singleName string
	}{
		{
			name: "non empty + single",
			input: []string{
				"/storage/value",
				"/storage/sig/signame/value",
				"/storage/hash/hashname/value",
			},
			countMap:   map[string]int{"value": 3},
			singleName: "value",
		},
		{
			name: "non empty + multiple",
			input: []string{
				"/storage/key1",
				"/storage/sig/rsa/key1",
				"/storage/hash/sha256/key1",
				"/storage/key2",
				"/storage/sig/rsa/key2",
				"/storage/hash/sha256/key2",
			},
			countMap:   map[string]int{"key1": 3, "key2": 3},
			singleName: "",
		},
		{
			name:       "empty",
			input:      []string{},
			countMap:   map[string]int{},
			singleName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(prefix, nil, nil)

			result, err := dn.ParseKeys(tt.input, false)
			require.NoError(t, err)
			require.Len(t, result.Result(), len(tt.countMap))

			for k, v := range tt.countMap {
				require.Contains(t, result.Result(), k)
				assert.Len(t, result.Result()[k], v)
			}

			if tt.singleName != "" {
				_, ok := result.SelectSingle()
				assert.True(t, ok)
			} else {
				_, ok := result.SelectSingle()
				assert.False(t, ok)
			}
		})
	}
}

func TestDefaultNamer_ParseKeys_Fail(t *testing.T) {
	t.Parallel()

	prefix := storagePrefix

	t.Run("ignoreError = false", func(t *testing.T) {
		t.Parallel()

		dn := namer.NewDefaultNamer(prefix, nil, nil)
		_, err := dn.ParseKeys([]string{"/non-storage/value"}, false)
		require.Error(t, err)
	})

	t.Run("ignoreError = true, skip all", func(t *testing.T) {
		t.Parallel()

		dn := namer.NewDefaultNamer(prefix, nil, nil)
		results, err := dn.ParseKeys([]string{"/non-storage/value"}, true)
		require.NoError(t, err)
		assert.Empty(t, results.Result())
	})

	t.Run("ignoreError = true", func(t *testing.T) {
		t.Parallel()

		dn := namer.NewDefaultNamer(prefix, nil, nil)
		results, err := dn.ParseKeys([]string{
			"/non-storage/value",
			"/storage/value-1",
		}, true)
		require.NoError(t, err)
		assert.Len(t, results.Result(), 1)
		assert.Contains(t, results.Result(), "value-1")
	})
}

func TestDefaultNamer_Prefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefix   string
		val      string
		isPrefix bool
		expected string
	}{
		{
			name:     "value key, not prefix",
			prefix:   "/storage",
			val:      "my-object",
			isPrefix: false,
			expected: "/storage/my-object",
		},
		{
			name:     "value key with trailing slash, not prefix",
			prefix:   "/storage",
			val:      "my-object/",
			isPrefix: false,
			expected: "/storage/my-object",
		},
		{
			name:     "empty val, prefix true",
			prefix:   "/storage",
			val:      "",
			isPrefix: true,
			expected: "/storage/",
		},
		{
			name:     "empty val, prefix false",
			prefix:   "/storage",
			val:      "",
			isPrefix: false,
			expected: "/storage/",
		},
		{
			name:     "with subpath, prefix true",
			prefix:   "/storage",
			val:      "folder/object",
			isPrefix: true,
			expected: "/storage/folder/object/",
		},
		{
			name:     "with subpath, prefix false",
			prefix:   "/storage",
			val:      "folder/object",
			isPrefix: false,
			expected: "/storage/folder/object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dn := namer.NewDefaultNamer(tt.prefix, nil, nil)
			result := dn.Prefix(tt.val, tt.isPrefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}
