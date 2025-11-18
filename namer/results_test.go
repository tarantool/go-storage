package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/namer"
)

func TestNewResults(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, namer.NewResults(map[string][]namer.Key{}))
}

func TestResults_SelectSingle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		results        map[string][]namer.Key
		isSingleResult bool
	}{
		{
			name:           "single",
			results:        map[string][]namer.Key{"key": {}},
			isSingleResult: true,
		},
		{
			name:           "multiple",
			results:        map[string][]namer.Key{"key1": {}, "key2": {}},
			isSingleResult: false,
		},
		{
			name:           "empty",
			results:        map[string][]namer.Key{},
			isSingleResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := namer.NewResults(tt.results)
			_, ok := r.SelectSingle()
			require.Equal(t, ok, tt.isSingleResult)
		})
	}
}

func TestResults_Len(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results map[string][]namer.Key
		len     int
	}{
		{
			name:    "single",
			results: map[string][]namer.Key{"key": {}},
			len:     1,
		},
		{
			name:    "multiple",
			results: map[string][]namer.Key{"key1": {}, "key2": {}},
			len:     2,
		},
		{
			name:    "empty",
			results: map[string][]namer.Key{},
			len:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := namer.NewResults(tt.results)
			resultsLen := r.Len()
			assert.Equal(t, resultsLen, tt.len)
		})
	}
}

func TestResults_Items(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results map[string][]namer.Key
		oneOf   []string
	}{
		{
			name:    "single",
			results: map[string][]namer.Key{"key": {}},
			oneOf:   []string{"key"},
		},
		{
			name:    "multiple",
			results: map[string][]namer.Key{"key1": {}, "key2": {}},
			oneOf:   []string{"key1", "key2"},
		},
		{
			name:    "empty",
			results: map[string][]namer.Key{},
			oneOf:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cnt := 0

			results := namer.NewResults(tt.results)
			for name := range results.Items() {
				cnt++

				assert.Contains(t, tt.oneOf, name)
			}

			assert.Equal(t, len(tt.oneOf), cnt)
		})
	}
}

func TestResults_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results map[string][]namer.Key
		oneOf   []string
	}{
		{
			name:    "single",
			results: map[string][]namer.Key{"key": {}},
			oneOf:   []string{"key"},
		},
		{
			name:    "multiple",
			results: map[string][]namer.Key{"key1": {}, "key2": {}},
			oneOf:   []string{"key1", "key2"},
		},
		{
			name:    "empty",
			results: map[string][]namer.Key{},
			oneOf:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			results := namer.NewResults(tt.results)
			for _, name := range tt.oneOf {
				_, ok := results.Select(name)
				assert.True(t, ok)
			}

			_, ok := results.Select("not-presented")
			assert.False(t, ok)
		})
	}
}
