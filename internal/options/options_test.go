package options_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-storage/internal/options"
)

func zero[T any]() (out T) { return } //nolint:nonamedreturns

func TestApplyOptions(t *testing.T) {
	t.Parallel()

	type config struct {
		value int
		name  string
		flag  bool
	}

	tests := []struct {
		name        string
		constructor options.OptionConstructor[config]
		callbacks   []options.OptionCallback[config]
		expected    config
	}{
		{
			name:        "nil constructor and no callbacks",
			constructor: nil,
			callbacks:   nil,
			expected:    zero[config](),
		},
		{
			name:        "nil constructor and empty callbacks",
			constructor: nil,
			callbacks:   []options.OptionCallback[config]{},
			expected:    zero[config](),
		},
		{
			name: "constructor returns default, no callbacks",
			constructor: func() config {
				return config{value: 42, name: "default", flag: false}
			},
			callbacks: nil,
			expected:  config{value: 42, name: "default", flag: false},
		},
		{
			name:        "nil constructor, single callback",
			constructor: nil,
			callbacks: []options.OptionCallback[config]{
				func(c *config) { c.value = 100 },
			},
			expected: config{value: 100, name: "", flag: false},
		},
		{
			name: "constructor with one callback",
			constructor: func() config {
				return config{value: 1, name: "initial", flag: false}
			},
			callbacks: []options.OptionCallback[config]{
				func(c *config) { c.value = 999 },
			},
			expected: config{value: 999, name: "initial", flag: false},
		},
		{
			name: "multiple callbacks applied in order",
			constructor: func() config {
				return zero[config]()
			},
			callbacks: []options.OptionCallback[config]{
				func(c *config) { c.value += 5 },
				func(c *config) { c.name = "after" },
				func(c *config) { c.flag = true },
			},
			expected: config{value: 5, name: "after", flag: true},
		},
		{
			name:        "callbacks modify multiple fields",
			constructor: nil,
			callbacks: []options.OptionCallback[config]{
				func(c *config) {
					c.value = 10
					c.name = "ten"
					c.flag = true
				},
			},
			expected: config{value: 10, name: "ten", flag: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := options.ApplyOptions(tt.constructor, tt.callbacks)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyOptions_Int(t *testing.T) {
	t.Parallel()

	constructor := func() int { return 7 }
	callbacks := []options.OptionCallback[int]{
		func(i *int) { *i += 3 },
		func(i *int) { *i *= 2 },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, 20, result) // (7 + 3) * 2 = 20
}

func TestApplyOptions_Pointer(t *testing.T) {
	t.Parallel()

	type data struct{ x int }

	constructor := func() *data { return &data{x: 1} }
	callbacks := []options.OptionCallback[*data]{
		func(d **data) { (*d).x = 2 },
		func(d **data) { *d = &data{x: 3} },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, &data{x: 3}, result)
}

func TestApplyOptions_Slice(t *testing.T) {
	t.Parallel()

	constructor := func() []int { return []int{1, 2} }
	callbacks := []options.OptionCallback[[]int]{
		func(s *[]int) { *s = append(*s, 3) },
		func(s *[]int) { (*s)[0] = 99 },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, []int{99, 2, 3}, result)
}

func TestApplyOptions_Map(t *testing.T) {
	t.Parallel()

	constructor := func() map[string]int { return map[string]int{"a": 1} }
	callbacks := []options.OptionCallback[map[string]int]{
		func(m *map[string]int) { (*m)["b"] = 2 },
		func(m *map[string]int) { delete(*m, "a") },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, map[string]int{"b": 2}, result)
}

func TestApplyOptions_ZeroSizedType(t *testing.T) {
	t.Parallel()

	type empty struct{}

	constructor := func() empty { return empty{} }
	callbacks := []options.OptionCallback[empty]{
		func(e *empty) {},
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, empty{}, result)
}

func TestApplyOptions_NilConstructorPointer(t *testing.T) {
	t.Parallel()

	type data struct{ x int }

	var constructor options.OptionConstructor[*data]

	callbacks := []options.OptionCallback[*data]{
		func(d **data) { *d = &data{x: 42} },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, &data{x: 42}, result)
}

func TestApplyOptions_ChainCallbacks(t *testing.T) {
	t.Parallel()

	type counter struct{ val int }

	constructor := func() counter { return counter{val: 0} }
	callbacks := []options.OptionCallback[counter]{
		func(c *counter) { c.val++ },
		func(c *counter) { c.val *= 5 },
		func(c *counter) { c.val += 2 },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, 7, result.val) // (0+1)*5+2 = 7
}

func TestApplyOptions_NoCallbacks(t *testing.T) {
	t.Parallel()

	type simple struct{ v int }

	constructor := func() simple { return simple{v: 77} }
	result := options.ApplyOptions(constructor, nil)
	assert.Equal(t, simple{v: 77}, result)
}

func TestApplyOptions_EmptyCallbacks(t *testing.T) {
	t.Parallel()

	type simple struct{ v int }

	constructor := func() simple { return simple{v: 88} }

	var callbacks []options.OptionCallback[simple]

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, simple{v: 88}, result)
}

func TestApplyOptions_NilPointerConstructor(t *testing.T) {
	t.Parallel()

	type data struct{ x int }

	var constructor options.OptionConstructor[*data]

	callbacks := []options.OptionCallback[*data]{
		func(d **data) { *d = &data{x: 42} },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, &data{x: 42}, result)
}

func TestApplyOptions_ConstructorReturnsNil(t *testing.T) {
	t.Parallel()

	type data struct{ x int }

	constructor := func() *data { return nil }
	callbacks := []options.OptionCallback[*data]{
		func(d **data) { *d = &data{x: 100} },
	}

	result := options.ApplyOptions(constructor, callbacks)
	assert.Equal(t, &data{x: 100}, result)
}
