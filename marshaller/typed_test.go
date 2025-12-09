package marshaller_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/marshaller"
)

type TestStruct struct {
	Name  string   `yaml:"name"`
	Value int      `yaml:"value"`
	Tags  []string `yaml:"tags,omitempty"`
}

type NestedStruct struct {
	ID     int        `yaml:"id"`
	Data   TestStruct `yaml:"data"`
	Active bool       `yaml:"active"`
}

func TestTypedYamlMarshaller_New(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[TestStruct]()
	require.NotNil(t, marsh)
}

func TestTypedYamlMarshaller_Marshal_Success(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[TestStruct]()

	data := TestStruct{
		Name:  "test",
		Value: 42,
		Tags:  []string{"tag1", "tag2"},
	}

	result, err := marsh.Marshal(data)
	require.NoError(t, err)
	require.NotEmpty(t, result)

	expectedYaml := `name: test
value: 42
tags:
    - tag1
    - tag2
`
	require.YAMLEq(t, expectedYaml, string(result))
}

func TestTypedYamlMarshaller_Marshal_EmptyStruct(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[TestStruct]()

	data := TestStruct{
		Name:  "",
		Value: 0,
		Tags:  nil,
	}

	result, err := marsh.Marshal(data)
	require.NoError(t, err)
	require.NotEmpty(t, result)

	expectedYaml := `name: ""
value: 0
`
	require.YAMLEq(t, expectedYaml, string(result))
}

func TestTypedYamlMarshaller_Marshal_NestedStruct(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[NestedStruct]()

	data := NestedStruct{
		ID: 1,
		Data: TestStruct{
			Name:  "nested",
			Value: 100,
			Tags:  nil,
		},
		Active: true,
	}

	result, err := marsh.Marshal(data)
	require.NoError(t, err)
	require.NotEmpty(t, result)

	expectedYaml := `id: 1
data:
    name: nested
    value: 100
active: true
`
	require.YAMLEq(t, expectedYaml, string(result))
}

func TestTypedYamlMarshaller_Unmarshal_Success(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[TestStruct]()

	yamlData := []byte(`name: test
value: 42
tags:
    - tag1
    - tag2
`)

	result, err := marsh.Unmarshal(yamlData)
	require.NoError(t, err)

	expected := TestStruct{
		Name:  "test",
		Value: 42,
		Tags:  []string{"tag1", "tag2"},
	}
	require.Equal(t, expected, result)
}

func TestTypedYamlMarshaller_Unmarshal_EmptyYaml(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[TestStruct]()

	yamlData := []byte(``)

	result, err := marsh.Unmarshal(yamlData)
	require.NoError(t, err)

	expected := TestStruct{
		Name:  "",
		Value: 0,
		Tags:  nil,
	}
	require.Equal(t, expected, result)
}

func TestTypedYamlMarshaller_Unmarshal_NestedStruct(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[NestedStruct]()

	yamlData := []byte(`id: 1
data:
    name: nested
    value: 100
active: true
`)

	result, err := marsh.Unmarshal(yamlData)
	require.NoError(t, err)

	expected := NestedStruct{
		ID: 1,
		Data: TestStruct{
			Name:  "nested",
			Value: 100,
			Tags:  nil,
		},
		Active: true,
	}
	require.Equal(t, expected, result)
}

func TestTypedYamlMarshaller_Unmarshal_InvalidYaml(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[TestStruct]()

	invalidYaml := []byte(`name: test
value: not_a_number
`)

	_, err := marsh.Unmarshal(invalidYaml)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Failed to unmarshal")
}

func TestTypedYamlMarshaller_RoundTrip(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[TestStruct]()

	original := TestStruct{
		Name:  "roundtrip",
		Value: 99,
		Tags:  []string{"a", "b", "c"},
	}

	marshaled, err := marsh.Marshal(original)
	require.NoError(t, err)
	require.NotEmpty(t, marshaled)

	unmarshaled, err := marsh.Unmarshal(marshaled)
	require.NoError(t, err)

	require.Equal(t, original, unmarshaled)
}

func TestTypedYamlMarshaller_WithPrimitiveType(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[int]()

	yamlData := []byte(`42`)

	result, err := marsh.Unmarshal(yamlData)
	require.NoError(t, err)
	require.Equal(t, 42, result)

	marshaled, err := marsh.Marshal(100)
	require.NoError(t, err)
	require.Equal(t, "100\n", string(marshaled))
}

func TestTypedYamlMarshaller_WithSliceType(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewTypedYamlMarshaller[[]string]()

	yamlData := []byte(`- item1
- item2
- item3
`)

	result, err := marsh.Unmarshal(yamlData)
	require.NoError(t, err)
	require.Equal(t, []string{"item1", "item2", "item3"}, result)

	original := []string{"a", "b", "c"}
	marshaled, err := marsh.Marshal(original)
	require.NoError(t, err)

	unmarshaled, err := marsh.Unmarshal(marshaled)
	require.NoError(t, err)
	require.Equal(t, original, unmarshaled)
}
