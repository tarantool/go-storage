package marshaller_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/marshaller"
)

type TestStruct struct {
	Name  string   `json:"name"           yaml:"name"`
	Value int      `json:"value"          yaml:"value"`
	Tags  []string `json:"tags,omitempty" yaml:"tags,omitempty"`
}

type NestedStruct struct {
	ID     int        `json:"id"     yaml:"id"`
	Data   TestStruct `json:"data"   yaml:"data"`
	Active bool       `json:"active" yaml:"active"`
}

func TestYamlMarshaller_New(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[TestStruct]()
	require.NotNil(t, marsh)
}

func TestYamlMarshaller_Marshal_Success(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[TestStruct]()

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

func TestYamlMarshaller_Marshal_EmptyStruct(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[TestStruct]()

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

func TestYamlMarshaller_Marshal_NestedStruct(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[NestedStruct]()

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

func TestYamlMarshaller_Unmarshal_Success(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[TestStruct]()

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

func TestYamlMarshaller_Unmarshal_EmptyYaml(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[TestStruct]()

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

func TestYamlMarshaller_Unmarshal_NestedStruct(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[NestedStruct]()

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

func TestYamlMarshaller_Unmarshal_InvalidYaml(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[TestStruct]()

	invalidYaml := []byte(`name: test
value: not_a_number
`)

	_, err := marsh.Unmarshal(invalidYaml)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal")
}

func TestYamlMarshaller_RoundTrip(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[TestStruct]()

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

func TestYamlMarshaller_WithPrimitiveType(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[int]()

	yamlData := []byte(`42`)

	result, err := marsh.Unmarshal(yamlData)
	require.NoError(t, err)
	require.Equal(t, 42, result)

	marshaled, err := marsh.Marshal(100)
	require.NoError(t, err)
	require.Equal(t, "100\n", string(marshaled))
}

func TestYamlMarshaller_WithSliceType(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewYamlMarshaller[[]string]()

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

func TestJSONMarshaller_RoundTrip(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewJSONMarshaller[TestStruct]()

	original := TestStruct{
		Name:  "roundtrip",
		Value: 99,
		Tags:  []string{"a", "b", "c"},
	}

	marshaled, err := marsh.Marshal(original)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"roundtrip","value":99,"tags":["a","b","c"]}`, string(marshaled))

	unmarshaled, err := marsh.Unmarshal(marshaled)
	require.NoError(t, err)
	require.Equal(t, original, unmarshaled)
}

func TestJSONMarshaller_NestedStruct(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewJSONMarshaller[NestedStruct]()

	original := NestedStruct{
		ID: 1,
		Data: TestStruct{
			Name:  "nested",
			Value: 100,
			Tags:  nil,
		},
		Active: true,
	}

	marshaled, err := marsh.Marshal(original)
	require.NoError(t, err)

	unmarshaled, err := marsh.Unmarshal(marshaled)
	require.NoError(t, err)
	require.Equal(t, original, unmarshaled)
}

func TestJSONMarshaller_Unmarshal_InvalidJSON(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewJSONMarshaller[TestStruct]()

	_, err := marsh.Unmarshal([]byte(`{not json}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal")
}

func TestBytesMarshaller_Passthrough(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewBytesMarshaller()

	original := []byte{0x00, 0x01, 0x02, 0xff}

	marshaled, err := marsh.Marshal(original)
	require.NoError(t, err)
	require.Equal(t, original, marshaled)

	unmarshaled, err := marsh.Unmarshal(marshaled)
	require.NoError(t, err)
	require.Equal(t, original, unmarshaled)
}

func TestBytesMarshaller_Nil(t *testing.T) {
	t.Parallel()

	marsh := marshaller.NewBytesMarshaller()

	marshaled, err := marsh.Marshal(nil)
	require.NoError(t, err)
	require.Nil(t, marshaled)

	unmarshaled, err := marsh.Unmarshal(nil)
	require.NoError(t, err)
	require.Nil(t, unmarshaled)
}

func TestBytesMarshaller_ImplementsMarshaller(t *testing.T) {
	t.Parallel()

	var _ marshaller.Marshaller[[]byte] = marshaller.NewBytesMarshaller()
}
