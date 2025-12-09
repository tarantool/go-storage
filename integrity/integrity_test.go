package integrity_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

type TestStruct struct {
	Name  string `yaml:"name"`
	Value int    `yaml:"value"`
}

func TestGeneratorAndValidator(t *testing.T) {
	t.Parallel()

	namer := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshaller := marshaller.NewTypedYamlMarshaller[TestStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	gen := integrity.NewGenerator[TestStruct](
		namer,
		marshaller,
		hashers,
		nil, // no signers for this test.
	)

	val := integrity.NewValidator[TestStruct](
		namer,
		marshaller,
		hashers,
		nil, // no verifiers for this test.
	)

	testData := TestStruct{
		Name:  "test",
		Value: 42,
	}

	kvs, err := gen.Generate("myobject", testData)
	require.NoError(t, err)
	assert.NotEmpty(t, kvs)

	validated, err := val.Validate(kvs)
	require.NoError(t, err)
	assert.Equal(t, testData, validated)
}

func TestValidatorWithMissingHash(t *testing.T) {
	t.Parallel()

	namer := namer.NewDefaultNamer("test", []string{"sha256"}, []string{})
	marshaller := marshaller.NewTypedYamlMarshaller[TestStruct]()
	hashers := []hasher.Hasher{hasher.NewSHA256Hasher()}

	gen := integrity.NewGenerator[TestStruct](
		namer,
		marshaller,
		hashers,
		nil,
	)

	val := integrity.NewValidator[TestStruct](
		namer,
		marshaller,
		hashers,
		nil,
	)

	testData := TestStruct{
		Name:  "test",
		Value: 42,
	}

	kvs, err := gen.Generate("myobject", testData)
	require.NoError(t, err)

	// Remove hash key to simulate missing hash.
	var filteredKvs []kv.KeyValue
	for _, kv := range kvs {
		keyStr := string(kv.Key)
		if !contains(keyStr, "hash") {
			filteredKvs = append(filteredKvs, kv)
		}
	}

	_, err = val.Validate(filteredKvs)
	assert.Error(t, err)
	// Should fail because hash key is in parsed results but not in input.
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
