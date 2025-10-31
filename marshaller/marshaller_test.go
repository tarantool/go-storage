package marshaller_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-storage/marshaller"
)

type YamlStruct struct {
	Title string `yaml:"title"`
	Link  string `yaml:"link"`
}

func TestMarshal(t *testing.T) {
	t.Parallel()

	marshaller := marshaller.NewYamlMarshaller()

	data, err := marshaller.Marshal([]byte{123, 123})
	require.NoError(t, err)
	require.NotNil(t, data)
}

func TestUnmarshal(t *testing.T) {
	t.Parallel()

	marshaller := marshaller.NewYamlMarshaller()

	var unmarshaledYaml YamlStruct

	validYaml := `
title: "Link"
link: "https://some.link"
`

	err := marshaller.Unmarshal([]byte(validYaml), &unmarshaledYaml)
	require.NoError(t, err)

	invalidYaml := `
TITLE: 123
     Link: true
`

	err = marshaller.Unmarshal([]byte(invalidYaml), &unmarshaledYaml)
	require.Error(t, err)
}
