package integrity_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/v2/namer"
)

// mustNamer builds a namer via namer.New, mapping each hasher/signer name to a
// same-named location segment, failing the test on construction error. The
// (objectLocation, hashNames, sigNames) shape keeps tests of the namer-agnostic
// Generator/Validator concise.
func mustNamer(t *testing.T, objectLocation string, hashNames, sigNames []string) namer.Namer {
	t.Helper()

	hashLocations := make([]namer.HashLocation, 0, len(hashNames))
	for _, name := range hashNames {
		hashLocations = append(hashLocations, namer.HashLocation{HasherName: name, Location: name})
	}

	sigLocations := make([]namer.SigLocation, 0, len(sigNames))
	for _, name := range sigNames {
		sigLocations = append(sigLocations, namer.SigLocation{SignerName: name, Location: name})
	}

	nm, err := namer.New(objectLocation, hashLocations, sigLocations)
	require.NoError(t, err)

	return nm
}

type mockNamer struct {
	generateNamesErr error
	prefixVal        string
}

func (m *mockNamer) GenerateNames(name string) ([]namer.Key, error) {
	if m.generateNamesErr != nil {
		return nil, m.generateNamesErr
	}

	return []namer.Key{
		namer.NewKey(name, namer.KeyTypeValue, "", "/test/"+name),
	}, nil
}

func (m *mockNamer) ParseKey(name string) (namer.Key, error) {
	return namer.Key{}, errors.New("not implemented")
}

func (m *mockNamer) ParseKeys(names []string, ignoreError bool) (namer.Results, error) {
	return namer.Results{}, errors.New("not implemented")
}

func (m *mockNamer) Prefix(val string, isPrefix bool) string {
	switch {
	case m.prefixVal != "":
		return m.prefixVal
	case isPrefix:
		return "/test/" + val + "/"
	default:
		return "/test/" + val
	}
}

func (m *mockNamer) Prefixes(val string, isPrefix bool) []string {
	return []string{m.Prefix(val, isPrefix)}
}

type mockSignerVerifier struct {
	name      string
	signErr   error
	verifyErr error
}

func (m *mockSignerVerifier) Name() string { return m.name }

func (m *mockSignerVerifier) Sign(data []byte) ([]byte, error) {
	if m.signErr != nil {
		return nil, m.signErr
	}

	return []byte("mock-signature-" + m.name), nil
}

func (m *mockSignerVerifier) Verify(data []byte, signature []byte) error {
	return m.verifyErr
}
