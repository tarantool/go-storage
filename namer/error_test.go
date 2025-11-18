package namer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-storage/namer"
)

func TestInvalidNameError_Error(t *testing.T) {
	t.Parallel()

	err := namer.InvalidNameError{Name: "name", Problem: "problem"}
	assert.Equal(t, "invalid name 'name': problem", err.Error())
}

func TestInvalidKeyError_Error(t *testing.T) {
	t.Parallel()

	err := namer.InvalidKeyError{Key: "name", Problem: "problem"}
	assert.Equal(t, "invalid key 'name': problem", err.Error())
}
