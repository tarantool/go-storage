//go:build integration

//nolint:paralleltest
package integrity_test

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/driver"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/integrity"
)

const (
	writers            = 5
	readers            = 5
	iterNumsConcurrent = 1000
)

const megabyte = 1024 * 1024
const iterNumsManyRequests = 1000

func generateRandomString(t *testing.T, length int) string {
	t.Helper()

	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	byteString := make([]byte, length)

	for i := range length {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		require.NoError(t, err)

		byteString[i] = charset[n.Int64()]
	}

	return string(byteString)
}

func TestTypedIntegration_ConcurrentAccess(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		key := "shared-key"

		var waitGr sync.WaitGroup

		for range writers {
			waitGr.Add(1)

			go func() {
				defer waitGr.Done()

				for i := range iterNumsConcurrent {
					value := IntegrationStruct{
						Name:  fmt.Sprintf("object-%d", i),
						Value: i,
					}
					err := typed.Put(ctx, key, value)
					assert.NoError(t, err)
				}
			}()
		}

		for range readers {
			waitGr.Add(1)

			go func() {
				defer waitGr.Done()

				for range iterNumsConcurrent {
					_, err := typed.Get(ctx, key)
					if err != nil {
						assert.ErrorIs(t, err, integrity.ErrNotFound)
					}
				}
			}()
		}

		waitGr.Wait()

		// Cleanup.
		cleanupTyped(t, typed, key)
	})
}

func TestTypedIntegration_ManyRequests(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()

		storageInstance := storage.NewStorage(driverInstance)

		cleanupNames := make([]string, 0, iterNumsManyRequests)

		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*rsaPK2048)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		for iter := range iterNumsManyRequests {
			name := fmt.Sprintf("obj_%d", iter)
			value := IntegrationStruct{Name: fmt.Sprintf("object_%d", iter), Value: iter}
			err := typed.Put(ctx, name, value)
			require.NoError(t, err)

			cleanupNames = append(cleanupNames, name)
		}

		for iter := range iterNumsManyRequests {
			value := IntegrationStruct{Name: fmt.Sprintf("object_%d", iter), Value: iter}
			result, err := typed.Get(ctx, fmt.Sprintf("obj_%d", iter))
			require.NoError(t, err)

			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("obj_%d", iter), result.Name)
			assert.True(t, result.Value.IsSome())

			val, ok := result.Value.Get()
			require.True(t, ok)
			assert.Equal(t, value, val)
		}

		// Cleanup.
		cleanupTyped(t, typed, cleanupNames...)
	})
}

func TestTypedIntegration_LargeValue(t *testing.T) {
	testTypedLargeValue := func(t *testing.T, driverInstance driver.Driver, maxMemory int) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)

		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			WithSignerVerifier(crypto.NewRSAPSSSignerVerifier(*rsaPK2048)).
			WithHasher(hasher.NewSHA256Hasher()).
			Build()

		value := IntegrationStruct{Name: generateRandomString(t, maxMemory), Value: 10}

		err := typed.Put(ctx, "big-object", value)
		require.NoError(t, err)

		result, err := typed.Get(ctx, "big-object")
		require.NoError(t, err)
		assert.Equal(t, "big-object", result.Name)
		assert.True(t, result.Value.IsSome())

		val, ok := result.Value.Get()
		require.True(t, ok)
		assert.Equal(t, value, val)

		// Cleanup.
		cleanupTyped(t, typed, "big-object")
	}

	t.Run("TCS", func(t *testing.T) {
		driver, cleanup := createTcsTestDriver(t.Context(), t)
		defer cleanup()

		testTypedLargeValue(t, driver, megabyte/2)
	})
	t.Run("etcd", func(t *testing.T) {
		driver, cleanup := createEtcdTestDriver(t.Context(), t)
		defer cleanup()

		testTypedLargeValue(t, driver, megabyte)
	})
}

func TestTypedIntegration_SpecialNames(t *testing.T) {
	executeOnStorage(t, func(t *testing.T, driverInstance driver.Driver) {
		t.Helper()

		ctx := t.Context()
		storageInstance := storage.NewStorage(driverInstance)
		typed := integrity.NewTypedBuilder[IntegrationStruct](storageInstance).
			WithPrefix("/test").
			Build()

		value := IntegrationStruct{Name: "obj", Value: 10}
		names := []string{"ȵ_ț-object", "¼-Õ-object", "$%#!-object"}

		for _, name := range names {
			err := typed.Put(ctx, name, value)
			require.NoError(t, err)
		}

		for _, name := range names {
			result, err := typed.Get(ctx, name)
			require.NoError(t, err)
			assert.Equal(t, name, result.Name)
			assert.True(t, result.Value.IsSome())

			val, ok := result.Value.Get()
			require.True(t, ok)
			assert.Equal(t, value, val)
		}

		// Cleanup.
		cleanupTyped(t, typed, names...)
	})
}
