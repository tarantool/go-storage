package etcd //nolint:testpackage

import (
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-storage/predicate"
)

func TestPredicateToCmp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		predicate   predicate.Predicate
		checkerFunc func(cmp etcd.Cmp)
	}{
		{
			name:      "value equal predicate",
			predicate: predicate.ValueEqual([]byte("test-key"), []byte("test-value")),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_VALUE, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				assert.Equal(t, []byte("test-value"), cmp.ValueBytes())
			},
		},
		{
			name:      "value not equal predicate",
			predicate: predicate.ValueNotEqual([]byte("test-key"), []byte("test-value")),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_VALUE, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_NOT_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_Value{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, []byte("test-value"),
					cmp.TargetUnion.(*etcdserverpb.Compare_Value).Value) //nolint:forcetypeassert
			},
		},
		{
			name:      "version equal predicate",
			predicate: predicate.VersionEqual([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
		{
			name:      "version not equal predicate",
			predicate: predicate.VersionNotEqual([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_NOT_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
		{
			name:      "version greater predicate",
			predicate: predicate.VersionGreater([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_GREATER, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
		{
			name:      "version less predicate",
			predicate: predicate.VersionLess([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_LESS, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmp, err := predicateToCmp(tt.predicate)

			require.NoError(t, err)
			assert.NotNil(t, cmp)

			if tt.checkerFunc != nil {
				tt.checkerFunc(cmp)
			}
		})
	}
}

func TestValuePredicateToCmp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		predicate   predicate.Predicate
		checkerFunc func(cmp etcd.Cmp)
	}{
		{
			name:      "value equal operation",
			predicate: predicate.ValueEqual([]byte("test-key"), []byte("test-value")),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_VALUE, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_Value{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, []byte("test-value"),
					cmp.TargetUnion.(*etcdserverpb.Compare_Value).Value) //nolint:forcetypeassert
			},
		},
		{
			name:      "value not equal operation",
			predicate: predicate.ValueNotEqual([]byte("test-key"), []byte("test-value")),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_VALUE, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_NOT_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_Value{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, []byte("test-value"),
					cmp.TargetUnion.(*etcdserverpb.Compare_Value).Value) //nolint:forcetypeassert
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmp, err := valuePredicateToCmp(tt.predicate)

			require.NoError(t, err)
			assert.NotNil(t, cmp)

			if tt.checkerFunc != nil {
				tt.checkerFunc(cmp)
			}
		})
	}
}

func TestVersionPredicateToCmp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		predicate   predicate.Predicate
		checkerFunc func(cmp etcd.Cmp)
	}{
		{
			name:      "version equal operation",
			predicate: predicate.VersionEqual([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
		{
			name:      "version not equal operation",
			predicate: predicate.VersionNotEqual([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_NOT_EQUAL, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
		{
			name:      "version greater operation",
			predicate: predicate.VersionGreater([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_GREATER, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
		{
			name:      "version less operation",
			predicate: predicate.VersionLess([]byte("test-key"), int64(123)),
			checkerFunc: func(cmp etcd.Cmp) {
				assert.Equal(t, etcdserverpb.Compare_MOD, cmp.Target)
				assert.Equal(t, etcdserverpb.Compare_LESS, cmp.Result)
				assert.Equal(t, []byte("test-key"), cmp.KeyBytes())
				require.IsType(t, &etcdserverpb.Compare_ModRevision{}, cmp.TargetUnion) //nolint:exhaustruct
				assert.Equal(t, int64(123),
					cmp.TargetUnion.(*etcdserverpb.Compare_ModRevision).ModRevision) //nolint:forcetypeassert
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmp, err := versionPredicateToCmp(tt.predicate)

			require.NoError(t, err)
			assert.NotNil(t, cmp)

			if tt.checkerFunc != nil {
				tt.checkerFunc(cmp)
			}
		})
	}
}

func TestPredicateToCmp_EmptyKey(t *testing.T) {
	t.Parallel()

	pred := predicate.ValueEqual([]byte(""), []byte("test-value"))

	cmp, err := predicateToCmp(pred)
	require.NoError(t, err)
	assert.NotNil(t, cmp)
}

func TestPredicateToCmp_EmptyValue(t *testing.T) {
	t.Parallel()

	pred := predicate.ValueEqual([]byte("test-key"), []byte(""))

	cmp, err := predicateToCmp(pred)
	require.NoError(t, err)
	assert.NotNil(t, cmp)
}
