package storage

//go:generate go tool minimock -g -i github.com/tarantool/go-storage/v2.Prefixer -o prefixer_mock_test.go -n PrefixerMock -p storage
//go:generate go tool minimock -g -i github.com/tarantool/go-storage/v2.Storage -o storage_mock_test.go -n StorageMock -p storage
