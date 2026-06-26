package mocks

//go:generate go tool minimock -g -i github.com/tarantool/go-storage/v2/driver/tcs.Client -pr tcs_ -s _mock.go -n TCSClientMock
