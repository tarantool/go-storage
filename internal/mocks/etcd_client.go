package mocks

//go:generate go tool minimock -g -i github.com/tarantool/go-storage/driver/etcd.Client -pr etcd_ -s _mock.go -n EtcdClientMock
