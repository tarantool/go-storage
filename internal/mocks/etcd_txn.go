package mocks

//go:generate go tool minimock -g -i go.etcd.io/etcd/client/v3.Txn -pr etcd_ -s _mock.go -n ETCDTxnMock
