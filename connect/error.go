package connect

import "errors"

var (
	// ErrNoEndpoint is returned when no endpoint is provided.
	ErrNoEndpoint = errors.New("at least one endpoint is required")
	// ErrSSLDisabled is returned when SSL is requested but support is disabled.
	ErrSSLDisabled = errors.New("SSL support is disabled")
	// ErrInvalidSSLConfig is returned when SSL configuration is invalid.
	ErrInvalidSSLConfig = errors.New("invalid SSL configuration")

	errNoCACerts         = errors.New("failed to append CA certificates")
	errFailedReadCAFile  = errors.New("failed to read CA file")
	errFailedReadCAPath  = errors.New("failed to read CA path")
	errFailedLoadKeyPair = errors.New("failed to load key pair")
	errFailedTLSConfig   = errors.New("failed to build TLS config")
	errFailedEtcdClient  = errors.New("failed to create etcd client")
	errFailedEtcdProbe   = errors.New("failed to probe etcd cluster")
	errFailedTarantool   = errors.New("failed to connect to Tarantool")
	errCertFileRead      = errors.New("failed to read certificate file")
	errUnknownCipher     = errors.New("unknown cipher")
	errFailedDecryptKey  = errors.New("failed to decrypt private key")
	errFailedReadKeyFile = errors.New("failed to read key file")
	errFailedReadPwdFile = errors.New("failed to read password file")
)
