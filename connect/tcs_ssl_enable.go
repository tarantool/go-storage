//go:build go_storage_ssl

package connect

import (
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tlsdialer"
)

func newDialerForAddress(cfg Config, address string) (tarantool.Dialer, error) {
	if cfg.SSL.Enable {
		return tlsdialer.OpenSSLDialer{
			Address:         address,
			Auth:            tarantool.AutoAuth,
			User:            cfg.Username,
			Password:        cfg.Password,
			SslKeyFile:      cfg.SSL.KeyFile,
			SslCertFile:     cfg.SSL.CertFile,
			SslCaFile:       cfg.SSL.CaFile,
			SslCiphers:      cfg.SSL.Ciphers,
			SslPassword:     cfg.SSL.Password,
			SslPasswordFile: cfg.SSL.PasswordFile,
		}, nil
	}

	return &tarantool.NetDialer{
		Address:              address,
		User:                 cfg.Username,
		Password:             cfg.Password,
		RequiredProtocolInfo: tarantool.ProtocolInfo{},
	}, nil
}
