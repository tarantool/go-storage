package connect

import (
	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tlsdialer/v2"
)

// newDialerForAddress builds a dialer for a single TCS endpoint. With
// cfg.SSL.Enable the dialer is a tlsdialer.TLSDialer over the backend picked at
// build time by newTLSBackend: the pure-Go gostls one by default and the OpenSSL
// one with the openssl build tag.
func newDialerForAddress(cfg Config, address string) tarantool.Dialer {
	if cfg.SSL.Enable {
		return tlsdialer.TLSDialer{
			Address:              address,
			Auth:                 tarantool.AutoAuth,
			User:                 cfg.Username,
			Password:             cfg.Password,
			RequiredProtocolInfo: tarantool.ProtocolInfo{}, //nolint:exhaustruct
			SslKeyFile:           cfg.SSL.KeyFile,
			SslCertFile:          cfg.SSL.CertFile,
			SslCaFile:            cfg.SSL.CaFile,
			SslCiphers:           cfg.SSL.Ciphers,
			SslPassword:          cfg.SSL.Password,
			SslPasswordFile:      cfg.SSL.PasswordFile,
			Backend:              newTLSBackend(),
		}
	}

	return &tarantool.NetDialer{
		Address:              address,
		User:                 cfg.Username,
		Password:             cfg.Password,
		RequiredProtocolInfo: tarantool.ProtocolInfo{}, //nolint:exhaustruct
	}
}
