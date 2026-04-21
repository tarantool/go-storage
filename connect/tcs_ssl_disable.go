//go:build !go_storage_ssl

package connect

import (
	"github.com/tarantool/go-tarantool/v2"
)

func newDialerForAddress(cfg Config, address string) (tarantool.Dialer, error) {
	if cfg.SSL.Enable {
		return nil, ErrSSLDisabled
	}

	return &tarantool.NetDialer{
		Address:              address,
		User:                 cfg.Username,
		Password:             cfg.Password,
		RequiredProtocolInfo: tarantool.ProtocolInfo{}, //nolint:exhaustruct
	}, nil
}
