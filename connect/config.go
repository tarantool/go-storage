package connect

import "time"

const (
	defaultDialTimeout = 5 * time.Second
)

// SSLConfig contains SSL/TLS configuration options.
type SSLConfig struct {
	// Enable indicates whether SSL/TLS is enabled.
	Enable bool
	// CaFile is the path to the CA certificate file.
	CaFile string
	// CaPath is the path to a directory containing CA certificate files.
	CaPath string
	// CertFile is the path to the client certificate file.
	CertFile string
	// KeyFile is the path to the client private key file.
	KeyFile string
	// Ciphers is a colon-separated list of cipher suites in OpenSSL format.
	Ciphers string
	// Password is the password for decrypting the private key.
	Password string /* #nosec G117 */
	// PasswordFile is the path to a file containing the password for the private key.
	PasswordFile string
	// VerifyHost indicates whether to verify the server's hostname.
	VerifyHost bool
	// VerifyPeer indicates whether to verify the server's certificate.
	VerifyPeer bool
}

// Config contains connection configuration options.
type Config struct {
	// Endpoints is a list of storage endpoint addresses.
	Endpoints []string
	// Username is the username for authentication.
	Username string
	// Password is the password for authentication.
	Password string /* #nosec G117 */
	// SSL contains SSL/TLS configuration options.
	SSL SSLConfig
	// DialTimeout is the timeout for establishing connections.
	DialTimeout time.Duration
}

func (c Config) dialTimeout() time.Duration {
	if c.DialTimeout == 0 {
		return defaultDialTimeout
	}

	return c.DialTimeout
}
