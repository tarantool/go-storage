package connect

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	etcdclient "go.etcd.io/etcd/client/v3"
)

func createEtcdClient(ctx context.Context, cfg Config) (*etcdclient.Client, CleanupFunc, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, nil, ErrNoEndpoint
	}

	err := validateSSLConfig(cfg.SSL)
	if err != nil {
		return nil, nil, err
	}

	etcdConfig := etcdclient.Config{ //nolint:exhaustruct
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.dialTimeout(),
		Username:    cfg.Username,
		Password:    cfg.Password,
	}

	if cfg.SSL.Enable {
		tlsConfig, buildErr := buildTLSConfig(cfg.SSL)
		if buildErr != nil {
			return nil, nil, fmt.Errorf("%w: %w", errFailedTLSConfig, buildErr)
		}

		etcdConfig.TLS = tlsConfig
	}

	client, clientErr := etcdclient.New(etcdConfig)
	if clientErr != nil {
		return nil, nil, fmt.Errorf("%w: %w", errFailedEtcdClient, clientErr)
	}

	_, statusErr := client.Status(ctx, cfg.Endpoints[0])
	if statusErr != nil {
		_ = client.Close()

		return nil, nil, fmt.Errorf("%w: %w", errFailedEtcdProbe, statusErr)
	}

	return client, func() { _ = client.Close() }, nil
}

func validateSSLConfig(cfg SSLConfig) error {
	if !cfg.Enable {
		return nil
	}

	if (cfg.CertFile != "" && cfg.KeyFile == "") || (cfg.CertFile == "" && cfg.KeyFile != "") {
		return fmt.Errorf("%w: both CertFile and KeyFile must be specified together", ErrInvalidSSLConfig)
	}

	return nil
}

func buildTLSConfig(cfg SSLConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{} //nolint:exhaustruct

	if cfg.Ciphers != "" {
		cipherIDs, err := parseCiphers(cfg.Ciphers)
		if err != nil {
			return nil, err
		}

		tlsConfig.CipherSuites = cipherIDs
	}

	if cfg.CaFile != "" || cfg.CaPath != "" {
		certPool := x509.NewCertPool()

		err := loadCAFiles(certPool, cfg)
		if err != nil {
			return nil, err
		}

		err = loadCADirectory(certPool, cfg)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs = certPool
	}

	err := loadCertificates(tlsConfig, cfg)
	if err != nil {
		return nil, err
	}

	if !cfg.VerifyHost && !cfg.VerifyPeer {
		tlsConfig.InsecureSkipVerify = true
	}

	return tlsConfig, nil
}

func loadCAFiles(certPool *x509.CertPool, cfg SSLConfig) error {
	if cfg.CaFile == "" {
		return nil
	}

	caCert, err := os.ReadFile(cfg.CaFile)
	if err != nil {
		return fmt.Errorf("%w %q: %w", errFailedReadCAFile, cfg.CaFile, err)
	}

	if !certPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("%w from %q", errNoCACerts, cfg.CaFile)
	}

	return nil
}

func loadCADirectory(certPool *x509.CertPool, cfg SSLConfig) error {
	if cfg.CaPath == "" {
		return nil
	}

	entries, err := os.ReadDir(cfg.CaPath)
	if err != nil {
		return fmt.Errorf("%w %q: %w", errFailedReadCAPath, cfg.CaPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		caCertPath := filepath.Join(cfg.CaPath, entry.Name())

		caCert, readErr := os.ReadFile(caCertPath) //nolint:gosec // G304: file path from config
		if readErr != nil {
			return fmt.Errorf("%w %q: %w", errCertFileRead, caCertPath, readErr)
		}

		if !certPool.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("%w from %q", errNoCACerts, caCertPath)
		}
	}

	return nil
}

func loadCertificates(tlsConfig *tls.Config, cfg SSLConfig) error {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil
	}

	password, err := resolveKeyPassword(cfg)
	if err != nil {
		return err
	}

	var cert tls.Certificate

	if password != "" {
		cert, err = loadEncryptedKeyPair(cfg.CertFile, cfg.KeyFile, password)
		if err != nil {
			return err
		}
	} else {
		cert, err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return fmt.Errorf("%w: %w", errFailedLoadKeyPair, err)
		}
	}

	tlsConfig.Certificates = []tls.Certificate{cert}

	return nil
}

func resolveKeyPassword(cfg SSLConfig) (string, error) {
	if cfg.Password != "" {
		return cfg.Password, nil
	}

	if cfg.PasswordFile == "" {
		return "", nil
	}

	data, err := os.ReadFile(cfg.PasswordFile)
	if err != nil {
		return "", fmt.Errorf("%w %q: %w", errFailedReadPwdFile, cfg.PasswordFile, err)
	}

	return strings.TrimRight(string(data), "\r\n"), nil
}

func loadEncryptedKeyPair(certFile, keyFile, password string) (tls.Certificate, error) {
	keyData, err := os.ReadFile(keyFile) //nolint:gosec // G304: file path from config
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("%w %q: %w", errFailedReadKeyFile, keyFile, err)
	}

	keyBlock, _ := pem.Decode(keyData)
	if keyBlock == nil {
		return tls.Certificate{}, fmt.Errorf("%w: failed to decode PEM block from %q", errFailedDecryptKey, keyFile)
	}

	if x509.IsEncryptedPEMBlock(keyBlock) { //nolint:staticcheck
		decrypted, decErr := x509.DecryptPEMBlock(keyBlock, []byte(password)) //nolint:staticcheck
		if decErr != nil {
			return tls.Certificate{}, fmt.Errorf("%w: %w", errFailedDecryptKey, decErr)
		}

		keyBlock.Bytes = decrypted
		keyBlock.Headers = nil
	}

	certData, err := os.ReadFile(certFile) //nolint:gosec // G304: file path from config
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("%w %q: %w", errCertFileRead, certFile, err)
	}

	cert, err := tls.X509KeyPair(certData, pem.EncodeToMemory(keyBlock))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("%w: %w", errFailedLoadKeyPair, err)
	}

	return cert, nil
}
