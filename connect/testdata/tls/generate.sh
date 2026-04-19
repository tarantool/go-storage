#!/usr/bin/env bash
# Script to generate TLS certificates for testing.
# Run this script to regenerate certificates if they expire.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create extensions file for localhost.
cat > domains.ext << 'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

# Generate CA certificate.
openssl req -x509 -nodes -new -sha256 -days 8192 -newkey rsa:2048 \
    -keyout ca.key -out ca.pem \
    -subj "/C=US/CN=go-storage-test-CA"
openssl x509 -outform pem -in ca.pem -out ca.crt

# Generate server certificate.
openssl req -new -nodes -newkey rsa:2048 \
    -keyout localhost.key -out localhost.csr \
    -subj "/C=US/ST=State/L=City/O=go-storage-test/CN=localhost"
openssl x509 -req -sha256 -days 8192 \
    -in localhost.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -extfile domains.ext -out localhost.crt

# Generate encrypted private key for testing password-protected keys
# Using -traditional flag for PKCS#1 format (compatible with
# x509.IsEncryptedPEMBlock).
openssl rsa -aes256 -passout pass:testpass -in localhost.key \
    -out localhost-encrypted.key -traditional

# Generate password file for testing PasswordFile config option.
echo -n "testpass" > password.txt

# Create directory with CA certificate for testing CaPath config option.
mkdir -p only_ca
cp ca.crt only_ca/

# Cleanup temporary files.
rm -f ca.key ca.pem ca.srl localhost.csr domains.ext

echo "Generated:"
echo "ca.crt, localhost.crt, localhost.key,"
echo "localhost-encrypted.key, password.txt, only_ca/ca.crt"
