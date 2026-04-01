#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CERTS_DIR="$PROJECT_DIR/certs"
STORE_PASSWORD="changeit"
VALIDITY_DAYS=365

# Services that need certificates
BROKERS=("kafka-primary" "kafka-secondary")
CLIENTS=("schema-registry" "schema-registry-secondary" "mirrormaker2" "kafka-producer" "kafka-consumer" "kafka-ui" "init-topics")

echo "=== Kafka mTLS Certificate Generator ==="
echo "Output directory: $CERTS_DIR"

# Clean and recreate
rm -rf "$CERTS_DIR"
mkdir -p "$CERTS_DIR"

# -----------------------------------------------
# 1. Generate CA
# -----------------------------------------------
echo ""
echo "--- Generating CA ---"
openssl req -new -x509 \
  -keyout "$CERTS_DIR/ca-key.pem" \
  -out "$CERTS_DIR/ca-cert.pem" \
  -days "$VALIDITY_DAYS" \
  -subj "/CN=KafkaDevCA" \
  -nodes 2>/dev/null

echo "CA certificate created."

# -----------------------------------------------
# 2. Generate broker server certificates
# -----------------------------------------------
generate_cert() {
  local name="$1"
  local san="$2"
  local keystore="$CERTS_DIR/${name}.keystore.p12"

  echo "Generating certificate for: $name (SAN: $san)"

  # Generate keypair in PKCS12 keystore
  keytool -genkeypair \
    -alias "$name" \
    -keyalg RSA -keysize 2048 \
    -dname "CN=$name" \
    -ext "SAN=$san" \
    -keystore "$keystore" \
    -storetype PKCS12 \
    -storepass "$STORE_PASSWORD" \
    -keypass "$STORE_PASSWORD" \
    -validity "$VALIDITY_DAYS" 2>/dev/null

  # Export CSR
  keytool -certreq \
    -alias "$name" \
    -keystore "$keystore" \
    -storetype PKCS12 \
    -storepass "$STORE_PASSWORD" \
    -file "$CERTS_DIR/${name}.csr" 2>/dev/null

  # Sign CSR with CA (include SANs in the signed cert)
  openssl x509 -req \
    -CA "$CERTS_DIR/ca-cert.pem" \
    -CAkey "$CERTS_DIR/ca-key.pem" \
    -CAcreateserial \
    -in "$CERTS_DIR/${name}.csr" \
    -out "$CERTS_DIR/${name}-signed.pem" \
    -days "$VALIDITY_DAYS" \
    -extfile <(printf "subjectAltName=%s" "$san") 2>/dev/null

  # Import CA cert into keystore (must be imported before the signed cert)
  keytool -importcert \
    -alias ca-root \
    -keystore "$keystore" \
    -storetype PKCS12 \
    -storepass "$STORE_PASSWORD" \
    -file "$CERTS_DIR/ca-cert.pem" \
    -noprompt 2>/dev/null

  # Import signed cert into keystore (replaces the self-signed one)
  keytool -importcert \
    -alias "$name" \
    -keystore "$keystore" \
    -storetype PKCS12 \
    -storepass "$STORE_PASSWORD" \
    -file "$CERTS_DIR/${name}-signed.pem" \
    -noprompt 2>/dev/null

  echo "  -> $keystore"
}

echo ""
echo "--- Generating broker certificates ---"
for broker in "${BROKERS[@]}"; do
  generate_cert "$broker" "DNS:${broker},DNS:localhost,DNS:kafka.internal"
done

# -----------------------------------------------
# 3. Generate client certificates
# -----------------------------------------------
echo ""
echo "--- Generating client certificates ---"
for client in "${CLIENTS[@]}"; do
  san="DNS:${client}"
  if [[ "$client" == "schema-registry" || "$client" == "schema-registry-secondary" ]]; then
    san="DNS:${client},DNS:schema-registry.internal"
  fi
  generate_cert "$client" "$san"
done

# -----------------------------------------------
# 4. Generate shared truststore (CA cert only)
# -----------------------------------------------
echo ""
echo "--- Generating shared truststore ---"
keytool -importcert \
  -alias ca-root \
  -keystore "$CERTS_DIR/truststore.p12" \
  -storetype PKCS12 \
  -storepass "$STORE_PASSWORD" \
  -file "$CERTS_DIR/ca-cert.pem" \
  -noprompt 2>/dev/null

echo "  -> $CERTS_DIR/truststore.p12"

# -----------------------------------------------
# 5. Export PEM files for nginx proxies
# -----------------------------------------------
echo ""
echo "--- Exporting PEM files for nginx ---"
NGINX_SERVICES=("schema-registry" "schema-registry-secondary")
for svc in "${NGINX_SERVICES[@]}"; do
  # Export private key from PKCS12 keystore
  openssl pkcs12 \
    -in "$CERTS_DIR/${svc}.keystore.p12" \
    -nocerts -nodes \
    -passin "pass:$STORE_PASSWORD" \
    -out "$CERTS_DIR/${svc}.key.pem" 2>/dev/null

  # Export signed certificate from PKCS12 keystore (exclude CA cert)
  openssl pkcs12 \
    -in "$CERTS_DIR/${svc}.keystore.p12" \
    -clcerts -nokeys \
    -passin "pass:$STORE_PASSWORD" \
    -out "$CERTS_DIR/${svc}.cert.pem" 2>/dev/null

  echo "  -> $CERTS_DIR/${svc}.cert.pem"
  echo "  -> $CERTS_DIR/${svc}.key.pem"
done

# -----------------------------------------------
# 6. Set permissions (containers may run as non-root)
# -----------------------------------------------
chmod 644 "$CERTS_DIR"/*.p12 "$CERTS_DIR"/*.pem

# -----------------------------------------------
# 7. Create credentials file for Kafka Docker image
# -----------------------------------------------
echo ""
echo "--- Creating credentials file ---"
echo -n "$STORE_PASSWORD" > "$CERTS_DIR/ssl-credentials"
chmod 644 "$CERTS_DIR/ssl-credentials"
echo "  -> $CERTS_DIR/ssl-credentials"

# Clean up intermediate files
rm -f "$CERTS_DIR"/*.csr "$CERTS_DIR"/*-signed.pem "$CERTS_DIR"/*.srl

echo ""
echo "=== Done! Generated certificates in $CERTS_DIR ==="
echo ""
echo "Files:"
ls -la "$CERTS_DIR"
