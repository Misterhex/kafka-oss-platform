#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DNS_DIR="$PROJECT_DIR/config/dns"

usage() {
    echo "Usage: $0 {secondary|primary|status}"
    echo ""
    echo "  secondary  - Flip DNS to secondary cluster (kafka-secondary + schema-registry-secondary)"
    echo "  primary    - Flip DNS back to primary cluster"
    echo "  status     - Show current DNS target"
    exit 1
}

[ $# -ge 1 ] || usage

case "$1" in
    secondary)
        echo "=== DNS Failover: Flipping to SECONDARY cluster ==="
        echo ""
        cp "$DNS_DIR/db.internal.failover" "$DNS_DIR/db.internal"
        echo "Zone file updated. CoreDNS will reload within 2 seconds."
        echo ""
        echo "  kafka.internal           -> 172.20.0.11 (kafka-secondary)"
        echo "  schema-registry.internal -> 172.20.0.21 (schema-registry-secondary)"
        echo ""
        echo "Kafka clients will reconnect automatically (no container restart needed)."
        ;;
    primary)
        echo "=== DNS Failover: Flipping to PRIMARY cluster ==="
        echo ""
        cp "$DNS_DIR/db.internal.primary" "$DNS_DIR/db.internal"
        echo "Zone file updated. CoreDNS will reload within 2 seconds."
        echo ""
        echo "  kafka.internal           -> 172.20.0.10 (kafka-primary)"
        echo "  schema-registry.internal -> 172.20.0.20 (schema-registry)"
        echo ""
        echo "Kafka clients will reconnect automatically (no container restart needed)."
        ;;
    status)
        echo "=== Current DNS Zone ==="
        echo ""
        grep -E "^(kafka|schema-registry)" "$DNS_DIR/db.internal" || echo "No entries found"
        ;;
    *)
        usage
        ;;
esac
