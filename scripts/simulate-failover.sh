#!/usr/bin/env bash
set -euo pipefail

COMPOSE_BASE="docker compose -f docker-compose.yml -f docker-compose.failover.yml"

echo "=== Kafka Failover Simulation ==="
echo ""

# Step 1: Stop producer and consumer
echo "Stopping kafka-producer and kafka-consumer..."
docker compose stop kafka-producer kafka-consumer
echo ""

# Step 2: Restart with secondary profile
echo "Restarting with secondary profile (kafka-secondary + schema-registry-secondary)..."
$COMPOSE_BASE up -d kafka-producer kafka-consumer
echo ""

# Step 3: Tail logs to observe results
echo "Tailing logs for 30 seconds (Ctrl+C to stop early)..."
echo ""
perl -e 'alarm 30; exec @ARGV' $COMPOSE_BASE logs -f kafka-producer kafka-consumer || true

echo ""
echo "=== Failover simulation complete ==="
echo "Run 'docker compose logs -f kafka-producer kafka-consumer' to continue watching."
