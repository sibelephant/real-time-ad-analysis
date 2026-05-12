#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NETWORK="real-time-ad-analysis_ad-analysis-network"

wait_for_http() {
  local url=$1
  local name=$2
  local max_attempts=40
  local attempt=0
  echo -n "Waiting for $name to be ready..."
  until curl -sf "$url" > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo " TIMED OUT! Check: docker compose logs"
      exit 1
    fi
    echo -n "."
    sleep 3
  done
  echo " OK"
}

wait_for_pinot() {
  local max_attempts=60
  local attempt=0
  echo -n "Waiting for Pinot Controller to be ready..."
  until [[ "$(curl -s http://localhost:9000/health 2>/dev/null)" =~ ^(OK|GOOD)$ ]]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo " TIMED OUT! Check: docker compose logs pinot-controller"
      exit 1
    fi
    echo -n "."
    sleep 3
  done
  echo " OK"
}

wait_for_kafka() {
  local max_attempts=30
  local attempt=0
  echo -n "Waiting for Redpanda to be ready..."
  until docker exec redpanda rpk cluster info > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo " TIMED OUT! Check: docker compose logs redpanda"
      exit 1
    fi
    echo -n "."
    sleep 3
  done
  echo " OK"
}

# 1. Wait for services
wait_for_kafka
wait_for_pinot
wait_for_http "http://localhost:8081" "Flink JobManager"

# 2. Create Redpanda topics
echo ""
echo "==> Creating Redpanda topics..."
docker exec redpanda rpk topic create ad-events --partitions 4 --replicas 1 || echo "  (ad-events already exists)"
docker exec redpanda rpk topic create enriched-ad-events --partitions 4 --replicas 1 || echo "  (enriched-ad-events already exists)"

# 3. Create Pinot schema and table
echo ""
echo "==> Creating Pinot schema and table..."
docker run --rm \
  --network "$NETWORK" \
  -v "${PROJECT_DIR}/pinot:/pinot" \
  apachepinot/pinot:1.0.0 AddTable \
    -schemaFile /pinot/schema.json \
    -tableConfigFile /pinot/table.json \
    -controllerHost pinot-controller \
    -controllerPort 9000 \
    -exec 2>&1 | grep -E "(INFO \[AddTable|status|error)" || true

# 4. Submit the Flink SQL streaming job
echo ""
echo "==> Submitting Flink SQL streaming job..."
docker exec flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /flink/job.sql

echo ""
echo "=============================================="
echo " Setup complete! System is running."
echo "=============================================="
echo ""
echo "  Flink Web UI:        http://localhost:8081"
echo "  Pinot Data Explorer: http://localhost:9000"
echo ""
echo "  Then start generating events:"
echo "    curl -X POST http://localhost:8000/start_generation"
echo ""
echo "  Query analytics:"
echo "    curl http://localhost:8000/analytics/summary"
echo "    curl http://localhost:8000/analytics/realtime"
echo "=============================================="
