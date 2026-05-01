#!/usr/bin/env bash
set -uo pipefail

failures=0

check() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    printf '  [OK]   %s\n' "$name"
  else
    printf '  [FAIL] %s\n' "$name"
    failures=$((failures + 1))
  fi
}

echo "Checking sentinel-fraud-pipeline services..."

check "kafka"    docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092
check "minio"    curl -sf http://localhost:9000/minio/health/live
check "spark"    curl -sf http://localhost:8080
check "postgres" docker compose exec -T postgres pg_isready -U airflow -d airflow

echo
if [[ $failures -eq 0 ]]; then
  echo "All services healthy."
  exit 0
fi

echo "$failures service(s) failed."
exit 1
