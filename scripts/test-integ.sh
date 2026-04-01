#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.test.yml"
export TEST_DATABASE_URL="postgresql://scry_test:scry_test@localhost:5433/scry_test?sslmode=disable"

cleanup() {
    docker compose -f "$COMPOSE_FILE" down --volumes 2>/dev/null
    docker image prune -f --filter "until=168h" > /dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for test database..."
for i in $(seq 1 30); do
    if pg_isready -h localhost -p 5433 -U scry_test -q 2>/dev/null; then
        break
    fi
    sleep 1
done

cargo test "$@" -- --include-ignored
