#!/usr/bin/env bash
set -euo pipefail

# Run Scry via Docker against i-want-my-mtg's local postgres
# Usage: ./scripts/run-local.sh <command> [flags]
#
# Examples:
#   ./scripts/run-local.sh ingest
#   ./scripts/run-local.sh ingest -s
#   ./scripts/run-local.sh post-ingest-updates
#   ./scripts/run-local.sh health

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ $# -eq 0 ]; then
    echo "Usage: ./scripts/run-local.sh <command> [flags]"
    echo ""
    echo "Builds and runs Scry from local source against"
    echo "i-want-my-mtg's postgres container."
    exit 1
fi

# Verify i-want-my-mtg's network exists
if ! docker network inspect i-want-my-mtg_default > /dev/null 2>&1; then
    echo "Error: i-want-my-mtg docker network not found."
    echo "Start it first: cd ../i-want-my-mtg && docker compose up -d postgres"
    exit 1
fi

VERSION=$(grep '^version' "$PROJECT_ROOT/Cargo.toml" | head -1 | sed 's/.*"\(.*\)"/\1/')
echo "scry v${VERSION} — building and running: scry $*"
docker compose build etl
docker compose run --rm etl cargo run -- "$@"

# Clean up dangling images from builds
docker image prune -f --filter "until=168h" > /dev/null 2>&1 || true
