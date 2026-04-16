# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Scry is a Rust CLI tool for ETL (Extract, Transform, Load) of Magic: The Gathering data from the Scryfall API into a PostgreSQL database. It is part of the larger "I Want My MTG" project. The web app lives in a separate repository: [i-want-my-mtg](https://github.com/matthewdtowles/i-want-my-mtg).

## Common Commands

```bash
cargo test                        # Run unit tests only (integration tests are #[ignore]d)
cargo test card::                 # Run unit tests in a specific module
cargo build --release             # Production build
cargo run -- ingest               # Full ingest: sets, cards, prices + post-ingest prune/updates
cargo run -- ingest -s            # Ingest sets only
cargo run -- ingest -c            # Ingest cards only
cargo run -- ingest -p            # Ingest prices only
cargo run -- ingest -k abc        # Ingest cards for a specific set code
cargo run -- ingest -r            # Reset all data before ingesting (interactive confirm)
cargo run -- post-ingest-prune    # Prune unwanted data (foreign unpriced, empty sets, dup foils)
cargo run -- post-ingest-updates  # Recalculate set sizes, prices, fix main set classifications
cargo run -- cleanup -c           # Stream-cleanup individual cards based on filtering rules
cargo run -- health               # Basic health check
cargo run -- health --detailed    # Detailed health check
cargo run -- interactive          # Launch interactive menu (run multiple commands in one session)
cargo run -- retention            # Apply tiered retention to price_history, set_price_history, portfolio_value_history
cargo run -- truncate-history     # Truncate price_history (interactive confirm)
```

Set `SCRY_LOG` env var for log verbosity (default: `scry=info`). Reads `DATABASE_URL` or individual `DB_*` vars from `.env`.

### CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) on push to main:
1. **test** — Runs `cargo test -- --include-ignored` (unit + integration)
2. **tag** — Creates GitHub release from Cargo.toml version
3. **build** — Builds and pushes Docker image to `ghcr.io/matthewdtowles/scry:latest`

### Integration Tests

Integration tests are marked `#[ignore]` so `cargo test` skips them (they require a running PostgreSQL instance). To run locally:

```bash
./scripts/test-integ.sh                             # Run all tests (starts/stops test DB automatically)
./scripts/test-integ.sh --test set_repository_test  # Run a single integration test file
cargo test -- --include-ignored                     # Run all tests (if DB is already running)
cargo test -- --ignored                             # Run only integration tests (if DB is already running)
```

The script starts a Postgres container on port 5433, runs tests with `--include-ignored`, and tears down on exit. CI provisions its own Postgres service container.

### Docker

```bash
docker build -t scry --target production .    # Build production image
docker build -t scry-dev --target development .  # Build development image
```

## Architecture

### Module Structure

Each feature module follows a consistent pattern: `domain/` (data types), `mapper.rs` (API JSON to domain), `repository.rs` (SQLx queries), `service.rs` (business logic).

```
src/
├── main.rs              — Entry point: tokio runtime, logging, DI wiring
├── cli/
│   ├── commands.rs      — Clap CLI definitions (Commands enum)
│   └── controller.rs    — Command dispatch, orchestrates service calls
├── config.rs            — Env-based config (DATABASE_URL, pool size)
├── database.rs          — ConnectionPool wrapper around SQLx PgPool
├── card/
│   ├── domain/          — Card, CardRarity, Format, Legality, LegalityStatus, MainSetClassifier
│   ├── event_processor.rs — JsonEventProcessor impl for streaming card parsing
│   ├── mapper.rs        — Scryfall JSON → Card domain mapping
│   ├── repository.rs    — Card/legality UPSERT queries
│   └── service.rs       — Card ingestion, cleanup, pruning logic
├── set/
│   ├── domain/          — Set, SetPrice
│   ├── mapper.rs        — Scryfall JSON → Set domain mapping
│   ├── repository.rs    — Set UPSERT/delete queries
│   └── service.rs       — Set ingestion, cleanup, size/price updates
├── price/
│   ├── domain/          — Price, PriceAccumulator
│   ├── event_processor.rs — JsonEventProcessor impl for streaming price parsing
│   ├── repository.rs    — Price/price_history queries
│   └── service.rs       — Price ingestion, retention, cleanup
├── health_check/
│   ├── models.rs        — Health check result types
│   └── service.rs       — Data integrity checks
└── utils/
    ├── http_client.rs   — Reqwest-based Scryfall API client
    ├── json.rs          — JSON helper utilities
    └── json_stream_parser.rs — Generic streaming JSON parser using actson
```

### Key Design Patterns

**Streaming JSON parsing**: Card and price ingestion uses `JsonStreamParser<T, P>` with the `JsonEventProcessor` trait. This streams Scryfall's bulk data files (~200MB+) through actson without loading them into memory. Each module implements its own `EventProcessor` that emits batches.

**Dependency injection via constructor**: `main.rs` wires up the dependency graph manually — `ConnectionPool` → services → `CliController`. Services take `Arc<ConnectionPool>` and `Arc<HttpClient>`.

**Ingest pipeline**: The `ingest` command runs a full pipeline: ingest (sets → cards → prices) → post-ingest prune (remove unwanted data) → post-ingest updates (set sizes, set prices, main set classification fixes).

**Batch processing with concurrency**: Card ingestion uses `Semaphore` for bounded concurrency (6 concurrent tasks) with batch sizes of 500 records. Repositories use SQLx `QueryBuilder` for bulk UPSERTs via `ON CONFLICT`.

### Database

Shares the same PostgreSQL database as the NestJS web app ([i-want-my-mtg](https://github.com/matthewdtowles/i-want-my-mtg)). Schema and migrations are managed in the web app repo. Core tables: `card`, `set`, `price`, `price_history`, `legality`, `set_price`.

Uses SQLx with the `runtime-tokio-rustls` feature. The `ConnectionPool` struct wraps `PgPool` and provides helper methods for common query patterns (count, execute, fetch).

### Card Filtering

Cards go through `should_filter()` and `merge_and_filter_cards()` during ingestion. Split cards are merged (combining mana costs from both faces). Foreign cards without prices are pruned post-ingest. The `MainSetClassifier` determines whether a card belongs to a set's "main" (base) subset.
