# Scry

Rust CLI tool for ETL (Extract, Transform, Load) of Magic: The Gathering data from the [Scryfall API](https://scryfall.com/docs/api) into a PostgreSQL database. Part of the [I Want My MTG](https://github.com/matthewdtowles/i-want-my-mtg) project.

## Quick Start

```bash
# Set up environment
cp .env.example .env  # Configure DATABASE_URL

# Run
cargo run -- ingest           # Full ingest (sets, cards, prices)
cargo run -- health           # Data integrity check
```

## Commands

### `ingest` — Ingest MTG data from Scryfall

With no flags, ingests all sets, cards, and prices. Automatically runs post-ingest pruning and updates afterward.

```bash
scry ingest              # Ingest everything (sets, cards, prices)
scry ingest -s           # Ingest sets only
scry ingest -c           # Ingest cards only
scry ingest -p           # Ingest prices only
scry ingest -k <CODE>    # Ingest cards for a specific set (e.g., -k mh3)
scry ingest -r           # Reset all data before ingesting (requires confirmation)
```

Flags can be combined, e.g. `scry ingest -s -p` to ingest sets and prices.

### `post-ingest-prune` — Prune unwanted ingested data

Removes foreign cards without prices, sets missing price data, empty sets, and duplicate foil cards. Runs automatically after `ingest`, but can be run standalone.

```bash
scry post-ingest-prune
```

### `post-ingest-updates` — Update set sizes, prices, and portfolio snapshots

Fixes main set misclassifications, calculates set sizes, updates set prices, and takes daily portfolio value snapshots for all users. Runs automatically after `ingest`, but can be run standalone.

```bash
scry post-ingest-updates
```

### `cleanup` — Remove previously saved sets/cards

Only necessary if filtering rules have been updated to exclude sets or cards that were already ingested.

```bash
scry cleanup             # Clean up sets based on set filtering rules
scry cleanup -c          # Also clean up individual cards based on card filtering rules
scry cleanup -c -n 1000  # Card cleanup with custom batch size (default: 500)
```

### `health` — Check data integrity

```bash
scry health              # Basic health check
scry health --detailed   # Detailed health check
```

### `retention` — Apply retention policy

Applies a tiered retention policy to `price_history`, `set_price_history`, and `portfolio_value_history`: keeps daily rows for 7 days, weekly (Mondays) for 7-28 days, and monthly (1st of month) for 28+ days.

```bash
scry retention
```

### `backfill` — Backfill price_history from MTGJSON

One-time operation that downloads AllPrices.json from MTGJSON and backfills the `price_history` table with historical price data. Averages prices across providers (TCGPlayer, Card Kingdom, Cardsphere) per card per date.

```bash
scry backfill                      # Backfill historical prices
scry backfill --truncate           # Truncate price_history first (requires confirmation)
scry backfill --skip-retention     # Skip retention policy after backfill
```

### `truncate-history` — Truncate the price_history table

Deletes all data from the price_history table. Requires interactive confirmation.

```bash
scry truncate-history
```

### `portfolio-summary` — Refresh portfolio value snapshots

Takes a daily portfolio value snapshot for all users.

```bash
scry portfolio-summary
```

## Docker

```bash
docker build -t scry --target production .       # Build production image
docker run --rm -e DATABASE_URL=... scry ingest   # Run with Docker
```

## Environment

| Variable | Description | Default |
| -------- | ----------- | ------- |
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `SCRY_LOG` | Log verbosity | `scry=info` |

## Crate Structure

Scry is a CLI binary, but the project uses both `main.rs` and `lib.rs`. The binary entry point is `main.rs`, which owns CLI-specific wiring (`cli/` module, dependency setup, and command dispatch). The `lib.rs` re-exports all other modules as a library crate so that integration tests in `tests/` can import them via `use scry::...`. Without `lib.rs`, Cargo's integration tests would have no access to internal types like repositories and domain models.

- **`main.rs`** — Binary entry point. Declares `mod cli` (not shared with the library) and wires up the dependency graph.
- **`lib.rs`** — Library crate. Re-exports shared modules (`card`, `set`, `price`, `config`, `database`, etc.) for use by integration tests.

## Testing

### Running Tests

```bash
cargo test                                          # Unit tests only (integration tests are skipped)
cargo test card::                                   # Unit tests in a specific module
./scripts/test-integ.sh                             # All tests (starts/stops test DB automatically)
./scripts/test-integ.sh --test set_repository_test  # Single integration test file
cargo test -- --include-ignored                     # All tests (if DB is already running)
cargo test -- --ignored                             # Integration tests only (if DB is already running)
```

### Adding Tests

**Unit tests** live inline in `src/` files inside a `#[cfg(test)] mod tests` block. These run with `cargo test` and should not require any external services.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example() {
        assert!(true);
    }
}
```

**Integration tests** live in the `tests/` directory and run against a real PostgreSQL database. Each integration test must be marked `#[ignore]` so `cargo test` skips it. Use `tests/common/mod.rs` helpers for database setup and test data.

```rust
mod common;

#[tokio::test]
#[ignore]
async fn test_example_repository() {
    let db = common::setup_test_db().await;
    // ...
}
```

## Build & Deploy

CI/CD via GitHub Actions (`.github/workflows/ci.yml`) on push to main:
1. Runs `cargo test -- --include-ignored` (unit + integration)
2. Creates GitHub release from `Cargo.toml` version
3. Builds and pushes Docker image to `ghcr.io/matthewdtowles/scry:latest`

The web app's deploy pipeline pulls the latest image and extracts the scry binary for cron job execution on the production server.
