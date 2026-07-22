# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Scry is a Rust CLI tool for ETL (Extract, Transform, Load) of Magic: The Gathering data into a PostgreSQL database. Its sources are **MTGJSON** bulk files (`AllPrintings.json`, `AllPricesToday.json`, `AllPrices.json`), the **Card Kingdom** direct pricelist (live buylist), and the **fbettega** tournament-deck feed - not the Scryfall API (Scryfall is only the source of the card *images* the web app renders). It is part of the larger "I Want My MTG" project. The web app lives in a separate repository: [i-want-my-mtg](https://github.com/matthewdtowles/i-want-my-mtg).

## Common Commands

```bash
cargo test                        # Run unit tests only (integration tests are #[ignore]d)
cargo test card::                 # Run unit tests in a specific module
cargo build --release             # Production build
cargo run -- ingest               # Full ingest: sets, cards + sealed, prices + post-ingest prune/updates
cargo run -- ingest -s            # Ingest sets only
cargo run -- ingest -c            # Ingest cards only
cargo run -- ingest -p            # Ingest prices only
cargo run -- ingest -k abc        # Ingest cards for a specific set code
cargo run -- ingest -b            # Refresh Card Kingdom direct buylist only (no MTGJSON price re-ingest)
cargo run -- ingest -r            # Reset all data before ingesting (interactive confirm)
cargo run -- ingest-decks --days 2 # Ingest published tournament decks (fbettega feed) for the last N days
cargo run -- post-ingest-prune    # Prune unwanted data (foreign unpriced, empty sets, dup foils)
cargo run -- post-ingest-updates  # Recalculate set sizes, prices, fix main set classifications, portfolio snapshots
cargo run -- cleanup -c           # Stream-cleanup individual cards based on filtering rules
cargo run -- health               # Basic health check; exits non-zero when prices are stale
cargo run -- health --detailed    # Detailed health check
cargo run -- interactive          # Launch interactive menu (run multiple commands in one session)
cargo run -- retention            # Apply tiered retention to price_history, set_price_history, portfolio_value_history (see #63)
cargo run -- portfolio-summary    # Compute portfolio_summary + card_performance for all users (cron)
cargo run -- backfill             # One-time: load historical prices from AllPrices.json into price_history
cargo run -- backfill-set-price-history # One-time: derive set_price_history from price_history
cargo run -- truncate-history     # Truncate price_history (interactive confirm)
```

Set `SCRY_LOG` env var for log verbosity (default: `scry=info`). Reads `DATABASE_URL` or individual `DB_*` vars from `.env`.

### CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) on push to main:
1. **lint** — `cargo fmt --all --check` + `cargo clippy --all-targets -- -D warnings` (both gate the pipeline)
2. **test** — Runs `cargo test -- --include-ignored` (unit + integration)
3. **version** — Computes the next semver from git tags + the squash-merged PR title (`.github/scripts/next-version.sh`): `feat:` → minor, `!` → major, anything else → patch
4. **tag** — Creates GitHub release for the computed version
5. **build** — Builds and pushes Docker image to `ghcr.io/matthewdtowles/scry:latest`, stamping the version into Cargo.toml via the `APP_VERSION` build arg

Git tags are the source of truth for the version; Cargo.toml stays at its `0.0.0-dev` placeholder. Use squash merge so the PR title becomes the commit subject on main.

### Deployment order (Scry + web)

Scry writes tables that the **web app's migrations create** ([i-want-my-mtg](https://github.com/matthewdtowles/i-want-my-mtg)), and Scry's CI does **not** touch the production server - the binary only reaches it when the web app's deploy extracts it from `scry:latest`. So when a change spans both repos (e.g. Scry starts writing a new table):

1. **Publish Scry first** (this CI). Safe - the server keeps running the old binary, which stays correct because changes here are additive.
2. **Then deploy the web app.** It runs migrations (creates the table), then extracts the new binary - schema before binary, in one deploy.

Never manually refresh the binary on the server (`docker cp` it out of the image) before the web migration has run, or the new binary will write to a table that does not exist yet.

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

The crate is a library (`lib.rs`) with a thin binary (`main.rs`) over it, so the
code and its tests compile once. `cli` lives in the library.

```
src/
├── main.rs              — Thin shim: builds services, runs CliController (all logic is in lib.rs)
├── lib.rs               — Library root; declares every module (incl. cli)
├── cli/
│   ├── commands.rs      — Clap CLI definitions (Commands enum)
│   ├── controller.rs    — Command dispatch + interactive menu + prompts/display
│   └── ingest_pipeline.rs — IngestPipeline: ingest ordering, prune policy, first-error-wins aggregation
├── config.rs            — Env-based config (DATABASE_URL / DB_* parts, pool size)
├── database.rs          — ConnectionPool wrapper around SQLx PgPool
├── ingest.rs            — Single-pass card + sealed tee (CardSealedEventProcessor) over AllPrintings.json
├── card/
│   ├── domain/          — Card, CardRarity, Format, Legality, LegalityStatus, MainSetClassifier
│   ├── event_processor.rs — JsonEventProcessor impl for streaming card parsing
│   ├── mapper.rs        — MTGJSON JSON → Card domain mapping
│   ├── ports.rs        — CardDataSource + CardRepositoryPort traits (so CardService is testable with fakes)
│   ├── repository.rs    — Card/legality UPSERT queries (impls CardRepositoryPort)
│   └── service.rs       — Card ingestion, cleanup, pruning (depends on the ports above)
├── set/
│   ├── domain/          — Set, SetPrice
│   ├── mapper.rs        — MTGJSON JSON → Set domain mapping
│   ├── ports.rs        — SetCodesSource trait (the sealed-product filter depends on it, not SetRepository directly)
│   ├── repository.rs    — Set UPSERT/delete queries (impls SetCodesSource)
│   └── service.rs       — Set ingestion, cleanup, size/price updates
├── price/
│   ├── domain/          — Price, PriceAccumulator, GranularPrice/CardPrices (granular_price.rs)
│   ├── event_processor.rs — Averaged-price processor for AllPricesToday.json (new) and AllPrices.json (new_historical)
│   ├── cardkingdom.rs   — Card Kingdom direct pricelist parsing (live buylist + qty)
│   ├── write_timings.rs — Per-table write timing instrumentation
│   ├── repository.rs    — price/price_history/granular_price queries + retention
│   └── service.rs       — Price ingestion, CK-direct enrichment, retention, cleanup
├── sealed_product/      — domain / event_processor / mapper / repository / service (sealed products)
├── portfolio/           — domain / repository / service (per-user summaries, snapshots, card performance)
├── published_deck/      — domain / source (fbettega feed) / repository / service (tournament decks)
├── health_check/
│   ├── models.rs        — Health check result types
│   └── service.rs       — Data integrity checks
└── utils/
    ├── clock.rs         — today() (single UTC "today" for the whole crate)
    ├── http_client.rs   — Reqwest client for MTGJSON + Card Kingdom (not Scryfall)
    ├── json.rs          — JSON helper utilities
    ├── json_stream_parser.rs — Generic streaming JSON parser using actson
    └── subtree_collector.rs — SubtreeCollector (events → serde_json::Value) + DocumentCursor (depth/skip tracking) shared by the event processors
```

### Key Design Patterns

**Streaming JSON parsing**: Card, sealed-product, and price ingestion use `JsonStreamParser<T, P>` with the `JsonEventProcessor` trait. This streams MTGJSON's bulk data files (~200MB+) through actson without loading them into memory. Each module implements its own `EventProcessor` that emits batches. Cards and sealed products both come from `AllPrintings.json`, so `ingest.rs`'s `CardSealedEventProcessor` tees one stream to both extractors (each tracks its own depth/skip state) to avoid downloading + parsing that file twice.

**Dependency injection via constructor**: `main.rs` wires up the dependency graph manually — `ConnectionPool` → services → `CliController`. Services take `Arc<ConnectionPool>` and `Arc<HttpClient>`.

**Ports for testability**: `CardService` depends on the `CardDataSource` + `CardRepositoryPort` traits (`card/ports.rs`), not the concrete `HttpClient`/`CardRepository`. Its `new()` wires the real adapters; `with_ports()` lets a test inject a canned byte stream + a spy repository, so `ingest_all` (stream → parse → persist) is unit-tested with no live HTTP or Postgres. Cross-module orchestration lives in the application layer: the single-pass card+sealed ingest and the pricing-aware `prune_duplicate_foils` are driven by `IngestPipeline` (`cli/ingest_pipeline.rs`), which owns both services, so `CardService` neither holds `PriceService` nor persists sealed products itself.

**Ingest pipeline**: The `ingest` command runs a full pipeline: ingest (sets → cards + sealed products in one `AllPrintings.json` pass → prices) → post-ingest prune (remove unwanted data) → post-ingest updates (set sizes, set prices, main set classification fixes). Cards + sealed share a single pass when both are requested (the default); a single `-c` or `--sealed` flag runs that one's standalone stream.

**Batch processing**: Card ingestion streams `AllPrintings.json` and flushes one batch per set (so a split card's faces stay together for the mana-cost merge), then saves each batch sequentially in bind-parameter-safe chunks of 500. The stream parser hands batches to the save closure one at a time, so batch saves are sequential - there is no concurrency layer. Repositories use SQLx `QueryBuilder` for bulk UPSERTs via `ON CONFLICT`.

### Database

Shares the same PostgreSQL database as the NestJS web app ([i-want-my-mtg](https://github.com/matthewdtowles/i-want-my-mtg)). Schema and migrations are managed in the web app repo. Core tables: `card`, `set`, `price`, `price_history`, `legality`, `set_price`, `granular_price` (current Card Kingdom buylist offer, one row per card+finish+vendor), `published_deck` / `published_deck_card` (tournament catalog), `portfolio_summary` / `portfolio_card_performance`.

`granular_price_history` no longer exists - the web repo dropped it in migration 042 (ROADMAP §10.10). The `retention` command still tries to prune it, which breaks the run; tracked in [#63](https://github.com/matthewdtowles/scry/issues/63).

Uses SQLx with the `runtime-tokio-rustls` feature. The `ConnectionPool` struct wraps `PgPool` and provides helper methods for common query patterns (count, execute, fetch).

### Card Filtering

Cards go through `should_filter()` and `merge_and_filter_cards()` during ingestion. Split cards are merged (combining mana costs from both faces). Foreign cards without prices are pruned post-ingest. The `MainSetClassifier` determines whether a card belongs to a set's "main" (base) subset.

### `is_main` Classification

Two independent concepts, both stored as boolean columns:

- **`card.in_main`** — Is this card in the booster's main slot (vs. bonus / showcase / variant) within its own set. Determined by `MainSetClassifier::is_main_set_card` in `src/card/domain/main_set_classifier.rs`. Reads MTGJSON's `boosterTypes`; when absent, falls back to intrinsic per-card signals (borderColor, frameEffects, availability) gated to booster-bearing set types (`expansion`, `core`, `draft_innovation`, `masters`, `funny`).

- **`set.is_main`** — Does this set appear in the default browse listing. Rule: `type IN ('expansion','core') AND code NOT IN BONUS_MAIN_SET_OVERRIDES`. The override list (`SetRepository::BONUS_MAIN_SET_OVERRIDES`) is the authoritative source for excluding bonus-sheet sets (BIG, TSB, MAT) that MTGJSON mistypes as `expansion`. Intentionally does NOT depend on `parent_code` — block-children (Dark Ascension, Eldritch Moon, …) get a parent_code from scry's canonical-parent normalization but are full main expansions and stay `is_main=true`. The rule is order-independent w.r.t. `update_parent_codes`.

To add a newly-discovered bonus sheet to the exclusion list, append its lowercase code to `BONUS_MAIN_SET_OVERRIDES` with a comment explaining what it is. Tests in `tests/set_repository_test.rs` cover the rule's behavior and order-independence.
