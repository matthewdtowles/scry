-- Schema for scry integration tests
-- Derived from i-want-my-mtg database schema (only tables scry uses)

-- Use advisory lock to prevent concurrent schema creation races
SELECT pg_advisory_lock(42);

-- Enum types (use DO blocks to handle pre-existing types).
--
-- We catch BOTH duplicate_object (42710) and unique_violation (23505). The
-- advisory lock above narrows the race window but does not eliminate it:
-- each DO block commits autonomously, so when parallel test sessions race
-- on CREATE TYPE, Postgres may raise either error code depending on whether
-- the conflict is detected via the type lookup or via the underlying
-- pg_type_typname_nsp_index uniqueness check. Catching only one of the two
-- causes CI to fail intermittently — keep both branches.
DO $$ BEGIN
    CREATE TYPE card_rarity_enum AS ENUM ('common', 'uncommon', 'rare', 'mythic', 'bonus', 'special');
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE format_enum AS ENUM (
        'standard', 'commander', 'modern', 'legacy', 'vintage',
        'brawl', 'explorer', 'historic', 'oathbreaker', 'pauper', 'pioneer'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE legality_status_enum AS ENUM ('legal', 'banned', 'restricted');
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;

-- Set table
CREATE TABLE IF NOT EXISTS "set" (
    code VARCHAR(10) PRIMARY KEY,
    base_size INTEGER NOT NULL DEFAULT 0,
    block VARCHAR(255),
    keyrune_code VARCHAR(10) NOT NULL,
    name VARCHAR(255) NOT NULL,
    parent_code VARCHAR(10),
    release_date DATE NOT NULL,
    "type" VARCHAR(50) NOT NULL,
    total_size INTEGER NOT NULL DEFAULT 0,
    is_main BOOLEAN NOT NULL DEFAULT true
);

-- Card table
CREATE TABLE IF NOT EXISTS card (
    id VARCHAR(36) PRIMARY KEY,
    artist VARCHAR(255),
    flavor_name VARCHAR(255),
    has_foil BOOLEAN NOT NULL DEFAULT false,
    has_non_foil BOOLEAN NOT NULL DEFAULT false,
    -- nullable: scry no longer writes img_src (the web derives it from
    -- scryfall_id). Mirrors prod after migration 037; the column is dropped
    -- entirely once that migration is everywhere.
    img_src VARCHAR(255),
    in_main BOOLEAN NOT NULL DEFAULT true,
    is_alternative BOOLEAN NOT NULL DEFAULT false,
    is_reserved BOOLEAN NOT NULL DEFAULT false,
    -- Printing language; defaults to 'English' so rows saved before the
    -- column existed stay non-foreign until re-ingested. Mirrors the web
    -- app migration that adds this column in prod.
    language VARCHAR(32) NOT NULL DEFAULT 'English',
    mana_cost VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    number VARCHAR(20) NOT NULL,
    oracle_text TEXT,
    tcgplayer_product_id VARCHAR(32),
    tcgplayer_etched_product_id VARCHAR(32),
    rarity card_rarity_enum NOT NULL DEFAULT 'common',
    set_code VARCHAR(10) NOT NULL REFERENCES "set"(code),
    sort_number VARCHAR(20) NOT NULL,
    "type" VARCHAR(255) NOT NULL,
    layout VARCHAR(50) NOT NULL DEFAULT 'normal',
    scryfall_id VARCHAR(36),
    -- nullable: rows not yet re-ingested read as NULL. Mirrors the prod
    -- column added by the web app migration.
    colors TEXT[]
);

-- The CREATE above is IF NOT EXISTS, so also add the column to any
-- pre-existing card table (same shape the prod migration takes).
ALTER TABLE card ADD COLUMN IF NOT EXISTS language VARCHAR(32) NOT NULL DEFAULT 'English';

CREATE UNIQUE INDEX IF NOT EXISTS idx_card_scryfall_id ON card (scryfall_id);

-- Legality table
CREATE TABLE IF NOT EXISTS legality (
    id SERIAL PRIMARY KEY,
    card_id VARCHAR(36) NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    format format_enum NOT NULL,
    status legality_status_enum NOT NULL,
    UNIQUE(card_id, format)
);

-- Price table
CREATE TABLE IF NOT EXISTS price (
    id SERIAL PRIMARY KEY,
    card_id VARCHAR(36) NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    foil NUMERIC(10,2),
    normal NUMERIC(10,2),
    date DATE NOT NULL,
    normal_change_weekly NUMERIC(10,2),
    foil_change_weekly NUMERIC(10,2),
    UNIQUE(card_id, date)
);

-- Price history table
CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    card_id VARCHAR(36) NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    foil NUMERIC(10,2),
    normal NUMERIC(10,2),
    date DATE NOT NULL,
    UNIQUE(card_id, date)
);

-- Granular price tables (web owns the real schema; this mirrors it for tests).
-- Current: one row per series (no date in the PK) = the current per-vendor offer.
CREATE TABLE IF NOT EXISTS granular_price (
    card_id VARCHAR(36) NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    provider VARCHAR NOT NULL,
    price_type VARCHAR NOT NULL,
    finish VARCHAR NOT NULL,
    condition VARCHAR NOT NULL DEFAULT 'NM',
    date DATE NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    qty INTEGER,
    PRIMARY KEY (card_id, provider, price_type, finish, condition)
);

-- History: dated series (date in the PK), retention-bounded.
CREATE TABLE IF NOT EXISTS granular_price_history (
    card_id VARCHAR(36) NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    provider VARCHAR NOT NULL,
    price_type VARCHAR NOT NULL,
    finish VARCHAR NOT NULL,
    condition VARCHAR NOT NULL DEFAULT 'NM',
    date DATE NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    qty INTEGER,
    PRIMARY KEY (card_id, provider, price_type, finish, condition, date)
);

-- Set price table
CREATE TABLE IF NOT EXISTS set_price (
    id SERIAL PRIMARY KEY,
    set_code VARCHAR(10) NOT NULL UNIQUE REFERENCES "set"(code) ON DELETE CASCADE,
    base_price NUMERIC(10,2),
    total_price NUMERIC(10,2),
    base_price_all NUMERIC(10,2),
    total_price_all NUMERIC(10,2),
    date DATE,
    base_price_change_weekly NUMERIC(10,2),
    total_price_change_weekly NUMERIC(10,2),
    base_price_all_change_weekly NUMERIC(10,2),
    total_price_all_change_weekly NUMERIC(10,2)
);

-- Set price history table
CREATE TABLE IF NOT EXISTS set_price_history (
    id SERIAL PRIMARY KEY,
    set_code VARCHAR(10) NOT NULL REFERENCES "set"(code) ON DELETE CASCADE,
    base_price NUMERIC(10,2),
    total_price NUMERIC(10,2),
    base_price_all NUMERIC(10,2),
    total_price_all NUMERIC(10,2),
    date DATE NOT NULL,
    UNIQUE(set_code, date)
);

-- Inventory table (mirrors the web schema: one row per card+finish, keyed by
-- (card_id, user_id, foil); user FK omitted - no users table in the fixture).
-- The portfolio queries read i.foil + i.quantity and GROUP BY i.user_id.
CREATE TABLE IF NOT EXISTS inventory (
    card_id VARCHAR(36) NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL,
    foil BOOLEAN NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (card_id, user_id, foil)
);

-- Portfolio value history table (mirrors web migration; user FK omitted - no
-- users table in the fixture)
CREATE TABLE IF NOT EXISTS portfolio_value_history (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id INTEGER NOT NULL,
    total_value NUMERIC(12,2) NOT NULL,
    total_cost NUMERIC(12,2),
    total_cards INTEGER NOT NULL,
    date DATE NOT NULL,
    CONSTRAINT uq_portfolio_value_history_user_date UNIQUE (user_id, date)
);

-- Transaction ledger (buys/sells) - source for card-performance FIFO math
CREATE TABLE IF NOT EXISTS "transaction" (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    card_id VARCHAR(36) NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    type VARCHAR NOT NULL,
    quantity INTEGER NOT NULL,
    price_per_unit NUMERIC(10,2) NOT NULL,
    is_foil BOOLEAN NOT NULL,
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    source VARCHAR,
    fees NUMERIC(10,2),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-user portfolio summary (one row per user)
CREATE TABLE IF NOT EXISTS portfolio_summary (
    user_id INTEGER PRIMARY KEY,
    total_value NUMERIC(12,2) NOT NULL,
    total_cost NUMERIC(12,2),
    total_realized_gain NUMERIC(12,2),
    total_cards INTEGER NOT NULL,
    total_quantity INTEGER NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refreshes_today INTEGER NOT NULL DEFAULT 0,
    last_refresh_date DATE NOT NULL DEFAULT CURRENT_DATE,
    computation_method VARCHAR(10) NOT NULL DEFAULT 'average'
);

-- Per-user, per-card performance rows
CREATE TABLE IF NOT EXISTS portfolio_card_performance (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id INTEGER NOT NULL,
    card_id VARCHAR NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    is_foil BOOLEAN NOT NULL,
    quantity INTEGER NOT NULL,
    total_cost NUMERIC(10,2) NOT NULL,
    average_cost NUMERIC(10,2) NOT NULL,
    current_value NUMERIC(10,2) NOT NULL,
    unrealized_gain NUMERIC(10,2) NOT NULL,
    realized_gain NUMERIC(10,2) NOT NULL,
    roi_percent NUMERIC(8,2),
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_portfolio_card_performance UNIQUE (user_id, card_id, is_foil)
);

-- Read-only tournament-deck catalog (fbettega feed)
CREATE TABLE IF NOT EXISTS published_deck (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source VARCHAR NOT NULL,
    source_uri VARCHAR NOT NULL,
    tournament_name VARCHAR,
    tournament_date DATE,
    format VARCHAR,
    archetype VARCHAR,
    player VARCHAR,
    result VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_published_deck_source UNIQUE (source, source_uri)
);

CREATE TABLE IF NOT EXISTS published_deck_card (
    published_deck_id INTEGER NOT NULL REFERENCES published_deck(id) ON DELETE CASCADE,
    card_id VARCHAR NOT NULL REFERENCES card(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
    is_sideboard BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (published_deck_id, card_id, is_sideboard)
);

-- Sealed product table
CREATE TABLE IF NOT EXISTS sealed_product (
    uuid VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    set_code VARCHAR(10) NOT NULL REFERENCES "set"(code) ON DELETE CASCADE,
    category VARCHAR(64),
    subtype VARCHAR(64),
    card_count INTEGER,
    product_size INTEGER,
    release_date DATE,
    contents_summary TEXT,
    tcgplayer_product_id VARCHAR(32)
);

SELECT pg_advisory_unlock(42);
