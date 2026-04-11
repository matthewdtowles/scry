-- Schema for scry integration tests
-- Derived from i-want-my-mtg database schema (only tables scry uses)

-- Use advisory lock to prevent concurrent schema creation races
SELECT pg_advisory_lock(42);

-- Enum types (use DO blocks to handle pre-existing types)
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
    img_src VARCHAR(255) NOT NULL,
    in_main BOOLEAN NOT NULL DEFAULT true,
    is_alternative BOOLEAN NOT NULL DEFAULT false,
    is_reserved BOOLEAN NOT NULL DEFAULT false,
    mana_cost VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    number VARCHAR(20) NOT NULL,
    oracle_text TEXT,
    purchase_url_tcgplayer VARCHAR(512),
    purchase_url_tcgplayer_etched VARCHAR(512),
    rarity card_rarity_enum NOT NULL DEFAULT 'common',
    set_code VARCHAR(10) NOT NULL REFERENCES "set"(code),
    sort_number VARCHAR(20) NOT NULL,
    "type" VARCHAR(255) NOT NULL,
    layout VARCHAR(50) NOT NULL DEFAULT 'normal'
);

-- Legality table
CREATE TABLE IF NOT EXISTS legality (
    id SERIAL PRIMARY KEY,
    card_id VARCHAR(36) NOT NULL REFERENCES card(id),
    format format_enum NOT NULL,
    status legality_status_enum NOT NULL,
    UNIQUE(card_id, format)
);

-- Price table
CREATE TABLE IF NOT EXISTS price (
    id SERIAL PRIMARY KEY,
    card_id VARCHAR(36) NOT NULL REFERENCES card(id),
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
    card_id VARCHAR(36) NOT NULL REFERENCES card(id),
    foil NUMERIC(10,2),
    normal NUMERIC(10,2),
    date DATE NOT NULL,
    UNIQUE(card_id, date)
);

-- Set price table
CREATE TABLE IF NOT EXISTS set_price (
    id SERIAL PRIMARY KEY,
    set_code VARCHAR(10) NOT NULL REFERENCES "set"(code) UNIQUE,
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
    set_code VARCHAR(10) NOT NULL REFERENCES "set"(code),
    base_price NUMERIC(10,2),
    total_price NUMERIC(10,2),
    base_price_all NUMERIC(10,2),
    total_price_all NUMERIC(10,2),
    date DATE NOT NULL,
    UNIQUE(set_code, date)
);

-- Inventory table
CREATE TABLE IF NOT EXISTS inventory (
    id SERIAL PRIMARY KEY,
    card_id VARCHAR(36) NOT NULL REFERENCES card(id),
    quantity INTEGER NOT NULL DEFAULT 0,
    foil_quantity INTEGER NOT NULL DEFAULT 0
);

-- Portfolio value history table
CREATE TABLE IF NOT EXISTS portfolio_value_history (
    id SERIAL PRIMARY KEY,
    total_value NUMERIC(12,2) NOT NULL,
    date DATE NOT NULL UNIQUE
);

SELECT pg_advisory_unlock(42);
