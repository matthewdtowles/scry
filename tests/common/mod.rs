use scry::card::domain::{Card, CardRarity, Format, Legality, LegalityStatus};
use scry::config::Config;
use scry::database::ConnectionPool;
use scry::set::domain::Set;

use chrono::NaiveDate;
use std::sync::Arc;

/// Create a fresh test database connection, create schema, return pool.
///
/// Each test gets its own pool. Schema uses IF NOT EXISTS for idempotency.
/// Tests must use unique set/card codes to avoid conflicts with parallel tests.
pub async fn setup_test_db() -> Arc<ConnectionPool> {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("TEST_DATABASE_URL or DATABASE_URL must be set");

    let config = Config {
        database_url,
        max_pool_size: 5,
    };

    let pool = ConnectionPool::new(&config)
        .await
        .expect("Failed to connect to test database");

    let schema_sql = include_str!("../fixtures/schema.sql");
    pool.execute_raw(schema_sql)
        .await
        .expect("Failed to create schema");

    Arc::new(pool)
}

pub fn create_test_set(code: &str) -> Set {
    Set {
        code: code.to_string(),
        base_size: 0,
        block: None,
        is_foreign_only: false,
        is_main: true,
        is_online_only: false,
        keyrune_code: code.to_string(),
        name: format!("Test Set {}", code.to_uppercase()),
        parent_code: None,
        release_date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        set_type: "core".to_string(),
        total_size: 0,
    }
}

pub fn create_test_card(id: &str, set_code: &str) -> Card {
    Card {
        artist: Some("Test Artist".to_string()),
        has_foil: true,
        has_non_foil: true,
        id: id.to_string(),
        img_src: format!("a/b/{}.jpg", id),
        in_main: true,
        is_alternative: false,
        is_reserved: false,
        is_online_only: false,
        is_oversized: false,
        language: "English".to_string(),
        layout: "normal".to_string(),
        legalities: vec![Legality::new(
            id.to_string(),
            Format::Standard,
            LegalityStatus::Legal,
        )],
        mana_cost: Some("{2}{U}".to_string()),
        name: format!("Test Card {}", id),
        number: "1".to_string(),
        oracle_text: Some("Test oracle text".to_string()),
        other_face_ids: None,
        rarity: CardRarity::Rare,
        set_code: set_code.to_string(),
        side: None,
        sort_number: "000001".to_string(),
        type_line: "Creature — Test".to_string(),
    }
}
