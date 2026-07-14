mod common;

use chrono::NaiveDate;
use scry::card::repository::CardRepository;
use scry::database::ConnectionPool;
use scry::published_deck::domain::{RawDeck, ResolvedCard};
use scry::published_deck::repository::PublishedDeckRepository;
use scry::set::repository::SetRepository;
use std::sync::Arc;

fn raw_deck(source_uri: &str, tournament_date: Option<NaiveDate>) -> RawDeck {
    RawDeck {
        source: "fbettega".to_string(),
        source_uri: source_uri.to_string(),
        tournament_name: Some("Test Cup".to_string()),
        tournament_date,
        format: Some("modern".to_string()),
        player: Some("Tester".to_string()),
        result: Some("1st".to_string()),
        lines: Vec::new(), // save_deck persists the pre-resolved `cards` arg, not lines
    }
}

async fn seed_named_card(db: &Arc<ConnectionPool>, set_code: &str, card_id: &str, name: &str) {
    SetRepository::new(db.clone())
        .save_sets(&[common::create_test_set(set_code)])
        .await
        .unwrap();
    let mut card = common::create_test_card(card_id, set_code);
    card.name = name.to_string();
    CardRepository::new(db.clone())
        .save_cards(&[card])
        .await
        .unwrap();
}

/// Count child rows for a specific deck (parallel-test safe).
async fn card_count_for(db: &Arc<ConnectionPool>, source_uri: &str) -> i64 {
    db.count(&format!(
        "SELECT COUNT(*) FROM published_deck_card pdc \
         JOIN published_deck pd ON pd.id = pdc.published_deck_id \
         WHERE pd.source_uri = '{source_uri}'"
    ))
    .await
    .unwrap()
}

async fn deck_exists(db: &Arc<ConnectionPool>, source_uri: &str) -> i64 {
    db.count(&format!(
        "SELECT COUNT(*) FROM published_deck WHERE source_uri = '{source_uri}'"
    ))
    .await
    .unwrap()
}

#[tokio::test]
#[ignore]
async fn save_deck_persists_the_deck_and_its_cards() {
    let db = common::setup_test_db().await;
    seed_named_card(&db, "pd1", "pd1-c1", "Card One").await;
    seed_named_card(&db, "pd1", "pd1-c2", "Card Two").await;
    let repo = PublishedDeckRepository::new(db.clone());

    let cards = vec![
        ResolvedCard {
            card_id: "pd1-c1".to_string(),
            quantity: 4,
            is_sideboard: false,
        },
        ResolvedCard {
            card_id: "pd1-c2".to_string(),
            quantity: 2,
            is_sideboard: true,
        },
    ];
    repo.save_deck(&raw_deck("uri-pd1", Some(day(2025, 6, 1))), &cards)
        .await
        .unwrap();

    assert_eq!(deck_exists(&db, "uri-pd1").await, 1);
    assert_eq!(card_count_for(&db, "uri-pd1").await, 2);
}

#[tokio::test]
#[ignore]
async fn save_deck_upserts_on_source_uri_and_replaces_cards() {
    let db = common::setup_test_db().await;
    seed_named_card(&db, "pd2", "pd2-c1", "First Card").await;
    seed_named_card(&db, "pd2", "pd2-c2", "Second Card").await;
    let repo = PublishedDeckRepository::new(db.clone());

    repo.save_deck(
        &raw_deck("uri-pd2", Some(day(2025, 6, 1))),
        &[ResolvedCard {
            card_id: "pd2-c1".to_string(),
            quantity: 4,
            is_sideboard: false,
        }],
    )
    .await
    .unwrap();

    // Re-ingest the same deck (same source_uri) with a different card list.
    repo.save_deck(
        &raw_deck("uri-pd2", Some(day(2025, 6, 2))),
        &[ResolvedCard {
            card_id: "pd2-c2".to_string(),
            quantity: 1,
            is_sideboard: false,
        }],
    )
    .await
    .unwrap();

    // One deck (upsert, not duplicate), and its children were replaced.
    assert_eq!(deck_exists(&db, "uri-pd2").await, 1);
    assert_eq!(card_count_for(&db, "uri-pd2").await, 1);
    let has_c1 = db
        .count("SELECT COUNT(*) FROM published_deck_card WHERE card_id = 'pd2-c1'")
        .await
        .unwrap();
    let has_c2 = db
        .count("SELECT COUNT(*) FROM published_deck_card WHERE card_id = 'pd2-c2'")
        .await
        .unwrap();
    assert_eq!(has_c1, 0, "old card row should be replaced");
    assert_eq!(has_c2, 1);
}

#[tokio::test]
#[ignore]
async fn resolve_card_ids_maps_lowercased_names_to_ids() {
    let db = common::setup_test_db().await;
    seed_named_card(&db, "pd3", "pd3-bolt", "Lightning Bolt").await;
    let repo = PublishedDeckRepository::new(db.clone());

    let map = repo
        .resolve_card_ids(&["lightning bolt".to_string()])
        .await
        .unwrap();

    assert_eq!(map.get("lightning bolt"), Some(&"pd3-bolt".to_string()));
}

#[tokio::test]
#[ignore]
async fn prune_older_than_deletes_stale_decks_and_keeps_recent_ones() {
    let db = common::setup_test_db().await;
    let repo = PublishedDeckRepository::new(db.clone());
    repo.save_deck(&raw_deck("uri-pd4-old", Some(day(2020, 1, 1))), &[])
        .await
        .unwrap();
    repo.save_deck(&raw_deck("uri-pd4-new", Some(day(2025, 6, 1))), &[])
        .await
        .unwrap();

    let deleted = repo.prune_older_than(day(2024, 1, 1)).await.unwrap();

    assert!(deleted >= 1);
    assert_eq!(deck_exists(&db, "uri-pd4-old").await, 0);
    assert_eq!(deck_exists(&db, "uri-pd4-new").await, 1);
}

fn day(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}
