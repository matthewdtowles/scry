mod common;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use scry::card::repository::CardRepository;
use scry::database::ConnectionPool;
use scry::price::domain::{GranularPrice, Price};
use scry::price::repository::PriceRepository;
use scry::set::repository::SetRepository;
use sqlx::QueryBuilder;
use std::sync::Arc;

fn create_test_price(card_id: &str, normal: f64, foil: f64) -> Price {
    Price {
        card_id: card_id.to_string(),
        normal: Some(Decimal::try_from(normal).unwrap()),
        foil: Some(Decimal::try_from(foil).unwrap()),
        date: NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
    }
}

#[tokio::test]
#[ignore]
async fn test_save_and_count_prices() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("p01")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p01-c1", "p01")])
        .await
        .unwrap();

    let price = create_test_price("p01-c1", 1.50, 3.00);
    let saved = price_repo.save_prices(&[price]).await.unwrap();
    assert_eq!(saved, 1);

    // Verify via fetch
    let ids = vec!["p01-c1".to_string()];
    let map = price_repo.fetch_prices_for_card_ids(&ids).await.unwrap();
    assert!(map.contains_key("p01-c1"));
}

#[tokio::test]
#[ignore]
async fn test_save_prices_upsert() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("p02")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p02-c1", "p02")])
        .await
        .unwrap();

    let price1 = create_test_price("p02-c1", 1.50, 3.00);
    price_repo.save_prices(&[price1]).await.unwrap();

    // Same card+date — should upsert
    let price2 = create_test_price("p02-c1", 2.00, 4.00);
    let saved = price_repo.save_prices(&[price2]).await.unwrap();
    assert_eq!(saved, 1);
}

#[tokio::test]
#[ignore]
async fn test_save_price_history() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("p03")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p03-c1", "p03")])
        .await
        .unwrap();

    let price = create_test_price("p03-c1", 1.50, 3.00);
    let saved = price_repo.save_price_history(&[price]).await.unwrap();
    assert_eq!(saved, 1);

    let count = price_repo.price_history_count().await.unwrap();
    assert!(count >= 1);
}

#[tokio::test]
#[ignore]
async fn test_delete_by_date() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("p04")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p04-c1", "p04")])
        .await
        .unwrap();

    // Use a unique date so delete_by_date doesn't affect other tests
    let price = Price {
        card_id: "p04-c1".to_string(),
        normal: Some(rust_decimal::Decimal::try_from(1.50).unwrap()),
        foil: Some(rust_decimal::Decimal::try_from(3.00).unwrap()),
        date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
    };
    price_repo.save_prices(&[price]).await.unwrap();

    let date = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let deleted = price_repo.delete_by_date(date).await.unwrap();
    assert!(deleted >= 1);
}

#[tokio::test]
#[ignore]
async fn test_fetch_prices_for_card_ids() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("p05")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p05-c1", "p05")])
        .await
        .unwrap();

    let price = create_test_price("p05-c1", 1.50, 3.00);
    price_repo.save_prices(&[price]).await.unwrap();

    let ids = vec!["p05-c1".to_string()];
    let map = price_repo.fetch_prices_for_card_ids(&ids).await.unwrap();
    assert!(map.contains_key("p05-c1"));
    let (normal, foil) = map.get("p05-c1").unwrap();
    assert!(normal.is_some());
    assert!(foil.is_some());
}

fn create_test_granular(card_id: &str, price: Decimal) -> GranularPrice {
    GranularPrice {
        card_id: card_id.to_string(),
        provider: "cardkingdom".to_string(),
        price_type: "buylist".to_string(),
        finish: "normal".to_string(),
        condition: "NM".to_string(),
        date: NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        price,
        qty: None,
    }
}

async fn fetch_granular(db: &Arc<ConnectionPool>, card_id: &str) -> Decimal {
    let mut qb = QueryBuilder::new("SELECT price FROM granular_price WHERE card_id = ");
    qb.push_bind(card_id.to_string());
    let rows: Vec<(Decimal,)> = db.fetch_all_query_builder(qb).await.unwrap();
    rows.into_iter()
        .next()
        .expect("granular row should exist")
        .0
}

#[tokio::test]
#[ignore]
async fn test_save_granular_prices_upsert_overwrites_price() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("p10")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p10-c1", "p10")])
        .await
        .unwrap();

    let n = price_repo
        .save_granular_prices(&[create_test_granular("p10-c1", Decimal::new(350, 2))])
        .await
        .unwrap();
    assert_eq!(n, 1);

    // Same series, same date: the current-table upsert overwrites the price.
    price_repo
        .save_granular_prices(&[create_test_granular("p10-c1", Decimal::new(375, 2))])
        .await
        .unwrap();
    assert_eq!(fetch_granular(&db, "p10-c1").await, Decimal::new(375, 2));
}

async fn fetch_granular_qty(db: &Arc<ConnectionPool>, card_id: &str) -> Option<i32> {
    let mut qb = QueryBuilder::new("SELECT qty FROM granular_price WHERE card_id = ");
    qb.push_bind(card_id.to_string());
    let rows: Vec<(Option<i32>,)> = db.fetch_all_query_builder(qb).await.unwrap();
    rows.into_iter()
        .next()
        .expect("granular row should exist")
        .0
}

#[tokio::test]
#[ignore]
async fn test_save_granular_prices_qty_last_writer_wins() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("p13")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p13-c1", "p13")])
        .await
        .unwrap();

    // CK-direct row carries a live buy quantity.
    let mut with_qty = create_test_granular("p13-c1", Decimal::new(350, 2));
    with_qty.qty = Some(16);
    price_repo.save_granular_prices(&[with_qty]).await.unwrap();
    assert_eq!(fetch_granular_qty(&db, "p13-c1").await, Some(16));

    // A later write without qty (e.g. MTGJSON) nulls it out -- last writer
    // wins; a stale quantity must not survive under a fresher price.
    let without_qty = create_test_granular("p13-c1", Decimal::new(375, 2));
    price_repo
        .save_granular_prices(&[without_qty])
        .await
        .unwrap();
    assert_eq!(fetch_granular_qty(&db, "p13-c1").await, None);
    assert_eq!(fetch_granular(&db, "p13-c1").await, Decimal::new(375, 2));
}

#[tokio::test]
#[ignore]
async fn test_save_granular_prices_current_ignores_older_date() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("p12")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p12-c1", "p12")])
        .await
        .unwrap();

    let newer = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let older = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();

    price_repo
        .save_granular_prices(&[granular_at("p12-c1", newer, Decimal::new(500, 2))])
        .await
        .unwrap();
    // The date guard rejects an older-dated row, so the current offer holds.
    price_repo
        .save_granular_prices(&[granular_at("p12-c1", older, Decimal::new(100, 2))])
        .await
        .unwrap();
    assert_eq!(fetch_granular(&db, "p12-c1").await, Decimal::new(500, 2));
}

fn granular_at(card_id: &str, date: NaiveDate, price: Decimal) -> GranularPrice {
    GranularPrice {
        card_id: card_id.to_string(),
        provider: "cardkingdom".to_string(),
        price_type: "buylist".to_string(),
        finish: "normal".to_string(),
        condition: "NM".to_string(),
        date,
        price,
        qty: None,
    }
}

#[tokio::test]
#[ignore]
async fn test_fetch_scryfall_card_id_map() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("p14")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("p14-c1", "p14")])
        .await
        .unwrap();

    let map = price_repo.fetch_scryfall_card_id_map().await.unwrap();
    // create_test_card sets scryfall_id = "scryfall-{id}"
    assert_eq!(
        map.get("scryfall-p14-c1").map(String::as_str),
        Some("p14-c1")
    );
}

/// The Black Lotus case: a card priced on some days and simply absent from
/// MTGJSON's build on others, so its price blinks out and back.
///
/// Asserts the three things that make the carry-forward honest rather than a
/// fabrication:
///  - a card with no row today gets one, from its newest history,
///  - that row keeps the date it was really observed on, not today's, so the
///    staleness is still legible without an extra column,
///  - a card that already has a price today is left completely alone.
#[tokio::test]
#[ignore]
async fn test_carry_forward_fills_only_missing_prices_and_keeps_the_observed_date() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("cf1")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[
            common::create_test_card("cf1-gone", "cf1"),
            common::create_test_card("cf1-priced", "cf1"),
            common::create_test_card("cf1-ancient", "cf1"),
        ])
        .await
        .unwrap();

    let today = chrono::Utc::now().date_naive();
    let observed = today - chrono::Duration::days(3);
    let too_old = today - chrono::Duration::days(90);

    // History: a recent observation for the flickering card, a stale one for a
    // card that stopped trading months ago, and a newer-but-superseded row to
    // prove DISTINCT ON picks the newest.
    price_repo
        .save_price_history(&[
            Price {
                card_id: "cf1-gone".into(),
                normal: Some(Decimal::try_from(32999.99).unwrap()),
                foil: None,
                date: observed,
            },
            Price {
                card_id: "cf1-gone".into(),
                normal: Some(Decimal::try_from(6500.0).unwrap()),
                foil: None,
                date: today - chrono::Duration::days(5),
            },
            Price {
                card_id: "cf1-ancient".into(),
                normal: Some(Decimal::try_from(10.0).unwrap()),
                foil: None,
                date: too_old,
            },
            // The already-priced card needs history too, or the NOT EXISTS
            // guard is never actually exercised: with no history row there is
            // nothing for the insert to have wrongly carried, and dropping the
            // guard would look harmless.
            Price {
                card_id: "cf1-priced".into(),
                normal: Some(Decimal::try_from(999.0).unwrap()),
                foil: None,
                date: observed,
            },
        ])
        .await
        .unwrap();

    // Today's build mentioned only `cf1-priced`.
    price_repo
        .save_prices(&[Price {
            card_id: "cf1-priced".into(),
            normal: Some(Decimal::try_from(1.25).unwrap()),
            foil: None,
            date: today,
        }])
        .await
        .unwrap();

    // Deliberately not asserting the returned count: carry-forward operates on
    // the whole table, so any other test that leaves a card with history and no
    // price would change it. The scoped row assertions below are what matter,
    // and they are strictly stronger - dropping the NOT EXISTS guard or widening
    // the age cutoff each add a third `cf1-` row and fail on the count below.
    price_repo.carry_forward_missing_prices(30).await.unwrap();

    let qb = QueryBuilder::new(
        "SELECT card_id, normal, date FROM price WHERE card_id LIKE 'cf1-%' ORDER BY card_id",
    );
    let rows: Vec<(String, Option<Decimal>, NaiveDate)> =
        db.fetch_all_query_builder(qb).await.unwrap();

    assert_eq!(
        rows.len(),
        2,
        "expected exactly the carried row and today's row: the 90-day-old card must \
         stay unpriced, and the already-priced card must not gain a second, older row"
    );

    let gone = rows
        .iter()
        .find(|r| r.0 == "cf1-gone")
        .expect("carried row");
    assert_eq!(gone.1, Some(Decimal::try_from(32999.99).unwrap()));
    assert_eq!(
        gone.2, observed,
        "the carried row must keep the date it was observed, not today's"
    );

    let priced = rows
        .iter()
        .find(|r| r.0 == "cf1-priced")
        .expect("today's row");
    assert_eq!(priced.1, Some(Decimal::try_from(1.25).unwrap()));
    assert_eq!(priced.2, today, "a card priced today must be untouched");
}
