mod common;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use scry::card::repository::CardRepository;
use scry::database::ConnectionPool;
use scry::portfolio::domain::PortfolioValueSnapshot;
use scry::portfolio::repository::PortfolioRepository;
use scry::set::repository::SetRepository;
use std::sync::Arc;

async fn seed_card(db: &Arc<ConnectionPool>, set_code: &str, card_id: &str) {
    SetRepository::new(db.clone())
        .save_sets(&[common::create_test_set(set_code)])
        .await
        .unwrap();
    CardRepository::new(db.clone())
        .save_cards(&[common::create_test_card(card_id, set_code)])
        .await
        .unwrap();
}

// The inserts below are idempotent (ON CONFLICT / clear-then-insert) so a test
// re-run against a persistent dev DB behaves like the fresh CI database.

/// `foil` and `normal` are SQL literals ("5.00" or "NULL").
async fn insert_price(db: &Arc<ConnectionPool>, card_id: &str, normal: &str, foil: &str) {
    db.execute_raw(&format!(
        "INSERT INTO price (card_id, normal, foil, date) \
         VALUES ('{card_id}', {normal}, {foil}, '2025-01-01') \
         ON CONFLICT (card_id, date) DO UPDATE SET normal = EXCLUDED.normal, foil = EXCLUDED.foil"
    ))
    .await
    .unwrap();
}

async fn insert_inventory(
    db: &Arc<ConnectionPool>,
    card_id: &str,
    user_id: i32,
    foil: bool,
    qty: i32,
) {
    db.execute_raw(&format!(
        "INSERT INTO inventory (card_id, user_id, foil, quantity) \
         VALUES ('{card_id}', {user_id}, {foil}, {qty}) \
         ON CONFLICT (card_id, user_id, foil) DO UPDATE SET quantity = EXCLUDED.quantity"
    ))
    .await
    .unwrap();
}

/// Transactions have no natural key, so clear this user's rows first to keep
/// the test idempotent on re-run.
async fn reset_txns(db: &Arc<ConnectionPool>, user_id: i32) {
    db.execute_raw(&format!(
        "DELETE FROM \"transaction\" WHERE user_id = {user_id}"
    ))
    .await
    .unwrap();
}

async fn insert_txn(
    db: &Arc<ConnectionPool>,
    user_id: i32,
    card_id: &str,
    kind: &str,
    qty: i32,
    price_per_unit: &str,
    is_foil: bool,
) {
    db.execute_raw(&format!(
        "INSERT INTO \"transaction\" (user_id, card_id, type, quantity, price_per_unit, is_foil, date) \
         VALUES ({user_id}, '{card_id}', '{kind}', {qty}, {price_per_unit}, {is_foil}, '2025-01-01')"
    ))
    .await
    .unwrap();
}

// --- calculate_portfolio_summaries ---

#[tokio::test]
#[ignore]
async fn summaries_include_unpriced_cards_at_zero_value() {
    // Regression guard for the LEFT JOIN fix: an unpriced card contributes 0 to
    // total_value but still counts toward total_cards + total_quantity.
    let db = common::setup_test_db().await;
    seed_card(&db, "pf1", "pf1-priced").await;
    seed_card(&db, "pf1", "pf1-unpriced").await;
    insert_price(&db, "pf1-priced", "5.00", "NULL").await;
    let user = 90001;
    insert_inventory(&db, "pf1-priced", user, false, 3).await;
    insert_inventory(&db, "pf1-unpriced", user, false, 2).await;

    let repo = PortfolioRepository::new(db.clone());
    let rows = repo.calculate_portfolio_summaries().await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.user_id == user)
        .expect("summary row");

    assert_eq!(row.total_value, Decimal::new(1500, 2)); // 3 * 5.00 + unpriced * 0
    assert_eq!(row.total_cards, 2);
    assert_eq!(row.total_quantity, 5);
}

#[tokio::test]
#[ignore]
async fn summaries_use_the_foil_price_for_foil_rows() {
    let db = common::setup_test_db().await;
    seed_card(&db, "pf2", "pf2-c").await;
    insert_price(&db, "pf2-c", "3.00", "10.00").await;
    let user = 90002;
    insert_inventory(&db, "pf2-c", user, true, 2).await;

    let repo = PortfolioRepository::new(db.clone());
    let rows = repo.calculate_portfolio_summaries().await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.user_id == user)
        .expect("summary row");

    assert_eq!(row.total_value, Decimal::new(2000, 2)); // 2 * foil 10.00, not normal 3.00
}

// --- calculate_card_performance ---

#[tokio::test]
#[ignore]
async fn card_performance_computes_cost_and_unrealized_gain_for_a_buy() {
    let db = common::setup_test_db().await;
    seed_card(&db, "pf3", "pf3-c").await;
    insert_price(&db, "pf3-c", "5.00", "NULL").await;
    let user = 90003;
    reset_txns(&db, user).await;
    insert_txn(&db, user, "pf3-c", "BUY", 4, "2.00", false).await;

    let repo = PortfolioRepository::new(db.clone());
    let rows = repo.calculate_card_performance().await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.user_id == user && r.card_id == "pf3-c")
        .expect("performance row");

    assert_eq!(row.quantity, 4);
    assert_eq!(row.total_cost, Decimal::new(800, 2)); // 4 @ 2.00
    assert_eq!(row.average_cost, Decimal::new(200, 2));
    assert_eq!(row.current_value, Decimal::new(2000, 2)); // 4 @ 5.00
    assert_eq!(row.unrealized_gain, Decimal::new(1200, 2)); // 4 * (5 - 2)
    assert!(row.realized_gain.is_zero());
    assert_eq!(row.roi_percent, Some(Decimal::new(15000, 2))); // (20 - 8) / 8 * 100
}

#[tokio::test]
#[ignore]
async fn card_performance_realized_gain_after_a_partial_sell() {
    let db = common::setup_test_db().await;
    seed_card(&db, "pf4", "pf4-c").await;
    insert_price(&db, "pf4-c", "5.00", "NULL").await;
    let user = 90004;
    reset_txns(&db, user).await;
    insert_txn(&db, user, "pf4-c", "BUY", 4, "2.00", false).await;
    insert_txn(&db, user, "pf4-c", "SELL", 1, "5.00", false).await;

    let repo = PortfolioRepository::new(db.clone());
    let rows = repo.calculate_card_performance().await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.user_id == user && r.card_id == "pf4-c")
        .expect("performance row");

    assert_eq!(row.quantity, 3); // 4 bought - 1 sold
    assert_eq!(row.current_value, Decimal::new(1500, 2)); // 3 @ 5.00
    assert_eq!(row.realized_gain, Decimal::new(300, 2)); // 5.00 revenue - cost basis 2.00
}

// --- save_snapshots ---

#[tokio::test]
#[ignore]
async fn save_snapshots_upserts_on_user_and_date() {
    let db = common::setup_test_db().await;
    let repo = PortfolioRepository::new(db.clone());
    let user = 90005;
    let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let snap = PortfolioValueSnapshot {
        user_id: user,
        total_value: Decimal::new(1000, 2),
        total_cost: None,
        total_cards: 2,
        date,
    };

    assert_eq!(
        repo.save_snapshots(std::slice::from_ref(&snap))
            .await
            .unwrap(),
        1
    );

    // Same user + date, new value: upsert, not a second row.
    let mut updated = snap.clone();
    updated.total_value = Decimal::new(2000, 2);
    repo.save_snapshots(&[updated]).await.unwrap();

    let count = db
        .count(&format!(
            "SELECT COUNT(*) FROM portfolio_value_history WHERE user_id = {user}"
        ))
        .await
        .unwrap();
    assert_eq!(count, 1);
}
