mod common;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use scry::card::repository::CardRepository;
use scry::price::domain::Price;
use scry::price::repository::PriceRepository;
use scry::set::repository::SetRepository;

fn create_test_price(card_id: &str, normal: f64, foil: f64) -> Price {
    Price {
        id: None,
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
        id: None,
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
