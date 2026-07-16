mod common;

use rust_decimal::Decimal;
use std::sync::Arc;

use chrono::NaiveDate;
use scry::card::repository::CardRepository;
use scry::card::service::CardService;
use scry::price::domain::Price;
use scry::price::repository::PriceRepository;
use scry::price::service::PriceService;
use scry::set::repository::SetRepository;
use scry::utils::http_client::HttpClient;

/// Characterizes `prune_duplicate_foils`: a non-ASCII-numbered printing (e.g. a
/// starred foil variant) is folded into its ASCII sibling of the same name -
/// the sibling gains foil availability + the foil price, and the variant row is
/// deleted. Uses `stx`, one of the dup-foil sets the method scans.
#[tokio::test]
#[ignore]
async fn test_prune_duplicate_foils_folds_variant_into_ascii_sibling() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("stx")])
        .await
        .unwrap();

    // ASCII sibling: no foil yet, no price.
    let mut ascii = common::create_test_card("stx-1", "stx");
    ascii.name = "Prune Bloodchief".to_string();
    ascii.number = "1".to_string();
    ascii.sort_number = "000001".to_string();
    ascii.has_foil = false;
    ascii.has_non_foil = true;

    // Non-ASCII foil variant of the same name, with a foil price to copy over.
    let mut variant = common::create_test_card("stx-star1", "stx");
    variant.name = "Prune Bloodchief".to_string();
    variant.number = "★1".to_string();
    variant.sort_number = "800001".to_string();
    variant.has_foil = true;
    variant.has_non_foil = false;

    card_repo.save_cards(&[ascii, variant]).await.unwrap();
    price_repo
        .save_prices(&[Price {
            card_id: "stx-star1".to_string(),
            normal: Some(Decimal::try_from(2.0).unwrap()),
            foil: Some(Decimal::try_from(5.0).unwrap()),
            date: NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        }])
        .await
        .unwrap();

    let http = Arc::new(HttpClient::new());
    let card_service = CardService::new(db.clone(), http.clone());
    let price_service = PriceService::new(db.clone(), http);

    let deleted = card_service
        .prune_duplicate_foils(&price_service)
        .await
        .unwrap();

    // The one variant row was removed.
    assert_eq!(deleted, 1);
    let stx_count = card_repo
        .count_for_sets(false)
        .await
        .unwrap()
        .into_iter()
        .find(|(code, _)| code == "stx")
        .map(|(_, n)| n)
        .unwrap_or(0);
    assert_eq!(stx_count, 1, "only the ASCII sibling should remain");

    // The ASCII sibling gained foil availability.
    let survivors = card_repo
        .fetch_ascii_cards_by_set_and_names("stx", &["Prune Bloodchief".to_string()])
        .await
        .unwrap();
    assert_eq!(survivors.len(), 1);
    assert!(survivors[0].has_foil, "ASCII sibling should now allow foil");

    // The foil price was copied onto the ASCII sibling.
    let prices = price_service
        .fetch_prices_for_card_ids(&["stx-1".to_string()])
        .await
        .unwrap();
    let (_, foil) = prices
        .get("stx-1")
        .expect("ASCII sibling should have a price");
    assert_eq!(*foil, Some(Decimal::try_from(5.0).unwrap()));
}
