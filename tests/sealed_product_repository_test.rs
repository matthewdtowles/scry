mod common;

use chrono::NaiveDate;
use scry::database::ConnectionPool;
use scry::sealed_product::domain::SealedProduct;
use scry::sealed_product::repository::SealedProductRepository;
use scry::set::repository::SetRepository;
use std::sync::Arc;

fn create_test_product(uuid: &str, set_code: &str) -> SealedProduct {
    SealedProduct {
        uuid: uuid.to_string(),
        name: format!("Test Product {}", uuid),
        set_code: set_code.to_string(),
        category: Some("booster_box".to_string()),
        subtype: Some("draft".to_string()),
        card_count: Some(540),
        product_size: Some(36),
        release_date: NaiveDate::from_ymd_opt(2024, 8, 2),
        contents_summary: Some("36x Draft Booster Pack".to_string()),
        purchase_url_tcgplayer: Some("https://tcgplayer.com/p/123".to_string()),
        tcgplayer_product_id: Some("541185".to_string()),
    }
}

async fn seed_set(db: Arc<ConnectionPool>, set_code: &str) {
    let set_repo = SetRepository::new(db);
    set_repo
        .save_sets(&[common::create_test_set(set_code)])
        .await
        .unwrap();
}

async fn count_for_set(db: &ConnectionPool, set_code: &str) -> i64 {
    // Scoped count so parallel tests don't interfere.
    db.count(&format!(
        "SELECT COUNT(*) FROM sealed_product WHERE set_code = '{}'",
        set_code
    ))
    .await
    .unwrap()
}

#[tokio::test]
#[ignore]
async fn test_save_and_count_sealed_products() {
    let db = common::setup_test_db().await;
    seed_set(db.clone(), "sp01").await;
    let repo = SealedProductRepository::new(db.clone());

    let products = vec![
        create_test_product("sp01-p1", "sp01"),
        create_test_product("sp01-p2", "sp01"),
    ];
    let saved = repo.save(&products).await.unwrap();
    assert_eq!(saved, 2);

    // Scoped to this test's set_code to avoid parallel-test pollution.
    assert_eq!(count_for_set(&db, "sp01").await, 2);

    // Repository's global count should include at least these two.
    assert!(repo.count().await.unwrap() >= 2);
}

#[tokio::test]
#[ignore]
async fn test_save_sealed_products_upsert() {
    let db = common::setup_test_db().await;
    seed_set(db.clone(), "sp02").await;
    let repo = SealedProductRepository::new(db.clone());

    let mut product = create_test_product("sp02-p1", "sp02");
    product.name = "Original Name".to_string();
    repo.save(&[product.clone()]).await.unwrap();
    assert_eq!(count_for_set(&db, "sp02").await, 1);

    // Update fields and re-save — should upsert, not duplicate.
    product.name = "Updated Name".to_string();
    product.card_count = Some(360);
    let saved = repo.save(&[product]).await.unwrap();
    assert_eq!(saved, 1); // 1 row affected by upsert

    // Row count for this set is unchanged — no new row.
    assert_eq!(count_for_set(&db, "sp02").await, 1);

    // Verify updated values persisted.
    let updated = db
        .count(
            "SELECT COUNT(*) FROM sealed_product \
             WHERE uuid = 'sp02-p1' AND name = 'Updated Name' AND card_count = 360",
        )
        .await
        .unwrap();
    assert_eq!(updated, 1);
}

#[tokio::test]
#[ignore]
async fn test_save_sealed_products_empty_returns_zero() {
    let db = common::setup_test_db().await;
    let repo = SealedProductRepository::new(db);

    let saved = repo.save(&[]).await.unwrap();
    assert_eq!(saved, 0);
}

#[tokio::test]
#[ignore]
async fn test_save_sealed_products_upsert_no_change_is_noop() {
    let db = common::setup_test_db().await;
    seed_set(db.clone(), "sp03").await;
    let repo = SealedProductRepository::new(db);

    let product = create_test_product("sp03-p1", "sp03");
    repo.save(&[product.clone()]).await.unwrap();

    // Re-saving identical row matches the WHERE DISTINCT FROM guard → 0 rows affected
    let saved = repo.save(&[product]).await.unwrap();
    assert_eq!(saved, 0);
}
