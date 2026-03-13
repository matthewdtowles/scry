mod common;

use scry::set::repository::SetRepository;

#[tokio::test]
#[ignore]
async fn test_save_and_count_sets() {
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    let sets = vec![
        common::create_test_set("s01"),
        common::create_test_set("s02"),
    ];
    repo.save_sets(&sets).await.unwrap();

    // Verify sets exist via fetch_empty_sets (they have no cards)
    let empty = repo.fetch_empty_sets().await.unwrap();
    assert!(empty.iter().any(|s| s.code == "s01"));
    assert!(empty.iter().any(|s| s.code == "s02"));
}

#[tokio::test]
#[ignore]
async fn test_save_sets_upsert() {
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db);

    let mut set = common::create_test_set("s03");
    set.name = "Original Name".to_string();
    repo.save_sets(&[set.clone()]).await.unwrap();

    // Update name and save again — should upsert, not duplicate
    set.name = "Updated Name".to_string();
    let saved = repo.save_sets(&[set]).await.unwrap();
    assert_eq!(saved, 1); // 1 row affected by upsert
}

#[tokio::test]
#[ignore]
async fn test_fetch_empty_sets() {
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db);

    let set = common::create_test_set("s04");
    repo.save_sets(&[set]).await.unwrap();

    let empty = repo.fetch_empty_sets().await.unwrap();
    assert!(empty.iter().any(|s| s.code == "s04"));
}

#[tokio::test]
#[ignore]
async fn test_delete_set_batch() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());

    let set = common::create_test_set("s05");
    set_repo.save_sets(&[set]).await.unwrap();

    // Add a card so delete_set_batch exercises cascade
    let card_repo = scry::card::repository::CardRepository::new(db);
    let card = common::create_test_card("s05-c1", "s05");
    card_repo.save_cards(&[card]).await.unwrap();

    let deleted = set_repo.delete_set_batch("s05").await.unwrap();
    assert_eq!(deleted, 1);

    // Verify set is gone
    let empty = set_repo.fetch_empty_sets().await.unwrap();
    assert!(!empty.iter().any(|s| s.code == "s05"));
}
