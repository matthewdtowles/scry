mod common;

use scry::card::repository::CardRepository;
use scry::set::repository::SetRepository;

#[tokio::test]
#[ignore]
async fn test_save_and_count_cards() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("c01")])
        .await
        .unwrap();

    let mut card1 = common::create_test_card("c01-1", "c01");
    card1.number = "1".to_string();
    card1.sort_number = "000001".to_string();
    let mut card2 = common::create_test_card("c01-2", "c01");
    card2.number = "2".to_string();
    card2.sort_number = "000002".to_string();

    card_repo.save_cards(&[card1, card2]).await.unwrap();

    // Verify via count_for_sets
    let counts = card_repo.count_for_sets(false).await.unwrap();
    let c01_count = counts.iter().find(|(code, _)| code == "c01").unwrap().1;
    assert_eq!(c01_count, 2);
}

#[tokio::test]
#[ignore]
async fn test_save_cards_upsert() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("c02")])
        .await
        .unwrap();

    let mut card = common::create_test_card("c02-1", "c02");
    card_repo.save_cards(&[card.clone()]).await.unwrap();

    // Update card name and re-save
    card.name = "Updated Card Name".to_string();
    let saved = card_repo.save_cards(&[card]).await.unwrap();
    assert_eq!(saved, 1); // 1 row affected by upsert

    let counts = card_repo.count_for_sets(false).await.unwrap();
    let c02_count = counts.iter().find(|(code, _)| code == "c02").unwrap().1;
    assert_eq!(c02_count, 1); // still only 1 card
}

#[tokio::test]
#[ignore]
async fn test_save_legalities() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("c03")])
        .await
        .unwrap();

    let card = common::create_test_card("c03-1", "c03");
    card_repo.save_cards(&[card.clone()]).await.unwrap();
    let saved = card_repo.save_legalities(&[card]).await.unwrap();
    assert!(saved > 0);

    let legality_count = card_repo.legality_count().await.unwrap();
    assert!(legality_count > 0);
}

#[tokio::test]
#[ignore]
async fn test_delete_cards_batch() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("c04")])
        .await
        .unwrap();

    let mut card1 = common::create_test_card("c04-1", "c04");
    card1.number = "1".to_string();
    let mut card2 = common::create_test_card("c04-2", "c04");
    card2.number = "2".to_string();
    card_repo.save_cards(&[card1, card2]).await.unwrap();

    let ids = vec!["c04-1".to_string()];
    let deleted = card_repo.delete_cards_batch(&ids, 500).await.unwrap();
    assert_eq!(deleted, 1);

    let counts = card_repo.count_for_sets(false).await.unwrap();
    let c04_count = counts.iter().find(|(code, _)| code == "c04").unwrap().1;
    assert_eq!(c04_count, 1);
}

#[tokio::test]
#[ignore]
async fn test_count_for_sets() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db);

    set_repo
        .save_sets(&[
            common::create_test_set("c05"),
            common::create_test_set("c06"),
        ])
        .await
        .unwrap();

    let mut card1 = common::create_test_card("c05-1", "c05");
    card1.number = "1".to_string();
    let mut card2 = common::create_test_card("c05-2", "c05");
    card2.number = "2".to_string();
    card2.sort_number = "000002".to_string();
    let mut card3 = common::create_test_card("c06-1", "c06");
    card3.number = "1".to_string();

    card_repo.save_cards(&[card1, card2, card3]).await.unwrap();

    let counts = card_repo.count_for_sets(false).await.unwrap();
    let c05_count = counts.iter().find(|(code, _)| code == "c05").unwrap().1;
    let c06_count = counts.iter().find(|(code, _)| code == "c06").unwrap().1;
    assert_eq!(c05_count, 2);
    assert_eq!(c06_count, 1);
}

#[tokio::test]
#[ignore]
async fn test_fetch_foreign_unpriced_ids() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("c08")])
        .await
        .unwrap();

    // Foreign + unpriced -> should be returned
    let mut foreign_unpriced = common::create_test_card("c08-1", "c08");
    foreign_unpriced.language = "Japanese".to_string();
    // English + unpriced -> must not be returned
    let mut english_unpriced = common::create_test_card("c08-2", "c08");
    english_unpriced.number = "2".to_string();
    // Foreign + priced -> must not be returned
    let mut foreign_priced = common::create_test_card("c08-3", "c08");
    foreign_priced.language = "German".to_string();
    foreign_priced.number = "3".to_string();

    card_repo
        .save_cards(&[foreign_unpriced, english_unpriced, foreign_priced])
        .await
        .unwrap();
    db.execute_raw(
        "INSERT INTO price (card_id, normal, date) VALUES ('c08-3', 1.00, CURRENT_DATE)
         ON CONFLICT (card_id, date) DO NOTHING",
    )
    .await
    .unwrap();

    let ids = card_repo.fetch_foreign_unpriced_ids().await.unwrap();
    assert!(ids.contains(&"c08-1".to_string()));
    assert!(!ids.contains(&"c08-2".to_string()));
    assert!(!ids.contains(&"c08-3".to_string()));

    // The persisted language must round-trip through save_cards.
    let fetched = card_repo
        .fetch_ascii_cards_by_set_and_names("c08", &["Test Card c08-1".to_string()])
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].language, "Japanese");
    assert!(fetched[0].is_foreign());
}

#[tokio::test]
#[ignore]
async fn test_save_cards_persists_scryfall_id() {
    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("c07")])
        .await
        .unwrap();

    let mut card = common::create_test_card("c07-1", "c07");
    card.scryfall_id = Some("11111111-2222-4333-8444-555555555555".to_string());
    card_repo.save_cards(&[card.clone()]).await.unwrap();

    let fetched = card_repo
        .fetch_ascii_cards_by_set_and_names("c07", &[card.name.clone()])
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(
        fetched[0].scryfall_id.as_deref(),
        Some("11111111-2222-4333-8444-555555555555")
    );

    // A scryfall_id change alone must be detected by the upsert
    card.scryfall_id = Some("11111111-2222-4333-8444-666666666666".to_string());
    let saved = card_repo.save_cards(&[card.clone()]).await.unwrap();
    assert_eq!(saved, 1);

    // An identical re-save must be a no-op
    let saved = card_repo.save_cards(&[card]).await.unwrap();
    assert_eq!(saved, 0);
}
