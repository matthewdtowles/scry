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

#[tokio::test]
#[ignore]
async fn test_update_parent_codes_breaks_circular_references() {
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    // Two sets point at each other (circular reference)
    let mut parent = common::create_test_set("pc01");
    parent.name = "Test Block".to_string();
    parent.block = Some("Test Block".to_string());
    parent.parent_code = Some("pc02".to_string());
    parent.base_size = 301;
    parent.release_date = chrono::NaiveDate::from_ymd_opt(2006, 10, 6).unwrap();

    let mut child = common::create_test_set("pc02");
    child.name = "Test Block Extras".to_string();
    child.block = Some("Test Block".to_string());
    child.parent_code = Some("pc01".to_string());
    child.base_size = 121;
    child.release_date = chrono::NaiveDate::from_ymd_opt(2006, 10, 6).unwrap();

    repo.save_sets(&[parent, child]).await.unwrap();
    repo.update_is_main().await.unwrap();
    repo.update_parent_codes().await.unwrap();

    // After normalization, canonical parent (pc01 = "Test Block") should have NULL parent_code
    let canonical_has_no_parent = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'pc01' AND parent_code IS NULL")
        .await
        .unwrap();
    assert_eq!(
        canonical_has_no_parent, 1,
        "Canonical parent pc01 should have NULL parent_code"
    );

    // pc02 should point to pc01
    let child_points_to_canonical = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'pc02' AND parent_code = 'pc01'")
        .await
        .unwrap();
    assert_eq!(
        child_points_to_canonical, 1,
        "pc02 should point to canonical parent pc01"
    );
}

#[tokio::test]
#[ignore]
async fn test_update_parent_codes_resolves_grandchild_chains() {
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    // otj is the root, big is a child, pbig is a grandchild
    let mut otj = common::create_test_set("pc10");
    otj.name = "Outlaws of Thunder Junction".to_string();
    otj.block = Some("Outlaws of Thunder Junction".to_string());
    otj.base_size = 261;
    otj.release_date = chrono::NaiveDate::from_ymd_opt(2024, 4, 19).unwrap();

    let mut big = common::create_test_set("pc11");
    big.name = "The Big Score".to_string();
    big.block = Some("Outlaws of Thunder Junction".to_string());
    big.parent_code = Some("pc10".to_string());
    big.base_size = 30;
    big.release_date = chrono::NaiveDate::from_ymd_opt(2024, 4, 19).unwrap();

    let mut pbig = common::create_test_set("pc12");
    pbig.name = "The Big Score Promos".to_string();
    pbig.block = Some("Outlaws of Thunder Junction".to_string());
    pbig.parent_code = Some("pc11".to_string()); // grandchild: points to big, not otj
    pbig.base_size = 0;
    pbig.is_main = false;
    pbig.set_type = "promo".to_string();
    pbig.release_date = chrono::NaiveDate::from_ymd_opt(2024, 4, 19).unwrap();

    repo.save_sets(&[otj, big, pbig]).await.unwrap();
    repo.update_is_main().await.unwrap();
    repo.update_parent_codes().await.unwrap();

    // After normalization, root pc10 should have NULL parent_code
    let root_has_no_parent = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'pc10' AND parent_code IS NULL")
        .await
        .unwrap();
    assert_eq!(
        root_has_no_parent, 1,
        "Root pc10 should have NULL parent_code"
    );

    // pc11 should point to root pc10
    let child_points_to_root = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'pc11' AND parent_code = 'pc10'")
        .await
        .unwrap();
    assert_eq!(
        child_points_to_root, 1,
        "pc11 should point to root pc10"
    );

    // Grandchild pc12 should point to root pc10, not intermediate pc11
    let grandchild_points_to_root = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'pc12' AND parent_code = 'pc10'")
        .await
        .unwrap();
    assert_eq!(
        grandchild_points_to_root, 1,
        "Grandchild pc12 should point to root pc10, not intermediate pc11"
    );
}

#[tokio::test]
#[ignore]
async fn test_update_parent_codes_time_spiral_fixup() {
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    // Use real tsp/tsb codes to exercise the one-off fixup SQL
    let mut tsp = common::create_test_set("tsp");
    tsp.name = "Time Spiral".to_string();
    tsp.block = Some("Time Spiral".to_string());
    tsp.parent_code = Some("tsb".to_string());
    tsp.base_size = 301;
    tsp.release_date = chrono::NaiveDate::from_ymd_opt(2006, 10, 6).unwrap();

    let mut tsb = common::create_test_set("tsb");
    tsb.name = "Time Spiral Timeshifted".to_string();
    tsb.block = None;
    tsb.parent_code = Some("tsp".to_string());
    tsb.base_size = 121;
    tsb.release_date = chrono::NaiveDate::from_ymd_opt(2006, 10, 6).unwrap();

    repo.save_sets(&[tsp, tsb]).await.unwrap();
    repo.update_is_main().await.unwrap();
    repo.update_parent_codes().await.unwrap();

    // tsp is the canonical parent — should have NULL parent_code
    let tsp_no_parent = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'tsp' AND parent_code IS NULL")
        .await
        .unwrap();
    assert_eq!(
        tsp_no_parent, 1,
        "tsp should have NULL parent_code as canonical parent"
    );

    // tsb should point to tsp, not the other way around
    let tsb_points_to_tsp = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'tsb' AND parent_code = 'tsp'")
        .await
        .unwrap();
    assert_eq!(
        tsb_points_to_tsp, 1,
        "tsb should point to tsp"
    );

    // tsb should be in the Time Spiral block after fixup
    let tsb_in_block = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'tsb' AND block = 'Time Spiral'")
        .await
        .unwrap();
    assert_eq!(
        tsb_in_block, 1,
        "tsb should be in the Time Spiral block"
    );
}
