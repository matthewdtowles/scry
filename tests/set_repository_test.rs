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
async fn test_fetch_all_set_codes() {
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db);

    // Use codes unique to this test so parallel tests don't collide.
    let sets = vec![
        common::create_test_set("fasc01"),
        common::create_test_set("fasc02"),
    ];
    repo.save_sets(&sets).await.unwrap();

    let codes = repo.fetch_all_set_codes().await.unwrap();
    assert!(codes.iter().any(|c| c == "fasc01"));
    assert!(codes.iter().any(|c| c == "fasc02"));
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
async fn test_update_is_main_block_children_stay_main() {
    // Block-children (Dark Ascension, Eldritch Moon, Born of the Gods, …)
    // are full main expansions and must stay is_main=true even after
    // update_parent_codes stamps a parent_code onto them.
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    let mut isd = common::create_test_set("im01");
    isd.name = "Innistrad".to_string();
    isd.block = Some("Innistrad Test".to_string());
    isd.set_type = "expansion".to_string();
    isd.base_size = 264;
    isd.release_date = chrono::NaiveDate::from_ymd_opt(2011, 9, 30).unwrap();

    let mut dka = common::create_test_set("im02");
    dka.name = "Dark Ascension".to_string();
    dka.block = Some("Innistrad Test".to_string());
    dka.set_type = "expansion".to_string();
    dka.base_size = 158;
    dka.release_date = chrono::NaiveDate::from_ymd_opt(2012, 2, 3).unwrap();

    repo.save_sets(&[isd, dka]).await.unwrap();
    repo.update_is_main().await.unwrap();
    repo.update_parent_codes().await.unwrap();

    // Sanity-check that update_parent_codes actually stamped a non-NULL
    // parent_code onto im02. Otherwise the rest of this test would silently
    // degrade into "is_main on rows with no parent_code" — not the scenario
    // we're trying to validate.
    let im02_parent_set = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'im02' AND parent_code = 'im01'")
        .await
        .unwrap();
    assert_eq!(
        im02_parent_set, 1,
        "update_parent_codes must set im02.parent_code = 'im01' for this test to be meaningful"
    );

    // Re-run is_main after parent_codes to prove order independence —
    // the result must not change.
    repo.update_is_main().await.unwrap();

    let main_count = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code IN ('im01','im02') AND is_main = true")
        .await
        .unwrap();
    assert_eq!(
        main_count, 2,
        "Both Innistrad (canonical) and Dark Ascension (block-child) should be is_main=true"
    );
}

#[tokio::test]
#[ignore]
async fn test_update_is_main_bonus_overrides() {
    // BIG, TSB, MAT are the documented bonus-sheet overrides. They must
    // flip is_main=false regardless of parent_code state.
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    let mut big = common::create_test_set("big");
    big.name = "The Big Score".to_string();
    big.set_type = "expansion".to_string();
    big.parent_code = Some("otj".to_string());
    big.base_size = 30;

    let mut tsb = common::create_test_set("tsb");
    tsb.name = "Time Spiral Timeshifted".to_string();
    tsb.set_type = "expansion".to_string();
    tsb.base_size = 121;

    let mut mat = common::create_test_set("mat");
    mat.name = "March of the Machine: The Aftermath".to_string();
    mat.set_type = "expansion".to_string();
    mat.base_size = 50;

    repo.save_sets(&[big, tsb, mat]).await.unwrap();
    repo.update_is_main().await.unwrap();

    let main_count = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code IN ('big','tsb','mat') AND is_main = true")
        .await
        .unwrap();
    assert_eq!(
        main_count, 0,
        "All three documented bonus sheets must be is_main=false"
    );
}

#[tokio::test]
#[ignore]
async fn test_update_is_main_non_expansion_types_excluded() {
    // commander, draft_innovation, masterpiece, promo, funny, starter must
    // all be is_main=false. Only expansion + core qualify.
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    let types = [
        ("nm01", "commander"),
        ("nm02", "draft_innovation"),
        ("nm03", "masterpiece"),
        ("nm04", "promo"),
        ("nm05", "funny"),
        ("nm06", "starter"),
        ("nm07", "duel_deck"),
        ("nm08", "masters"),
    ];
    let sets: Vec<_> = types
        .iter()
        .map(|(code, t)| {
            let mut s = common::create_test_set(code);
            s.set_type = t.to_string();
            s.base_size = 200;
            s
        })
        .collect();

    repo.save_sets(&sets).await.unwrap();
    repo.update_is_main().await.unwrap();

    let main_count = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code LIKE 'nm0%' AND is_main = true")
        .await
        .unwrap();
    assert_eq!(
        main_count, 0,
        "Non-expansion/core types must never be is_main=true"
    );
}

#[tokio::test]
#[ignore]
async fn test_update_is_main_order_independent() {
    // The same input data must produce the same is_main result regardless
    // of whether update_parent_codes ran before or after update_is_main.
    // This is the regression guard against the prior ordering accident.
    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());

    let seed = |codes: (&'static str, &'static str)| {
        let mut parent = common::create_test_set(codes.0);
        parent.name = "Test Parent Set".to_string();
        parent.block = Some("Order Independence Block".to_string());
        parent.set_type = "expansion".to_string();
        parent.base_size = 280;
        parent.release_date = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();

        let mut child = common::create_test_set(codes.1);
        child.name = "Test Child Expansion".to_string();
        child.block = Some("Order Independence Block".to_string());
        child.set_type = "expansion".to_string();
        child.base_size = 165;
        child.release_date = chrono::NaiveDate::from_ymd_opt(2020, 6, 1).unwrap();

        vec![parent, child]
    };

    // Order A uses one pair of codes, Order B uses another. Each sequence
    // therefore starts from an identical fresh input state (same shape, same
    // block, same types) and is unaffected by the other run's mutations —
    // which is the only way to truly compare the two orderings.
    let codes_a = ("oi01a", "oi02a");
    let codes_b = ("oi01b", "oi02b");

    // Order A: parent_codes → is_main
    repo.save_sets(&seed(codes_a)).await.unwrap();
    repo.update_parent_codes().await.unwrap();
    repo.update_is_main().await.unwrap();
    let main_after_a = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code IN ('oi01a','oi02a') AND is_main = true")
        .await
        .unwrap();

    // Order B: is_main → parent_codes (no second is_main pass — this is the
    // pipeline shape Copilot flagged as the one a weaker test could miss)
    repo.save_sets(&seed(codes_b)).await.unwrap();
    repo.update_is_main().await.unwrap();
    repo.update_parent_codes().await.unwrap();
    let main_after_b = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code IN ('oi01b','oi02b') AND is_main = true")
        .await
        .unwrap();

    assert_eq!(main_after_a, 2, "Both sets should be main under order A");
    assert_eq!(
        main_after_a, main_after_b,
        "is_main result must be identical regardless of execution order"
    );
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
    assert_eq!(child_points_to_root, 1, "pc11 should point to root pc10");

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
    assert_eq!(tsb_points_to_tsp, 1, "tsb should point to tsp");

    // tsb should be in the Time Spiral block after fixup
    let tsb_in_block = db
        .count("SELECT COUNT(*) FROM \"set\" WHERE code = 'tsb' AND block = 'Time Spiral'")
        .await
        .unwrap();
    assert_eq!(tsb_in_block, 1, "tsb should be in the Time Spiral block");
}

#[tokio::test]
#[ignore]
async fn test_calculate_set_prices_matches_by_set_code() {
    // Covers the `set_code = ANY(...)` code list filter in calculate_set_prices.
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use scry::card::repository::CardRepository;
    use scry::price::domain::Price;
    use scry::price::repository::PriceRepository;

    let db = common::setup_test_db().await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());
    let price_repo = PriceRepository::new(db);

    set_repo
        .save_sets(&[common::create_test_set("csp01")])
        .await
        .unwrap();
    let card = common::create_test_card("csp01-c1", "csp01");
    card_repo
        .save_cards(std::slice::from_ref(&card))
        .await
        .unwrap();
    price_repo
        .save_prices(&[Price {
            card_id: "csp01-c1".to_string(),
            normal: Some(Decimal::try_from(2.50).unwrap()),
            foil: Some(Decimal::try_from(5.00).unwrap()),
            date: NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        }])
        .await
        .unwrap();

    // Passing a multi-element array also exercises the ANY(...) bind.
    let set_prices = set_repo
        .calculate_set_prices(&["csp01".to_string(), "missing".to_string()])
        .await
        .unwrap();

    let sp = set_prices
        .iter()
        .find(|s| s.set_code == "csp01")
        .expect("a set price row for csp01");
    // base/total use COALESCE(normal, foil, 0) = 2.50 (normal is present).
    assert_eq!(sp.base_price, Decimal::try_from(2.50).unwrap());
    assert_eq!(sp.total_price, Decimal::try_from(2.50).unwrap());
    // *_all sum normal + foil = 7.50.
    assert_eq!(sp.base_price_all, Decimal::try_from(7.50).unwrap());
    assert_eq!(sp.total_price_all, Decimal::try_from(7.50).unwrap());
    // The non-existent code must not appear.
    assert!(!set_prices.iter().any(|s| s.set_code == "missing"));
}

#[tokio::test]
#[ignore]
async fn test_update_prices_and_history_go_through_set_price_insert() {
    // Covers both callers of the shared set_price_insert helper (§4.3/§4.4):
    // update_prices (ON CONFLICT set_code) and save_set_price_history
    // (ON CONFLICT set_code,date), including the upsert paths.
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use scry::set::domain::SetPrice;

    let db = common::setup_test_db().await;
    let repo = SetRepository::new(db.clone());
    repo.save_sets(&[common::create_test_set("spi01")])
        .await
        .unwrap();

    let make = |base: i64, date: (i32, u32, u32)| SetPrice {
        set_code: "spi01".to_string(),
        base_price: Decimal::new(base, 2),
        total_price: Decimal::new(base + 100, 2),
        base_price_all: Decimal::new(base + 200, 2),
        total_price_all: Decimal::new(base + 300, 2),
        date: NaiveDate::from_ymd_opt(date.0, date.1, date.2).unwrap(),
    };

    // Insert, then upsert on the same set_code -> still one row, updated value.
    repo.update_prices(vec![make(150, (2024, 6, 15))])
        .await
        .unwrap();
    repo.update_prices(vec![make(275, (2024, 6, 16))])
        .await
        .unwrap();
    let rows = db
        .count("SELECT COUNT(*) FROM set_price WHERE set_code = 'spi01'")
        .await
        .unwrap();
    assert_eq!(rows, 1, "update_prices upserts on set_code");
    let updated = db
        .count("SELECT COUNT(*) FROM set_price WHERE set_code = 'spi01' AND base_price = 2.75")
        .await
        .unwrap();
    assert_eq!(updated, 1, "second update_prices overwrote base_price");

    // History keys on (set_code, date) -> two distinct dates -> two rows.
    repo.save_set_price_history(vec![make(150, (2024, 6, 15))])
        .await
        .unwrap();
    repo.save_set_price_history(vec![make(275, (2024, 6, 16))])
        .await
        .unwrap();
    let hist = db
        .count("SELECT COUNT(*) FROM set_price_history WHERE set_code = 'spi01'")
        .await
        .unwrap();
    assert_eq!(hist, 2, "history keeps one row per (set_code, date)");
}
