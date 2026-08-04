//! The stale-row sweep (scry#67): rows MTGJSON no longer publishes.
//!
//! Its own test binary because `delete_stale_cards` / `delete_stale` are
//! deliberately table-wide - in production the sweep is the whole catalog's,
//! not one set's. Cargo runs test binaries sequentially, so the only tests that
//! could collide with a table-wide delete are the ones in this file, and those
//! serialize on the guard below. A Postgres advisory lock cannot do this job:
//! it is session-scoped, and every query here takes a different connection out
//! of the pool.

mod common;

use chrono::NaiveDate;
use scry::card::repository::CardRepository;
use scry::database::ConnectionPool;
use scry::sealed_product::domain::SealedProduct;
use scry::sealed_product::repository::SealedProductRepository;
use scry::set::repository::SetRepository;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

static SWEEP_LOCK: Mutex<()> = Mutex::const_new(());

/// Take the file's lock and clear the swept tables, so each test measures only
/// its own rows against a table-wide delete. Tests in a binary share one
/// process, so an in-process mutex is what actually serializes them.
async fn exclusive(db: &Arc<ConnectionPool>) -> MutexGuard<'static, ()> {
    let guard = SWEEP_LOCK.lock().await;
    db.execute_raw("DELETE FROM sealed_product").await.unwrap();
    db.execute_raw("DELETE FROM card").await.unwrap();
    guard
}

fn run_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 8, 4).unwrap()
}

/// Reproduces scry#67 against Postgres.
///
/// Secrets of Strixhaven #168 is Wildgrowth Archaic. A spoiler-season row named
/// "The Very Hungry Archaic" carried the same number under a uuid MTGJSON had
/// since replaced, so it was never upserted again and never deleted. Carrying
/// in_main = false, it became the lowest non-main number in the set - the
/// boundary `fix_main_classification` demotes above - and every main card from
/// 169 to 281 dropped out of the browse listing.
///
/// The ingest stamps only what the feed still lists, so the orphan is the one
/// row without today's date, and the sweep takes exactly it.
#[tokio::test]
#[ignore]
async fn sweep_deletes_the_row_mtgjson_replaced_and_keeps_its_replacement() {
    let db = common::setup_test_db().await;
    let _guard = exclusive(&db).await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("sos")])
        .await
        .unwrap();

    let mut live = common::create_test_card("sos-live-168", "sos");
    live.name = "Wildgrowth Archaic".to_string();
    live.number = "168".to_string();
    live.sort_number = "000168".to_string();

    let mut orphan = common::create_test_card("sos-orphan-168", "sos");
    orphan.name = "The Very Hungry Archaic".to_string();
    orphan.number = "168".to_string();
    orphan.sort_number = "000168".to_string();
    orphan.in_main = false;

    card_repo.save_cards(&[live, orphan]).await.unwrap();

    // The ingest stamps only the card the feed still lists.
    let stamped = card_repo
        .stamp_cards_seen(&["sos-live-168".to_string()], run_date())
        .await
        .unwrap();
    assert_eq!(stamped, 1);

    // Both gate inputs read what the run actually produced.
    assert_eq!(card_repo.count_seen_on(run_date()).await.unwrap(), 1);
    assert_eq!(
        card_repo.fetch_set_codes_with_cards().await.unwrap(),
        vec!["sos".to_string()]
    );

    let deleted = card_repo.delete_stale_cards(run_date()).await.unwrap();
    assert_eq!(deleted, 1, "only the unstamped orphan should be deleted");

    let remaining = card_repo.count_for_sets(false).await.unwrap();
    assert_eq!(
        remaining.iter().find(|(code, _)| code == "sos").unwrap().1,
        1,
        "the live card must survive its orphan twin"
    );
    // And it is the live one, not the orphan.
    assert_eq!(
        card_repo
            .fetch_ascii_cards_by_set_and_names(
                "sos",
                &[
                    "Wildgrowth Archaic".to_string(),
                    "The Very Hungry Archaic".to_string()
                ]
            )
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.name)
            .collect::<Vec<_>>(),
        vec!["Wildgrowth Archaic".to_string()]
    );
}

/// Sealed products carry the same exposure - same feed, same uuid upsert - so
/// they get the same stamp and the same sweep.
#[tokio::test]
#[ignore]
async fn sweep_deletes_sealed_products_the_feed_no_longer_lists() {
    let db = common::setup_test_db().await;
    let _guard = exclusive(&db).await;
    let set_repo = SetRepository::new(db.clone());
    let sealed_repo = SealedProductRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("swp")])
        .await
        .unwrap();

    let product = |uuid: &str, name: &str| SealedProduct {
        uuid: uuid.to_string(),
        name: name.to_string(),
        set_code: "swp".to_string(),
        category: None,
        subtype: None,
        card_count: None,
        product_size: None,
        release_date: None,
        contents_summary: None,
        tcgplayer_product_id: None,
    };

    sealed_repo
        .save(&[
            product("swp-live", "Draft Booster Box"),
            product("swp-orphan", "Renamed Preview Box"),
        ])
        .await
        .unwrap();

    let stamped = sealed_repo
        .stamp_seen(&["swp-live".to_string()], run_date())
        .await
        .unwrap();
    assert_eq!(stamped, 1);
    assert_eq!(sealed_repo.count_seen_on(run_date()).await.unwrap(), 1);

    let deleted = sealed_repo.delete_stale(run_date()).await.unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(sealed_repo.count().await.unwrap(), 1);
}

/// A row predating the column reads NULL, not an old date. `IS DISTINCT FROM`
/// is what makes the sweep see it - `last_seen < today` would silently keep
/// every orphan that existed before the migration, which is all of them.
#[tokio::test]
#[ignore]
async fn sweep_treats_a_null_stamp_as_unseen() {
    let db = common::setup_test_db().await;
    let _guard = exclusive(&db).await;
    let set_repo = SetRepository::new(db.clone());
    let card_repo = CardRepository::new(db.clone());

    set_repo
        .save_sets(&[common::create_test_set("nul")])
        .await
        .unwrap();
    card_repo
        .save_cards(&[common::create_test_card("nul-1", "nul")])
        .await
        .unwrap();

    assert_eq!(card_repo.count_seen_on(run_date()).await.unwrap(), 0);
    assert_eq!(card_repo.delete_stale_cards(run_date()).await.unwrap(), 1);
}
