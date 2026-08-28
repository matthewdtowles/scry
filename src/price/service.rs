use crate::price::cardkingdom::{granular_from_ck_products, CkPricelistEventProcessor, CkProduct};
use crate::price::domain::{CardPrices, Price};
use crate::price::event_processor::PriceEventProcessor;
use crate::price::repository::PriceRepository;
use crate::price::write_timings::{timed, WriteTimings};
use crate::utils::{clock, JsonStreamParser};
use crate::{database::ConnectionPool, utils::http_client::HttpClient};
use anyhow::Result;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// How our `price` table compares against the build MTGJSON says it has
/// published.
///
/// `expected` comes from MTGJSON's own `Meta.json`, not from the clock. The
/// previous version guessed it from a hardcoded publish hour, and that guess
/// was wrong every single day: the 08:00 UTC ingest was served the previous
/// day's build, so the check declared "the feed has not advanced" and mailed
/// about it four mornings running while nothing was actually wrong upstream.
///
/// Asking upstream what it has removes the guess entirely, and inverts what an
/// alert means. It no longer says "upstream published nothing" - which is
/// upstream's business and not worth an email. It now says *we are behind data
/// that exists*, which is our problem and is worth one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceCurrency {
    /// The date on the newest row in `price`, or `None` when the table is empty.
    pub newest: Option<NaiveDate>,
    /// The build date MTGJSON reports as currently published, or `None` when we
    /// could not reach it. Unknown means unverifiable, and an unverifiable
    /// check must stay quiet rather than guess.
    pub expected: Option<NaiveDate>,
}

impl PriceCurrency {
    /// True when there is nothing to report. Defined in terms of [`Self::alert`]
    /// so the two can never disagree. They were separate matches and they did
    /// drift: `is_current` said an empty table was fine whenever upstream
    /// happened to be unreachable.
    pub fn is_current(&self) -> bool {
        self.alert().is_none()
    }

    /// The whole alert line, or `None` when there is nothing to say.
    ///
    /// Returns the complete sentence rather than a fragment a caller prefixes,
    /// so the empty-table case cannot be described as a stale one.
    ///
    /// The two conditions are independent, and conflating them is what the
    /// earlier revision got wrong. Being *behind* needs upstream to compare
    /// against, so an unreachable `Meta.json` makes it unknowable and it stays
    /// quiet. An *empty* table needs nothing external - zero prices after a
    /// completed ingest is broken on its own terms - so it always speaks, and
    /// says as much as it can about what we should have had.
    pub fn alert(&self) -> Option<String> {
        match self.newest {
            None => Some(match self.expected {
                Some(expected) => format!(
                    "the price table is EMPTY after a completed ingest - upstream reports a \
                     {expected} build. This is not a late upstream build; no prices are being \
                     served at all."
                ),
                None => "the price table is EMPTY after a completed ingest, and MTGJSON could \
                         not be reached to say which build we should have had. No prices are \
                         being served at all."
                    .to_string(),
            }),
            Some(newest) => {
                // No upstream answer means no way to know whether we are behind.
                // Alerting on that would be the false alarm this replaced.
                let expected = self.expected?;
                if newest >= expected {
                    return None;
                }
                Some(format!(
                    "the ingest did not pick up the newest MTGJSON build - upstream reports \
                     {expected} but the price table holds {newest}. The run itself succeeded, \
                     so this is a stale download rather than a failure; prices are a build behind."
                ))
            }
        }
    }
}

const BATCH_SIZE: usize = 500;
/// Emit an info-level progress line roughly every this many cards so a slow but
/// healthy stream is visible at the default `scry=info` verbosity.
const PROGRESS_LOG_EVERY: usize = 20_000;

pub struct RetentionResult {
    pub weekly_deleted: i64,
    pub monthly_deleted: i64,
    pub total_deleted: i64,
}

#[derive(Debug, Default)]
pub struct CkDirectStats {
    pub rows_saved: i64,
    pub unmatched: u64,
}

pub struct PriceService {
    client: Arc<HttpClient>,
    repository: PriceRepository,
}

impl PriceService {
    pub fn new(db: Arc<ConnectionPool>, http_client: Arc<HttpClient>) -> Self {
        Self {
            client: http_client,
            repository: PriceRepository::new(db),
        }
    }

    pub async fn fetch_prices_for_card_ids(
        &self,
        card_ids: &[String],
    ) -> Result<HashMap<String, (Option<Decimal>, Option<Decimal>)>> {
        self.repository.fetch_prices_for_card_ids(card_ids).await
    }

    pub async fn update_price_foil_if_null(
        &self,
        card_id: &str,
        new_foil: &Decimal,
    ) -> Result<i64> {
        self.repository
            .update_price_foil_if_null(card_id, new_foil)
            .await
    }

    pub async fn insert_price_for_card(
        &self,
        card_id: &str,
        normal: Option<Decimal>,
        foil: Option<Decimal>,
    ) -> Result<i64> {
        self.repository
            .insert_price_for_card(card_id, normal, foil)
            .await
    }

    pub async fn fetch_price_count(&self) -> Result<i64> {
        self.repository.price_count().await
    }

    pub async fn fetch_price_history_count(&self) -> Result<i64> {
        self.repository.price_history_count().await
    }

    /// Ingest today's prices into the averaged `price` / `price_history` tables.
    /// The per-vendor granular store is no longer written here - CK-direct is the
    /// sole granular writer (ROADMAP 10.10). A hard write/stream failure aborts.
    pub async fn ingest_all_today(&self) -> Result<()> {
        debug!("Start ingestion of all prices");
        let byte_stream = self.client.all_today_prices_stream().await?;
        debug!("Received byte stream for today's prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = PriceEventProcessor::new(BATCH_SIZE);
        let timings = WriteTimings::default();
        let mut cards_seen = 0usize;
        let mut next_log = PROGRESS_LOG_EVERY;

        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                cards_seen += batch.len();
                if cards_seen >= next_log {
                    info!("Ingested {} card prices so far...", cards_seen);
                    next_log += PROGRESS_LOG_EVERY;
                }
                Box::pin(self.save_prices(batch, &valid_card_ids, &timings))
            })
            .await?;
        info!("Finished ingesting prices for {} cards.", cards_seen);
        timings.log_summary("ingest_all_today");

        Ok(())
    }

    /// Backfill historical averaged prices into `price_history`. A hard
    /// write/stream failure aborts.
    pub async fn ingest_all_historical(&self) -> Result<()> {
        debug!("Start ingestion of all historical prices");
        let byte_stream = self.client.all_prices_stream().await?;
        debug!("Received byte stream for historical prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = PriceEventProcessor::new_historical(BATCH_SIZE);
        let timings = WriteTimings::default();
        let mut cards_seen = 0usize;
        let mut next_log = PROGRESS_LOG_EVERY;

        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                cards_seen += batch.len();
                if cards_seen >= next_log {
                    info!("Ingested {} historical card prices so far...", cards_seen);
                    next_log += PROGRESS_LOG_EVERY;
                }
                Box::pin(self.save_price_history_only(batch, &valid_card_ids, &timings))
            })
            .await?;
        info!(
            "Finished ingesting historical prices for {} cards.",
            cards_seen
        );
        timings.log_summary("ingest_all_historical");

        Ok(())
    }

    /// Ingest Card Kingdom's direct pricelist: live buylist offers
    /// (`price_buy` + `qty_buying`), matched to cards via `scryfall_id`. Must
    /// run AFTER the MTGJSON ingest so the CK-direct row overwrites the
    /// indicative MTGJSON CK row on the shared granular key (last-writer-wins
    /// upsert). Hard failures return Err; the caller treats the whole
    /// CK-direct pass as best-effort enrichment.
    pub async fn ingest_cardkingdom_direct(&self) -> Result<CkDirectStats> {
        debug!("Start Card Kingdom direct pricelist ingestion");
        let scryfall_map = self.repository.fetch_scryfall_card_id_map().await?;
        if scryfall_map.is_empty() {
            warn!("No cards carry a scryfall_id; skipping CK-direct ingest.");
            return Ok(CkDirectStats::default());
        }
        let byte_stream = self.client.cardkingdom_pricelist_stream().await?;
        let today = clock::today();
        let rows_saved = AtomicI64::new(0);
        let unmatched = AtomicU64::new(0);
        let timings = WriteTimings::default();

        let event_processor = CkPricelistEventProcessor::new(BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                Box::pin(self.save_ck_batch(
                    batch,
                    &scryfall_map,
                    today,
                    &rows_saved,
                    &unmatched,
                    &timings,
                ))
            })
            .await?;
        timings.log_summary("ingest_cardkingdom_direct");

        Ok(CkDirectStats {
            rows_saved: rows_saved.load(Ordering::Relaxed),
            unmatched: unmatched.load(Ordering::Relaxed),
        })
    }

    async fn save_ck_batch(
        &self,
        products: Vec<CkProduct>,
        scryfall_map: &HashMap<String, String>,
        date: NaiveDate,
        rows_saved: &AtomicI64,
        unmatched: &AtomicU64,
        timings: &WriteTimings,
    ) -> Result<()> {
        let (rows, batch_unmatched) = granular_from_ck_products(products, scryfall_map, date);
        unmatched.fetch_add(batch_unmatched, Ordering::Relaxed);
        if rows.is_empty() {
            return Ok(());
        }
        let saved = timed(
            &timings.granular_price,
            self.repository.save_granular_prices(&rows),
        )
        .await?;
        rows_saved.fetch_add(saved, Ordering::Relaxed);
        Ok(())
    }

    /// How far back to look for a price to carry forward.
    ///
    /// Chosen to match the retention tiers - daily for 7 days, weekly to 28,
    /// monthly beyond - so there is real history across the whole window. Past
    /// roughly a month a frozen number is more misleading than an absent one:
    /// a card that stopped trading should eventually read as having no price
    /// rather than holding whatever it last sold for.
    pub const CARRY_FORWARD_MAX_AGE_DAYS: i32 = 30;

    /// Fill in cards today's build did not mention, from the newest price we
    /// have for them. See [`PriceRepository::carry_forward_missing_prices`] for
    /// why the carried row keeps its original date.
    pub async fn carry_forward_prices(&self) -> Result<i64> {
        let carried = self
            .repository
            .carry_forward_missing_prices(Self::CARRY_FORWARD_MAX_AGE_DAYS)
            .await?;
        Ok(carried)
    }

    /// Remove all prices older than the latest price date, in one statement (§5).
    pub async fn clean_up_prices(&self) -> Result<()> {
        let deleted = self.repository.delete_prices_before_latest().await?;
        if deleted == 0 {
            info!("No old prices found in price table.");
        } else {
            info!("Removed {deleted} old price rows.");
        }
        Ok(())
    }

    /// What we hold versus what MTGJSON reports it has published.
    ///
    /// `clean_up_prices` drops every date but the newest, so `newest` is the
    /// build the last successful ingest actually wrote.
    ///
    /// A failure reaching `Meta.json` is not an error: it leaves `expected` as
    /// `None`, which reads as "cannot tell" and stays silent. Alerting because
    /// we could not check would be the same false alarm this replaced.
    pub async fn price_currency(&self) -> Result<PriceCurrency> {
        let price_dates = self.repository.fetch_price_dates().await?;
        let expected = match self.client.fetch_published_build_date().await {
            Ok(date) => Some(date),
            Err(e) => {
                warn!("Could not read MTGJSON's published build date, skipping the freshness check: {e}");
                None
            }
        };
        Ok(PriceCurrency {
            newest: price_dates.iter().max().copied(),
            expected,
        })
    }

    pub async fn fetch_history_size(&self) -> Result<String> {
        self.repository.price_history_size().await
    }

    pub async fn apply_retention(&self) -> Result<RetentionResult> {
        info!("Starting retention cleanup on price_history");

        let weekly_deleted = self.repository.apply_weekly_retention().await?;
        info!("Weekly period: deleted {} rows", weekly_deleted);

        let monthly_deleted = self.repository.apply_monthly_retention().await?;
        info!("Monthly period: deleted {} rows", monthly_deleted);

        let total_deleted = weekly_deleted + monthly_deleted;
        Ok(RetentionResult {
            weekly_deleted,
            monthly_deleted,
            total_deleted,
        })
    }

    pub async fn truncate_history(&self) -> Result<()> {
        self.repository.truncate_price_history().await
    }

    pub async fn update_price_change_weekly(&self) -> Result<i64> {
        self.repository.update_price_change_weekly().await
    }

    async fn save_price_history_only(
        &self,
        card_prices: Vec<CardPrices>,
        valid_card_ids: &std::collections::HashSet<String>,
        timings: &WriteTimings,
    ) -> Result<()> {
        // Historical backfill: averaged prices -> price_history.
        let mut history: Vec<Price> = Vec::new();
        for cp in card_prices {
            for avg in cp.averages {
                if valid_card_ids.contains(&avg.card_id) {
                    history.push(avg);
                }
            }
        }

        if !history.is_empty() {
            let history_count = timed(
                &timings.price_history,
                self.repository.save_price_history(&history),
            )
            .await?;
            debug!("Saved batch of {} prices to history table.", history_count);
        }
        Ok(())
    }

    async fn save_prices(
        &self,
        card_prices: Vec<CardPrices>,
        valid_card_ids: &std::collections::HashSet<String>,
        timings: &WriteTimings,
    ) -> Result<()> {
        // Derive the averaged retail price per card for the price/price_history
        // tables, filtering to known card ids. The per-vendor granular rows the
        // event processor also emits are intentionally dropped - CK-direct is the
        // sole granular writer now (ROADMAP 10.10).
        let mut averages: Vec<Price> = Vec::new();
        for cp in card_prices {
            for avg in cp.averages {
                if valid_card_ids.contains(&avg.card_id) {
                    averages.push(avg);
                }
            }
        }

        if !averages.is_empty() {
            let saved_count = timed(&timings.price, self.repository.save_prices(&averages)).await?;
            debug!("Saved batch of {} prices to price table.", saved_count);
            let history_count = timed(
                &timings.price_history,
                self.repository.save_price_history(&averages),
            )
            .await?;
            debug!("Saved batch of {} prices to history table.", history_count);
        }
        Ok(())
    }
}

#[cfg(test)]
mod currency_tests {
    use super::*;

    fn d(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, day).unwrap()
    }

    #[test]
    fn holding_the_build_upstream_reports_is_current_and_silent() {
        let c = PriceCurrency {
            newest: Some(d(28)),
            expected: Some(d(28)),
        };
        assert!(c.is_current());
        assert_eq!(c.alert(), None);
    }

    /// The four mornings of false alarms. Upstream had not published a newer
    /// build, so being on the previous day's data was upstream's schedule, not
    /// our failure - and it must not mail. Under the old clock-based
    /// expectation this alerted every single day.
    #[test]
    fn upstream_not_having_published_yet_is_silent() {
        let c = PriceCurrency {
            newest: Some(d(27)),
            expected: Some(d(27)),
        };
        assert!(c.is_current());
        assert_eq!(c.alert(), None);
    }

    /// The case actually worth an email: a build exists that we did not get.
    #[test]
    fn being_behind_a_published_build_alerts_and_names_both_dates() {
        let c = PriceCurrency {
            newest: Some(d(27)),
            expected: Some(d(28)),
        };
        assert!(!c.is_current());
        let alert = c.alert().expect("being behind must alert");
        assert!(alert.contains("2026-08-27"), "{alert}");
        assert!(alert.contains("2026-08-28"), "{alert}");
        assert!(alert.contains("stale download"), "{alert}");
    }

    /// Prices ahead of upstream's reported build are not staleness - a run can
    /// straddle a publish. #71's `>=` semantics, preserved.
    #[test]
    fn being_ahead_of_upstream_is_still_current() {
        let c = PriceCurrency {
            newest: Some(d(29)),
            expected: Some(d(28)),
        };
        assert!(c.is_current());
        assert_eq!(c.alert(), None);
    }

    /// An unreachable Meta.json means the check could not run. Alerting on
    /// that would be the same false alarm this design replaced.
    #[test]
    fn an_unreachable_upstream_stays_silent() {
        let c = PriceCurrency {
            newest: Some(d(20)),
            expected: None,
        };
        assert!(c.is_current());
        assert_eq!(c.alert(), None);
    }

    /// The regression Copilot caught: an empty table is broken on its own
    /// terms, and must still say so when MTGJSON cannot be reached. Gating it
    /// on upstream meant the loudest internal failure went silent in exactly
    /// the circumstances - a network problem - most likely to accompany it.
    #[test]
    fn an_empty_price_table_alerts_even_when_upstream_is_unreachable() {
        let c = PriceCurrency {
            newest: None,
            expected: None,
        };
        assert!(!c.is_current());
        let alert = c.alert().expect("an empty table must alert regardless");
        assert!(alert.contains("EMPTY"), "{alert}");
        assert!(alert.contains("could not be reached"), "{alert}");
    }

    /// An empty table is a different problem from being a build behind, and
    /// must not be described as one.
    #[test]
    fn an_empty_price_table_is_not_described_as_a_stale_download() {
        let c = PriceCurrency {
            newest: None,
            expected: Some(d(28)),
        };
        assert!(!c.is_current());
        let alert = c.alert().expect("an empty table must alert");
        assert!(alert.contains("EMPTY"), "{alert}");
        assert!(!alert.contains("stale download"), "{alert}");
    }
}
