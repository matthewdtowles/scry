use crate::cli::confirm_destructive;
use crate::ingest::{CardSealedEventProcessor, IngestRecord};
use crate::sealed_product::domain::SealedProduct;
use crate::utils::JsonStreamParser;
use crate::{
    card::domain::Card, card::service::CardService, portfolio::service::PortfolioService,
    price::PriceService, published_deck::service::PublishedDeckService,
    sealed_product::service::SealedProductService, set::service::SetService,
};
use anyhow::Result;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// The ingest application service: owns pipeline ordering, prune policy, and the
/// first-error-wins aggregation. It borrows the feature services from the
/// controller for the duration of a run, so the controller stays responsible
/// for dispatch, prompts, and display while this holds the orchestration.
pub(crate) struct IngestPipeline<'a> {
    pub card_service: &'a CardService,
    pub set_service: &'a SetService,
    pub price_service: &'a Arc<PriceService>,
    pub portfolio_service: &'a PortfolioService,
    pub sealed_product_service: &'a SealedProductService,
    pub published_deck_service: &'a PublishedDeckService,
}

impl IngestPipeline<'_> {
    /// Minimum fraction of a set's cards that must carry a price for the set to
    /// be kept during post-ingest prune.
    const MIN_PRICE_PCT: f64 = 0.36;

    pub async fn run_full_ingest_pipeline(
        &self,
        sets: bool,
        cards: bool,
        prices: bool,
        set_cards: Option<String>,
        sealed: bool,
        reset: bool,
    ) -> Result<()> {
        let mut first_err: Option<anyhow::Error> = None;
        if let Err(e) = self
            .handle_ingest(sets, cards, prices, set_cards, sealed, reset)
            .await
        {
            error!("Ingestion failed: {}", e);
            first_err.get_or_insert(e);
        }
        if let Err(e) = self.post_ingest_prune().await {
            error!("Post ingestion pruning failed: {}", e);
            first_err.get_or_insert(e);
        }
        if let Err(e) = self.post_ingest_updates().await {
            error!("Post ingestion updates failed: {}", e);
            first_err.get_or_insert(e);
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn handle_ingest(
        &self,
        sets: bool,
        cards: bool,
        prices: bool,
        set_cards: Option<String>,
        sealed: bool,
        reset: bool,
    ) -> Result<()> {
        let mut first_err: Option<anyhow::Error> = None;
        if reset {
            match self.reset_data().await {
                Ok(()) => info!("Successfully reset data."),
                Err(e) => {
                    error!("Failed to reset data: {}", e);
                    first_err.get_or_insert(e);
                }
            }
        }
        let do_all = !sets && !cards && !prices && !sealed && set_cards.is_none();
        if do_all || sets {
            match self.update_sets().await {
                Ok(()) => info!("Successfully updated sets."),
                Err(e) => {
                    error!("Failed to update sets: {}", e);
                    first_err.get_or_insert(e);
                }
            }
        }
        // Cards and sealed products both come from AllPrintings.json. When both
        // are requested (the default full run), ingest them in a single pass
        // over that one stream instead of downloading + parsing the file twice;
        // when only one is requested, run that one's standalone pass. Either way
        // this happens before prices, which depend on cards.
        let do_cards = do_all || cards;
        let do_sealed = do_all || sealed;
        if do_cards && do_sealed {
            match self.ingest_cards_and_sealed().await {
                Ok(()) => info!("Card + sealed product ingest completed successfully."),
                Err(e) => {
                    error!("Card + sealed product ingest failure: {}", e);
                    first_err.get_or_insert(e);
                }
            }
        } else {
            if do_cards {
                match self.update_cards().await {
                    Ok(()) => info!("Card update completed successfully."),
                    Err(e) => {
                        error!("Card update failure: {}", e);
                        first_err.get_or_insert(e);
                    }
                }
            }
            if do_sealed {
                match self.update_sealed_products().await {
                    Ok(()) => info!("Sealed product update completed successfully."),
                    Err(e) => {
                        error!("Sealed product update failure: {}", e);
                        first_err.get_or_insert(e);
                    }
                }
            }
        }
        // `--cards` already ingests every set, so `--set-cards` is subsumed by it.
        // Warn instead of silently dropping it (§7).
        if cards && set_cards.is_some() {
            warn!("`--cards` ingests all sets' cards; `--set-cards` is redundant here and was skipped.");
        }
        if !cards {
            if let Some(set_code) = &set_cards {
                match self.card_service.ingest_set_cards(set_code).await {
                    Ok(ingested) => info!("{} cards for set code '{}'.", ingested, set_code),
                    Err(e) => {
                        error!("Error updating cards for set code '{}': {}", set_code, e);
                        first_err.get_or_insert(e);
                    }
                }
            }
        }
        if do_all || prices {
            match self.update_prices().await {
                Ok(()) => info!("Price update completed successfully."),
                Err(e) => {
                    error!("Price update failure: {}", e);
                    first_err.get_or_insert(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn update_sealed_products(&self) -> Result<()> {
        let total_before = self.sealed_product_service.fetch_count().await?;
        self.sealed_product_service.ingest_all().await?;
        let total_after = self.sealed_product_service.fetch_count().await?;
        info!("Sealed products before: {}", total_before);
        info!("Sealed products after: {}", total_after);
        Ok(())
    }

    pub async fn ingest_published_decks(&self, days: i64) -> Result<()> {
        let before = self.published_deck_service.fetch_count().await?;
        self.published_deck_service.ingest(days).await?;
        let after = self.published_deck_service.fetch_count().await?;
        info!("Published decks before: {}", before);
        info!("Published decks after: {}", after);
        Ok(())
    }

    /// Ingest cards + sealed products in a single pass over AllPrintings.json,
    /// used by default when both are requested. Sets must already be ingested
    /// (the card path skips unknown sets; sealed is filtered to set codes in
    /// the `set` table).
    pub async fn ingest_cards_and_sealed(&self) -> Result<()> {
        let cards_before = self.card_service.fetch_count().await?;
        let sealed_before = self.sealed_product_service.fetch_count().await?;
        let valid_set_codes = self.sealed_product_service.fetch_valid_set_codes().await?;
        let start = std::time::Instant::now();
        let sealed_saved = self.run_single_pass_ingest(valid_set_codes).await?;
        let elapsed = start.elapsed();
        let cards_after = self.card_service.fetch_count().await?;
        let sealed_after = self.sealed_product_service.fetch_count().await?;
        info!(
            "Single-pass card+sealed ingest finished in {:.1}s",
            elapsed.as_secs_f64()
        );
        info!("Cards: {} -> {}", cards_before, cards_after);
        info!(
            "Sealed products: {} -> {} ({} saved this run)",
            sealed_before, sealed_after, sealed_saved
        );
        Ok(())
    }

    /// The single-pass card+sealed ingest itself. Both records come from one
    /// `AllPrintings.json` stream via the tee processor (so the file is
    /// downloaded + tokenized once). This orchestration lives in the application
    /// layer because it is inherently cross-module - it drives the card
    /// persistence (`CardService::save_card_batch`) and the sealed persistence
    /// (the sealed repo) from a single stream. Returns sealed products saved.
    async fn run_single_pass_ingest(&self, valid_set_codes: HashSet<String>) -> Result<i64> {
        let valid_set_codes = Arc::new(valid_set_codes);
        let card_repo = self.card_service.repository();
        let sealed_repo = self.sealed_product_service.repository();
        let byte_stream = self.card_service.all_cards_stream().await?;
        let event_processor = CardSealedEventProcessor::new(
            CardService::BATCH_SIZE,
            SealedProductService::BATCH_SIZE,
        );
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let sealed_total = Arc::new(Mutex::new(0i64));
        let sealed_total_for_closure = sealed_total.clone();
        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let card_repo = card_repo.clone();
                let sealed_repo = sealed_repo.clone();
                let valid_set_codes = valid_set_codes.clone();
                let sealed_total = sealed_total_for_closure.clone();
                Box::pin(async move {
                    // In practice each batch is homogeneous (only one extractor
                    // flushes per event), but split by variant to be safe.
                    let mut cards: Vec<Card> = Vec::new();
                    let mut sealed: Vec<SealedProduct> = Vec::new();
                    for record in batch {
                        match record {
                            IngestRecord::Card(c) => cards.push(c),
                            IngestRecord::Sealed(s) => sealed.push(s),
                        }
                    }
                    if !cards.is_empty() {
                        CardService::save_card_batch(&card_repo, cards).await?;
                    }
                    if !sealed.is_empty() {
                        let set_code = sealed[0].set_code.clone();
                        let filtered: Vec<SealedProduct> = sealed
                            .into_iter()
                            .filter(|p| valid_set_codes.contains(&p.set_code))
                            .collect();
                        if !filtered.is_empty() {
                            match sealed_repo.save(&filtered).await {
                                Ok(count) => {
                                    let mut lock = sealed_total.lock().await;
                                    *lock += count;
                                }
                                Err(e) => warn!(
                                    "Failed to save sealed products for set {}: {:#}",
                                    set_code, e
                                ),
                            }
                        }
                    }
                    Ok(())
                })
            })
            .await?;
        let final_total = *sealed_total.lock().await;
        Ok(final_total)
    }

    /// Standalone CK-direct buylist refresh (`ingest -b`): re-run the live
    /// buylist enrichment without re-ingesting MTGJSON prices. Unlike the
    /// in-pipeline call (best-effort), failing IS the command's whole purpose,
    /// so errors propagate -> non-zero exit.
    pub async fn update_ck_buylist(&self) -> Result<()> {
        let stats = self.price_service.ingest_cardkingdom_direct().await?;
        info!(
            "CK-direct buylist: {} rows saved, {} offers unmatched.",
            stats.rows_saved, stats.unmatched
        );
        Ok(())
    }

    async fn update_prices(&self) -> Result<()> {
        let total_prices_before = self.price_service.fetch_price_count().await?;
        let total_history_before = self.price_service.fetch_price_history_count().await?;
        self.price_service.ingest_all_today().await?;
        // CK-direct buylist enrichment (live offers + buy qty) runs AFTER the
        // MTGJSON ingest so it overwrites the indicative CK rows. Best-effort:
        // a failure here must never block the averaged price refresh -- it is
        // logged and surfaced via a non-zero exit at the end of this fn.
        let ck_error = match self.price_service.ingest_cardkingdom_direct().await {
            Ok(stats) => {
                info!(
                    "CK-direct buylist: {} rows saved, {} offers unmatched.",
                    stats.rows_saved, stats.unmatched
                );
                None
            }
            Err(e) => {
                error!("CK-direct buylist ingest failed (price refresh unaffected): {e:?}");
                Some(e)
            }
        };
        self.price_service.clean_up_prices().await?;
        let total_prices_after = self.price_service.fetch_price_count().await?;
        let total_history_after = self.price_service.fetch_price_history_count().await?;
        info!("Total prices before: {}", total_prices_before);
        info!("Total prices after: {}", total_prices_after);
        info!("Total prices in history before: {}", total_history_before);
        info!("Total prices in history after: {}", total_history_after);
        let has_current_prices = self.price_service.prices_are_current().await?;
        if has_current_prices {
            info!("Price table is up to date.");
        } else {
            warn!("Prices for today's date not yet available.");
        }
        // Averaged prices are fully refreshed and cleaned up above; surface any
        // best-effort CK-direct failure last so the run exits non-zero (alerting)
        // without having held back the critical path.
        if let Some(e) = ck_error {
            return Err(anyhow::anyhow!(
                "CK-direct buylist ingest failed; averaged price/price_history were updated: {e}"
            ));
        }
        Ok(())
    }

    async fn update_sets(&self) -> Result<()> {
        let total_sets_before = self.set_service.fetch_count().await?;
        self.set_service.ingest_all().await?;
        let total_sets_after = self.set_service.fetch_count().await?;
        info!("Total sets before: {}", total_sets_before);
        info!("Total sets after: {}", total_sets_after);
        Ok(())
    }

    async fn update_cards(&self) -> Result<()> {
        let total_cards_before = self.card_service.fetch_count().await?;
        let total_legalities_before = self.card_service.fetch_legality_count().await?;
        self.card_service.ingest_all().await?;
        let total_cards_after = self.card_service.fetch_count().await?;
        let total_legalities_after = self.card_service.fetch_legality_count().await?;
        info!("Total cards before {}", total_cards_before);
        info!("Total cards after {}", total_cards_after);
        info!("Total legalities before {}", total_legalities_before);
        info!("Total legalities after {}", total_legalities_after);
        Ok(())
    }

    async fn reset_data(&self) -> Result<()> {
        let confirmed = confirm_destructive(
            "This will DELETE all MTG data before ingesting. Do you want to proceed?",
        );
        if !confirmed {
            warn!("Skipped data reset.");
            return Ok(());
        }
        self.card_service.reset_all_data().await?;
        info!("All MTG data deleted.");
        Ok(())
    }

    pub async fn post_ingest_prune(&self) -> Result<()> {
        info!("Begin post-ingestion pruning of sets and cards.");
        let total_sets_before = self.set_service.fetch_count().await?;
        let total_cards_before = self.card_service.fetch_count().await?;

        let cards_deleted = self.card_service.prune_foreign_unpriced().await?;
        info!("Pruned {} foreign cards without prices.", cards_deleted);

        let sets_deleted = self
            .set_service
            .prune_missing_prices(Self::MIN_PRICE_PCT)
            .await?;
        info!("Pruned {} sets missing prices.", sets_deleted);

        let sets_deleted = self.set_service.prune_empty_sets().await?;
        info!("Pruned {} sets without any cards.", sets_deleted);

        let cards_deleted = self
            .card_service
            .prune_duplicate_foils(self.price_service)
            .await?;
        info!("Pruned {} duplicate foil cards.", cards_deleted);

        let total_sets_after = self.set_service.fetch_count().await?;
        let total_cards_after = self.card_service.fetch_count().await?;
        info!(
            "Post-ingestion pruning complete. Total sets before {} | after {}",
            total_sets_before, total_sets_after
        );
        info!(
            "Total cards before {} | after {}",
            total_cards_before, total_cards_after
        );
        Ok(())
    }

    pub async fn post_ingest_updates(&self) -> Result<()> {
        let total_cards_updated = self.card_service.fix_main_classification().await?;
        info!(
            "Total cards moved from their main set to non-main: {}",
            total_cards_updated
        );
        let total_reclassified = self.card_service.reclassify_non_main_set_types().await?;
        info!(
            "Total cards reclassified from non-main set types: {}",
            total_reclassified
        );
        info!("Begin post-ingestion updates.");
        let base_sizes = self.card_service.count_per_all_sets(true).await?;
        info!("Found {} base_sizes for all sets.", base_sizes.len());
        let total_sizes = self.card_service.count_per_all_sets(false).await?;
        info!("Found {} total_sizes for all sets.", total_sizes.len());
        let total_sets_updated = self
            .set_service
            .update_sizes(base_sizes, total_sizes)
            .await?;
        info!("Total set sizes updated: {}", total_sets_updated);
        let total_is_main_updated = self.set_service.update_main_status().await?;
        info!("Total set is_main updated: {}", total_is_main_updated);
        let total_parent_codes_updated = self.set_service.update_parent_codes().await?;
        info!(
            "Total set parent_codes updated: {}",
            total_parent_codes_updated
        );
        let total_set_prices_updated = self.set_service.update_set_prices().await?;
        info!(
            "Total set prices rows updated: {}",
            total_set_prices_updated
        );
        let price_changes_updated = self.price_service.update_price_change_weekly().await?;
        info!(
            "Card price weekly changes updated: {}",
            price_changes_updated
        );
        let set_price_changes_updated = self.set_service.update_set_price_change_weekly().await?;
        info!(
            "Set price weekly changes updated: {}",
            set_price_changes_updated
        );

        let portfolio_snapshots = self.portfolio_service.snapshot_portfolio_values().await?;
        info!("Portfolio value snapshots saved: {}", portfolio_snapshots);
        Ok(())
    }
}
