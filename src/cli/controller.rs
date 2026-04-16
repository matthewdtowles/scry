use crate::{
    cli::Commands,
    portfolio::service::PortfolioService,
    price::PriceService,
    sealed_product::service::SealedProductService,
};
use anyhow::Result;
use dialoguer::{Confirm, Select, theme::ColorfulTheme};
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct CliController {
    card_service: crate::card::service::CardService,
    set_service: crate::set::service::SetService,
    price_service: Arc<PriceService>,
    health_service: crate::health_check::service::HealthCheckService,
    portfolio_service: PortfolioService,
    sealed_product_service: SealedProductService,
}

impl CliController {
    pub fn new(
        card_service: crate::card::service::CardService,
        set_service: crate::set::service::SetService,
        price_service: Arc<PriceService>,
        health_service: crate::health_check::service::HealthCheckService,
        portfolio_service: PortfolioService,
        sealed_product_service: SealedProductService,
    ) -> Self {
        Self {
            card_service,
            set_service,
            price_service,
            health_service,
            portfolio_service,
            sealed_product_service,
        }
    }

    pub async fn handle_command(&self, command: Commands) -> Result<()> {
        match command {
            Commands::Ingest {
                sets,
                cards,
                prices,
                set_cards,
                sealed,
                reset,
            } => {
                self.run_full_ingest_pipeline(sets, cards, prices, set_cards, sealed, reset)
                    .await
            }

            Commands::PostIngestPrune {} => {
                if let Err(e) = self.post_ingest_prune().await {
                    error!("Pruning failed: {}", e);
                }
                Ok(())
            }

            Commands::PostIngestUpdates {} => {
                if let Err(e) = self.post_ingest_updates().await {
                    error!("Set updates failed: {}", e);
                }
                Ok(())
            }

            Commands::Cleanup { cards, batch_size } => {
                if let Err(e) = self.handle_cleanup(cards, batch_size).await {
                    error!("Cleanup failed: {}", e);
                }
                Ok(())
            }

            Commands::Health { detailed } => {
                if let Err(e) = self.handle_health(detailed).await {
                    error!("Health check failed: {}", e);
                }
                Ok(())
            }

            Commands::Retention {} => {
                if let Err(e) = self.handle_retention().await {
                    error!("Retention cleanup failed: {}", e);
                }
                Ok(())
            }

            Commands::TruncateHistory {} => {
                if let Err(e) = self.handle_truncate_history().await {
                    error!("Truncate history failed: {}", e);
                }
                Ok(())
            }

            Commands::Backfill {
                truncate,
                skip_retention,
            } => {
                if let Err(e) = self.handle_backfill(truncate, skip_retention).await {
                    error!("Backfill failed: {}", e);
                }
                Ok(())
            }

            Commands::BackfillSetPriceHistory {} => {
                if let Err(e) = self.handle_backfill_set_price_history().await {
                    error!("Set price history backfill failed: {}", e);
                }
                Ok(())
            }

            Commands::PortfolioSummary {} => {
                if let Err(e) = self.handle_portfolio_summary().await {
                    error!("Portfolio summary computation failed: {}", e);
                }
                Ok(())
            }

            Commands::Interactive {} => self.interactive_mode().await,
        }
    }

    pub async fn interactive_mode(&self) -> Result<()> {
        let menu_items = [
            "Ingest (run ingestion tasks)",
            "Health Check (check data integrity)",
            "Maintenance (cleanup, retention, portfolio summary)",
            "One-Time Setup (destructive / historical operations)",
            "Help (show detailed descriptions)",
            "Exit",
        ];

        loop {
            println!();
            let selection = match Select::with_theme(&ColorfulTheme::default())
                .with_prompt("Main menu")
                .items(&menu_items)
                .default(0)
                .interact()
            {
                Ok(s) => s,
                Err(_) => {
                    info!("Exiting interactive mode.");
                    break;
                }
            };

            let result = match selection {
                0 => self.ingest_submenu().await,
                1 => self.health_submenu().await,
                2 => self.maintenance_submenu().await,
                3 => self.setup_submenu().await,
                4 => {
                    Self::print_help();
                    Ok(())
                }
                5 => {
                    info!("Exiting interactive mode.");
                    break;
                }
                _ => continue,
            };

            if let Err(e) = result {
                error!("Command failed: {}", e);
            }
        }
        Ok(())
    }

    async fn ingest_submenu(&self) -> Result<()> {
        let items = [
            "Ingest All (full pipeline: sets + cards + prices + sealed, then prune + updates)",
            "Ingest specific set (prompts for set code, then prune + updates)",
            "Post-Ingest Prune only (foreign unpriced, empty sets, duplicate foils)",
            "Post-Ingest Updates only (set sizes, prices, classifications, portfolio)",
            "Back",
        ];
        let selection = match Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Ingest")
            .items(&items)
            .default(0)
            .interact()
        {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        match selection {
            0 => {
                self.run_full_ingest_pipeline(false, false, false, None, false, false)
                    .await
            }
            1 => {
                let set_code: String = dialoguer::Input::with_theme(&ColorfulTheme::default())
                    .with_prompt("Enter set code")
                    .interact_text()
                    .unwrap_or_default();
                if set_code.is_empty() {
                    warn!("No set code entered, skipping.");
                    return Ok(());
                }
                self.run_full_ingest_pipeline(false, false, false, Some(set_code), false, false)
                    .await
            }
            2 => self.post_ingest_prune().await,
            3 => self.post_ingest_updates().await,
            _ => Ok(()),
        }
    }

    async fn health_submenu(&self) -> Result<()> {
        let items = [
            "Basic (quick integrity probe)",
            "Detailed (thorough integrity check across tables)",
            "Back",
        ];
        let selection = match Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Health Check")
            .items(&items)
            .default(0)
            .interact()
        {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        match selection {
            0 => self.handle_health(false).await,
            1 => self.handle_health(true).await,
            _ => Ok(()),
        }
    }

    async fn maintenance_submenu(&self) -> Result<()> {
        let items = [
            "Cleanup (remove sets/cards that fail current filter rules)",
            "Retention (apply tiered retention to price histories; normally run via cron)",
            "Portfolio Summary (compute per-user summaries; normally run via cron)",
            "Back",
        ];
        let selection = match Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Maintenance")
            .items(&items)
            .default(0)
            .interact()
        {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        match selection {
            0 => self.handle_cleanup(true, 500).await,
            1 => self.handle_retention().await,
            2 => self.handle_portfolio_summary().await,
            _ => Ok(()),
        }
    }

    async fn setup_submenu(&self) -> Result<()> {
        let items = [
            "Truncate Price History [DESTRUCTIVE] (delete ALL rows from price_history)",
            "Backfill Price History (load historical prices from AllPrices.json)",
            "Backfill Set Price History (derive set_price_history from price_history)",
            "Back",
        ];
        let selection = match Select::with_theme(&ColorfulTheme::default())
            .with_prompt("One-Time Setup")
            .items(&items)
            .default(0)
            .interact()
        {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        match selection {
            0 => self.handle_truncate_history().await,
            1 => self.handle_backfill(false, false).await,
            2 => self.handle_backfill_set_price_history().await,
            _ => Ok(()),
        }
    }

    fn print_help() {
        println!(
            "{}",
            r#"
Scry Interactive Mode — Help
============================

INGEST
  Ingest All               Full pipeline (matches `scry ingest`): pulls sets,
                           cards, prices, and sealed products from Scryfall +
                           MTGJSON, then runs prune + post-ingest updates.
  Ingest specific set      Prompts for a set code (e.g. 'mh3'), ingests cards
                           for that set, then runs prune + updates.
  Post-Ingest Prune        Re-run prune only: foreign unpriced cards, empty
                           sets, duplicate foils.
  Post-Ingest Updates      Re-run updates only: set sizes, set prices, main-set
                           classifications, portfolio snapshots.

HEALTH CHECK
  Basic                    Quick data integrity probe.
  Detailed                 Thorough integrity check across tables.

MAINTENANCE
  Cleanup                  Remove sets/cards that fail current filtering rules.
                           Run only after filter rules change.
  Retention                Apply tiered retention to price_history,
                           set_price_history, portfolio_value_history.
                           Normally run weekly via cron.
  Portfolio Summary        Compute portfolio_summary + card_performance for
                           all users. Normally run daily via cron.

ONE-TIME SETUP
  Truncate Price History   Drop every row from price_history (requires
                           confirm). Typically used only before a fresh backfill.
  Backfill Price History   Load AllPrices.json from MTGJSON into price_history.
                           One-time operation for new environments.
  Backfill Set Price       Derive set_price_history from existing
  History                  price_history. One-time operation.
"#
        );
    }

    async fn run_full_ingest_pipeline(
        &self,
        sets: bool,
        cards: bool,
        prices: bool,
        set_cards: Option<String>,
        sealed: bool,
        reset: bool,
    ) -> Result<()> {
        if let Err(e) = self
            .handle_ingest(sets, cards, prices, set_cards, sealed, reset)
            .await
        {
            error!("Ingestion failed: {}", e);
        }
        if let Err(e) = self.post_ingest_prune().await {
            error!("Post ingestion pruning failed: {}", e);
        }
        if let Err(e) = self.post_ingest_updates().await {
            error!("Post ingestion updates failed: {}", e);
        }
        Ok(())
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
        if reset {
            match self.reset_data().await {
                Ok(()) => info!("Successfully reset data."),
                Err(e) => error!("Failed to reset data: {}", e),
            }
        }
        let do_all = !sets && !cards && !prices && !sealed && set_cards.is_none();
        if do_all || sets {
            match self.update_sets().await {
                Ok(()) => info!("Successfully updated sets."),
                Err(e) => error!("Failed to update sets: {}", e),
            }
        }
        if do_all || cards {
            match self.update_cards().await {
                Ok(()) => info!("Card update completed successfully."),
                Err(e) => error!("Card udpate failure: {}", e),
            }
        }
        if !cards {
            if let Some(set_code) = &set_cards {
                match self.card_service.ingest_set_cards(set_code).await {
                    Ok(ingested) => info!("{} cards for set code '{}'.", ingested, set_code),
                    Err(e) => error!("Error updating cards for set code '{}': {}", set_code, e),
                }
            }
        }
        if do_all || prices {
            match self.update_prices().await {
                Ok(()) => info!("Price update completed successfully."),
                Err(e) => error!("Price update failure: {}", e),
            }
        }
        if do_all || sealed {
            match self.update_sealed_products().await {
                Ok(()) => info!("Sealed product update completed successfully."),
                Err(e) => error!("Sealed product update failure: {}", e),
            }
        }
        Ok(())
    }

    async fn handle_cleanup(&self, cards: bool, batch_size: i64) -> Result<()> {
        info!("Handle cleanup called.");
        let total_sets_before = self.set_service.fetch_count().await?;
        let total_cards_before = self.card_service.fetch_count().await?;
        info!(
            "Set cleanup starting: before -> {} sets | {} cards",
            total_sets_before, total_cards_before
        );
        let total_sets_deleted = self.set_service.cleanup_sets().await?;
        info!("Deleted {} total sets", total_sets_deleted);
        let total_sets_after = self.set_service.fetch_count().await?;
        let total_cards_after = self.card_service.fetch_count().await?;
        info!(
            "Set cleanup complete: after -> {} sets | {} cards",
            total_sets_after, total_cards_after
        );
        if cards {
            info!("Begin cleanup of individual cards.");
            let total_deleted = self.card_service.cleanup_cards(batch_size).await?;
            info!("Deleted {} total cards", total_deleted);
            let total_cards_after = self.card_service.fetch_count().await?;
            info!(
                "Card cleanup complete: after -> {} cards",
                total_cards_after
            );
        }
        Ok(())
    }

    async fn handle_health(&self, detailed: bool) -> Result<()> {
        if detailed {
            let status = self.health_service.detailed_check().await?;
            status.display();
        } else {
            let status = self.health_service.basic_check().await?;
            status.display();
        }
        Ok(())
    }

    async fn handle_retention(&self) -> Result<()> {
        info!("Starting price history retention cleanup");
        let result = self.price_service.apply_retention().await?;
        info!("Weekly period: deleted {} rows", result.weekly_deleted);
        info!("Monthly period: deleted {} rows", result.monthly_deleted);
        info!("Total deleted: {}", result.total_deleted);

        info!("Starting set price history retention cleanup");
        let (weekly, monthly) = self.set_service.apply_set_price_history_retention().await?;
        info!(
            "Set price history: weekly deleted {} rows, monthly deleted {} rows",
            weekly, monthly
        );

        info!("Starting portfolio value history retention cleanup");
        let (pvh_weekly, pvh_monthly) = self.portfolio_service.apply_retention().await?;
        info!(
            "Portfolio value history: weekly deleted {} rows, monthly deleted {} rows",
            pvh_weekly, pvh_monthly
        );
        Ok(())
    }

    async fn handle_truncate_history(&self) -> Result<()> {
        let count = self.price_service.fetch_price_history_count().await?;
        let size = self.price_service.fetch_history_size().await?;
        info!("Current price_history: {} rows, {}", count, size);

        let confirmed = Confirm::new()
            .with_prompt("This will DELETE ALL DATA from price_history. Type 'y' to confirm")
            .default(false)
            .interact()
            .unwrap();

        if !confirmed {
            warn!("Aborted. No data was deleted.");
            return Ok(());
        }

        self.price_service.truncate_history().await?;
        let new_size = self.price_service.fetch_history_size().await?;
        info!("Table truncated. New size: {}", new_size);
        warn!("Remember to reload price history data!");
        Ok(())
    }

    async fn handle_backfill(&self, truncate: bool, skip_retention: bool) -> Result<()> {
        let count_before = self.price_service.fetch_price_history_count().await?;
        let size_before = self.price_service.fetch_history_size().await?;
        info!(
            "Current price_history: {} rows, {}",
            count_before, size_before
        );

        if truncate {
            let confirmed = Confirm::new()
                .with_prompt(
                    "This will TRUNCATE price_history before backfill. Type 'y' to confirm",
                )
                .default(false)
                .interact()
                .unwrap();
            if !confirmed {
                warn!("Aborted backfill.");
                return Ok(());
            }
            self.price_service.truncate_history().await?;
            info!("Truncated price_history table.");
        }

        info!("Starting historical price backfill from AllPrices.json...");
        self.price_service.ingest_all_historical().await?;
        info!("Historical price backfill complete.");

        if !skip_retention {
            info!("Applying retention policy...");
            let result = self.price_service.apply_retention().await?;
            info!("Weekly period: deleted {} rows", result.weekly_deleted);
            info!("Monthly period: deleted {} rows", result.monthly_deleted);
            info!("Total deleted by retention: {}", result.total_deleted);
        }

        let count_after = self.price_service.fetch_price_history_count().await?;
        let size_after = self.price_service.fetch_history_size().await?;
        info!("Final price_history: {} rows, {}", count_after, size_after);

        info!("Starting set price history backfill from price_history...");
        self.handle_backfill_set_price_history().await?;

        Ok(())
    }

    async fn handle_backfill_set_price_history(&self) -> Result<()> {
        info!("Backfilling set_price_history from price_history...");
        let rows = self.set_service.backfill_set_price_history().await?;
        info!(
            "Set price history backfill complete: {} rows affected",
            rows
        );
        Ok(())
    }

    async fn handle_portfolio_summary(&self) -> Result<()> {
        info!("Computing portfolio summaries for all users");
        let (summaries_saved, performance_saved) =
            self.portfolio_service.compute_portfolio_summaries().await?;
        info!("Portfolio summaries saved: {}", summaries_saved);
        info!("Card performance rows saved: {}", performance_saved);
        Ok(())
    }

    async fn update_sealed_products(&self) -> Result<()> {
        let total_before = self.sealed_product_service.fetch_count().await?;
        self.sealed_product_service.ingest_all().await?;
        let total_after = self.sealed_product_service.fetch_count().await?;
        info!("Sealed products before: {}", total_before);
        info!("Sealed products after: {}", total_after);
        Ok(())
    }

    async fn update_prices(&self) -> Result<()> {
        let total_prices_before = self.price_service.fetch_price_count().await?;
        let total_history_before = self.price_service.fetch_price_history_count().await?;
        self.price_service.ingest_all_today().await?;
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
        let confirmed = Confirm::new()
            .with_prompt("This will DELETE all MTG data before ingesting. Do you want to proceed?")
            .default(false)
            .interact()
            .unwrap();
        if !confirmed {
            warn!("Skipped data reset.");
            return Ok(());
        }
        let prices_deleted = self.price_service.delete_all().await?;
        let cards_deleted = self.card_service.delete_all().await?;
        let sets_deleted = self.set_service.delete_all().await?;
        info!(
            "All MTG data deleted: {} sets | {} cards | {} prices",
            sets_deleted, cards_deleted, prices_deleted
        );
        Ok(())
    }

    async fn post_ingest_prune(&self) -> Result<()> {
        info!("Begin post-ingestion pruning of sets and cards.");
        let total_sets_before = self.set_service.fetch_count().await?;
        let total_cards_before = self.card_service.fetch_count().await?;

        let cards_deleted = self.card_service.prune_foreign_unpriced().await?;
        info!("Pruned {} foreign cards without prices.", cards_deleted);

        let min_price_pct = 0.36;
        let sets_deleted = self.set_service.prune_missing_prices(min_price_pct).await?;
        info!("Pruned {} sets missing prices.", sets_deleted);

        let sets_deleted = self.set_service.prune_empty_sets().await?;
        info!("Pruned {} sets without any cards.", sets_deleted);

        let cards_deleted = self.card_service.prune_duplicate_foils().await?;
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

    async fn post_ingest_updates(&self) -> Result<()> {
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
