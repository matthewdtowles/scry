use crate::{
    cli::{confirm_destructive, ingest_pipeline::IngestPipeline, Commands},
    portfolio::service::PortfolioService,
    price::PriceService,
    published_deck::service::PublishedDeckService,
    sealed_product::service::SealedProductService,
};
use anyhow::Result;
use dialoguer::{theme::ColorfulTheme, Select};
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct CliController {
    card_service: crate::card::service::CardService,
    set_service: crate::set::service::SetService,
    price_service: Arc<PriceService>,
    health_service: crate::health_check::service::HealthCheckService,
    portfolio_service: PortfolioService,
    sealed_product_service: SealedProductService,
    published_deck_service: PublishedDeckService,
}

impl CliController {
    pub fn new(
        card_service: crate::card::service::CardService,
        set_service: crate::set::service::SetService,
        price_service: Arc<PriceService>,
        health_service: crate::health_check::service::HealthCheckService,
        portfolio_service: PortfolioService,
        sealed_product_service: SealedProductService,
        published_deck_service: PublishedDeckService,
    ) -> Self {
        Self {
            card_service,
            set_service,
            price_service,
            health_service,
            portfolio_service,
            sealed_product_service,
            published_deck_service,
        }
    }

    /// Borrow the feature services as an [`IngestPipeline`] for one run. The
    /// pipeline owns ordering + prune policy; the controller owns dispatch.
    fn pipeline(&self) -> IngestPipeline<'_> {
        IngestPipeline {
            card_service: &self.card_service,
            set_service: &self.set_service,
            price_service: &self.price_service,
            portfolio_service: &self.portfolio_service,
            sealed_product_service: &self.sealed_product_service,
            published_deck_service: &self.published_deck_service,
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
                buylist,
            } => {
                if buylist {
                    return self.pipeline().update_ck_buylist().await;
                }
                self.pipeline()
                    .run_full_ingest_pipeline(sets, cards, prices, set_cards, sealed, reset)
                    .await
            }

            Commands::IngestCardsSealed {} => self
                .pipeline()
                .ingest_cards_and_sealed()
                .await
                .inspect_err(|e| error!("Combined card+sealed ingest failed: {}", e)),

            Commands::IngestDecks { days } => self
                .pipeline()
                .ingest_published_decks(days)
                .await
                .inspect_err(|e| error!("Published-deck ingest failed: {}", e)),

            Commands::PostIngestPrune {} => self
                .pipeline()
                .post_ingest_prune()
                .await
                .inspect_err(|e| error!("Pruning failed: {}", e)),

            Commands::PostIngestUpdates {} => self
                .pipeline()
                .post_ingest_updates()
                .await
                .inspect_err(|e| error!("Set updates failed: {}", e)),

            Commands::Cleanup { cards, batch_size } => self
                .handle_cleanup(cards, batch_size)
                .await
                .inspect_err(|e| error!("Cleanup failed: {}", e)),

            Commands::Health { detailed } => self
                .handle_health(detailed)
                .await
                .inspect_err(|e| error!("Health check failed: {}", e)),

            Commands::Retention {} => self
                .handle_retention()
                .await
                .inspect_err(|e| error!("Retention cleanup failed: {}", e)),

            Commands::TruncateHistory {} => self
                .handle_truncate_history()
                .await
                .inspect_err(|e| error!("Truncate history failed: {}", e)),

            Commands::Backfill {
                truncate,
                skip_retention,
            } => self
                .handle_backfill(truncate, skip_retention)
                .await
                .inspect_err(|e| error!("Backfill failed: {}", e)),

            Commands::BackfillSetPriceHistory {} => self
                .handle_backfill_set_price_history()
                .await
                .inspect_err(|e| error!("Set price history backfill failed: {}", e)),

            Commands::PortfolioSummary {} => self
                .handle_portfolio_summary()
                .await
                .inspect_err(|e| error!("Portfolio summary computation failed: {}", e)),

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
                self.pipeline()
                    .run_full_ingest_pipeline(true, true, true, None, true, false)
                    .await
            }
            1 => {
                let set_code: String = match dialoguer::Input::with_theme(&ColorfulTheme::default())
                    .with_prompt("Enter set code")
                    .interact_text()
                {
                    Ok(s) => s,
                    Err(_) => return Ok(()),
                };
                if set_code.is_empty() {
                    warn!("No set code entered, skipping.");
                    return Ok(());
                }
                self.pipeline()
                    .run_full_ingest_pipeline(false, false, false, Some(set_code), false, false)
                    .await
            }
            2 => self.pipeline().post_ingest_prune().await,
            3 => self.pipeline().post_ingest_updates().await,
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

        info!("Starting granular price history retention cleanup");
        let (gph_weekly, gph_monthly) = self.price_service.apply_granular_retention().await?;
        info!(
            "Granular price history: weekly deleted {} rows, monthly deleted {} rows",
            gph_weekly, gph_monthly
        );

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

        let confirmed = confirm_destructive(
            "This will DELETE ALL DATA from price_history. Type 'y' to confirm",
        );

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
            let confirmed = confirm_destructive(
                "This will TRUNCATE price_history before backfill. Type 'y' to confirm",
            );
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
}
