pub mod commands;
pub mod controller;
pub mod ingest_pipeline;

pub use commands::Commands;

use dialoguer::Confirm;
use tracing::warn;

/// Prompt the user to confirm a destructive operation.
///
/// In a non-interactive session (cron/Docker, no TTY — how this tool runs in
/// production), `dialoguer` returns `Err` because it can't read a response.
/// Treat that as "not confirmed" and refuse, rather than `.unwrap()`-panicking
/// with a backtrace.
pub(crate) fn confirm_destructive(prompt: &str) -> bool {
    match Confirm::new().with_prompt(prompt).default(false).interact() {
        Ok(confirmed) => confirmed,
        Err(_) => {
            warn!("Non-interactive session: refusing destructive operation without confirmation.");
            false
        }
    }
}
