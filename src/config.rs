use anyhow::Result;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub max_pool_size: u32,
    /// Server-side cap on any single statement, in milliseconds. Generous on
    /// purpose - the slowest real statement in a nightly run is well under a
    /// minute - because its job is to bound a hang, not to police slow queries.
    pub statement_timeout_ms: u64,
    /// Server-side cap on waiting for a row/table lock, in milliseconds. Much
    /// tighter than the statement timeout: nothing scry writes should ever
    /// queue behind another transaction for more than a moment.
    pub lock_timeout_ms: u64,
    /// Whole-command deadline enforced in-process, in seconds. Backstops the
    /// server-side timeouts, which cannot fire if the connection's socket died
    /// silently and the reply never arrives.
    pub command_timeout_seconds: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Config {
            database_url: Self::get_database_url()?,
            max_pool_size: Self::parse_env("DB_MAX_POOL_SIZE", "10")?,
            statement_timeout_ms: Self::parse_env("SCRY_STATEMENT_TIMEOUT_MS", "600000")?,
            lock_timeout_ms: Self::parse_env("SCRY_LOCK_TIMEOUT_MS", "30000")?,
            command_timeout_seconds: Self::parse_env("SCRY_COMMAND_TIMEOUT_SECONDS", "1800")?,
        })
    }

    fn get_database_url() -> Result<String> {
        if let Ok(url) = env::var("DATABASE_URL") {
            return Ok(url);
        }
        let host = env::var("DB_HOST")?;
        let port = env::var("DB_PORT")?;
        let username = env::var("DB_USERNAME")?;
        let password = env::var("DB_PASSWORD")?;
        let database = env::var("DB_NAME")?;
        // Percent-encode the credentials so a password with '@', '/', ':', '#',
        // etc. produces a valid URL instead of a mis-parsed one.
        let username = utf8_percent_encode(&username, NON_ALPHANUMERIC);
        let password = utf8_percent_encode(&password, NON_ALPHANUMERIC);
        Ok(format!(
            "postgresql://{}:{}@{}:{}/{}",
            username, password, host, port, database
        ))
    }

    fn parse_env<T: std::str::FromStr>(key: &str, default: &str) -> Result<T>
    where
        T::Err: std::fmt::Display,
    {
        env::var(key)
            .unwrap_or_else(|_| default.to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", key, e))
    }
}
