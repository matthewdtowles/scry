use anyhow::Result;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub max_pool_size: u32,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Config {
            database_url: Self::get_database_url()?,
            max_pool_size: Self::parse_env("DB_MAX_POOL_SIZE", "10")?,
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
