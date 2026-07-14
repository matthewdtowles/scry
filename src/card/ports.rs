//! Ports (traits) for the card module, so `CardService` depends on abstractions
//! it can be tested against with fakes - a canned byte stream and an in-memory
//! repository - instead of a live HTTP client + Postgres.

use crate::card::domain::Card;
use crate::card::repository::CardRepository;
use crate::utils::http_client::HttpClient;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use serde_json::Value;
use std::pin::Pin;

/// A boxed byte stream, the concrete shape the JSON stream parser consumes.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

/// The card catalog data source (MTGJSON). Everything `CardService` needs from
/// the HTTP client, so a test can feed it a canned `AllPrintings.json` stream.
#[async_trait]
pub trait CardDataSource: Send + Sync {
    async fn all_cards_stream(&self) -> Result<ByteStream>;
    async fn fetch_set_cards(&self, set_code: &str) -> Result<Value>;
}

#[async_trait]
impl CardDataSource for HttpClient {
    async fn all_cards_stream(&self) -> Result<ByteStream> {
        Ok(Box::pin(HttpClient::all_cards_stream(self).await?))
    }

    async fn fetch_set_cards(&self, set_code: &str) -> Result<Value> {
        HttpClient::fetch_set_cards::<Value>(self, set_code).await
    }
}

/// The card persistence port: every repository operation `CardService` performs.
#[async_trait]
pub trait CardRepositoryPort: Send + Sync {
    async fn count(&self) -> Result<u64>;
    async fn count_for_sets(&self, main_only: bool) -> Result<Vec<(String, i64)>>;
    async fn legality_count(&self) -> Result<u64>;
    async fn save_cards(&self, cards: &[Card]) -> Result<i64>;
    async fn save_legalities(&self, cards: &[Card]) -> Result<i64>;
    async fn set_exists(&self, code: &str) -> Result<bool>;
    async fn fetch_foreign_unpriced_ids(&self) -> Result<Vec<String>>;
    async fn delete_cards_batch(&self, ids: &[String], batch_size: i64) -> Result<i64>;
    async fn fetch_non_ascii_numbers_in_set(&self, set_code: &str) -> Result<Vec<Card>>;
    async fn fetch_ascii_cards_by_set_and_names(
        &self,
        set_code: &str,
        names: &[String],
    ) -> Result<Vec<Card>>;
    async fn fetch_in_main_cards_for_set_types(&self, set_types: &[&str]) -> Result<Vec<Card>>;
    async fn fetch_misclassified_as_in_main(&self) -> Result<Vec<Card>>;
    async fn reset_all_data(&self) -> Result<()>;
}

#[async_trait]
impl CardRepositoryPort for CardRepository {
    async fn count(&self) -> Result<u64> {
        CardRepository::count(self).await
    }
    async fn count_for_sets(&self, main_only: bool) -> Result<Vec<(String, i64)>> {
        CardRepository::count_for_sets(self, main_only).await
    }
    async fn legality_count(&self) -> Result<u64> {
        CardRepository::legality_count(self).await
    }
    async fn save_cards(&self, cards: &[Card]) -> Result<i64> {
        CardRepository::save_cards(self, cards).await
    }
    async fn save_legalities(&self, cards: &[Card]) -> Result<i64> {
        CardRepository::save_legalities(self, cards).await
    }
    async fn set_exists(&self, code: &str) -> Result<bool> {
        CardRepository::set_exists(self, code).await
    }
    async fn fetch_foreign_unpriced_ids(&self) -> Result<Vec<String>> {
        CardRepository::fetch_foreign_unpriced_ids(self).await
    }
    async fn delete_cards_batch(&self, ids: &[String], batch_size: i64) -> Result<i64> {
        CardRepository::delete_cards_batch(self, ids, batch_size).await
    }
    async fn fetch_non_ascii_numbers_in_set(&self, set_code: &str) -> Result<Vec<Card>> {
        CardRepository::fetch_non_ascii_numbers_in_set(self, set_code).await
    }
    async fn fetch_ascii_cards_by_set_and_names(
        &self,
        set_code: &str,
        names: &[String],
    ) -> Result<Vec<Card>> {
        CardRepository::fetch_ascii_cards_by_set_and_names(self, set_code, names).await
    }
    async fn fetch_in_main_cards_for_set_types(&self, set_types: &[&str]) -> Result<Vec<Card>> {
        CardRepository::fetch_in_main_cards_for_set_types(self, set_types).await
    }
    async fn fetch_misclassified_as_in_main(&self) -> Result<Vec<Card>> {
        CardRepository::fetch_misclassified_as_in_main(self).await
    }
    async fn reset_all_data(&self) -> Result<()> {
        CardRepository::reset_all_data(self).await
    }
}
