use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use reqwest::Client;
use serde::de::DeserializeOwned;
use tracing::{debug, info};

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    const BASE_INGESTION_URL: &str = "https://mtgjson.com/api/v5/";
    const ALL_CARDS_URL: &str = "AllPrintings.json";
    const SET_LIST_URL: &str = "SetList.json";
    const TODAY_PRICES_URL: &str = "AllPricesToday.json";
    const ALL_PRICES_URL: &str = "AllPrices.json";

    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    pub async fn all_cards_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::ALL_CARDS_URL);
        info!("Stream all cards from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn all_today_prices_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::TODAY_PRICES_URL);
        info!("Stream all prices from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn all_prices_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::ALL_PRICES_URL);
        info!("Stream all historical prices from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn fetch_set_cards<T>(&self, set_code: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{}.json", Self::BASE_INGESTION_URL, set_code);
        self.fetch_json(url.as_str()).await
    }

    pub async fn fetch_all_sets<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::SET_LIST_URL);
        self.fetch_json(url.as_str()).await
    }

    async fn fetch_json_bytes_stream(
        &self,
        url: &str,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        debug!("Fetch JSON Bytes Stream.");
        let response = self.client.get(url).send().await?.error_for_status()?;
        debug!("Received response from: {}", url);
        let byte_stream = response.bytes_stream();
        debug!("Returning response byte stream.");
        Ok(byte_stream)
    }

    async fn fetch_json<T>(&self, url: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let response = self.client.get(url).send().await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "HTTP request failed: {}",
                response.status()
            ));
        }
        Ok(response.json::<T>().await?)
    }
}
