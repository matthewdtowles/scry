use anyhow::Result;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use reqwest::header::{HeaderValue, CONTENT_RANGE, ETAG, IF_RANGE, RANGE};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use std::time::Duration;
use tracing::{debug, info, warn};

/// How many times one download may reconnect after its connection drops.
const MAX_RESUMES: u32 = 5;
/// Wait before each reconnect, multiplied by the attempt number.
const RESUME_DELAY: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(5)
};

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    const BASE_INGESTION_URL: &str = "https://mtgjson.com/api/v5/";
    const ALL_CARDS_URL: &str = "AllPrintings.json";
    const SET_LIST_URL: &str = "SetList.json";
    const TODAY_PRICES_URL: &str = "AllPricesToday.json";
    const ALL_PRICES_URL: &str = "AllPrices.json";
    const CK_PRICELIST_URL: &str = "https://api.cardkingdom.com/api/v2/pricelist";

    pub fn new() -> Self {
        // A bare `Client::new()` has no timeouts, so a stalled CDN connection
        // (or a body stream that goes silent mid-download) hangs forever with no
        // log. `connect_timeout` caps the handshake; `read_timeout` is a
        // per-read inactivity timeout that fails a stalled stream instead of a
        // total deadline, so a legitimately long download is not cut off.
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(60))
            .build()
            .expect("failed to build HTTP client");
        Self { client }
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

    pub async fn cardkingdom_pricelist_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        info!(
            "Stream Card Kingdom pricelist from: {}",
            Self::CK_PRICELIST_URL
        );
        self.fetch_json_bytes_stream(Self::CK_PRICELIST_URL).await
    }

    /// The build date carried by `AllPricesToday.json` itself, read without
    /// downloading it.
    ///
    /// `meta` is the first key in the file, so a 256-byte Range request answers
    /// "is there new price data?" for ~0.0005% of the 53MB body.
    ///
    /// This reads the date out of *the file we would actually ingest*, which is
    /// the only one that decides what we end up storing. `Meta.json` is a
    /// sibling endpoint and can disagree: on 2026-08-28 a download at 08:03
    /// returned the previous day's prices, so gating on anything other than
    /// this file risks fetching 53MB to discover we already had it. The
    /// `Last-Modified` header is likewise not a signal - it read 06:08 that
    /// morning for content that was not yet being served.
    pub async fn published_price_build_date(&self) -> Result<NaiveDate> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::TODAY_PRICES_URL);
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::RANGE, "bytes=0-255")
            .send()
            .await?
            .error_for_status()?;
        // Check the status before touching the body. A server that ignores the
        // Range header answers 200 with the whole file, and `.bytes()` would
        // then quietly pull 53MB - hourly, that is over a gigabyte a day to
        // answer a yes/no question, and it would keep working, so nothing would
        // ever surface it. Refuse instead: the caller treats an error as
        // "cannot tell" and skips the run.
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            let size = response
                .content_length()
                .map_or_else(|| "unknown".to_string(), |n| format!("{n}"));
            return Err(anyhow::anyhow!(
                "{url} ignored the Range header: expected 206 Partial Content, got {}. \
                 Refusing to read the {size}-byte body for a date check.",
                response.status()
            ));
        }
        let head = response.bytes().await?;
        let head = String::from_utf8_lossy(&head);
        // Deliberately not a JSON parse: the slice is a truncated document by
        // construction, so no parser can accept it. The shape is fixed and
        // upstream-controlled - `{"meta":{"date":"YYYY-MM-DD",...`.
        let date = head
            .split("\"date\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .ok_or_else(|| anyhow::anyhow!("no meta.date in the first bytes of {url}: {head:?}"))?;
        Ok(date.parse::<NaiveDate>()?)
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

    /// Streams `url`, picking up where it left off if the connection drops.
    ///
    /// A dropped connection used to fail the whole ingest. On 2026-09-30
    /// MTGJSON cut the AllPrintings download off about two minutes in on two
    /// hourly runs in a row, and each run lost its card update. After a drop
    /// this asks for the rest of the file with a Range request. `If-Range`
    /// carries the ETag, so if the file was republished mid-download the server
    /// sends the whole new file instead, which is refused rather than spliced
    /// onto the old one. Without an ETag there is no safe resume, so the drop
    /// fails the download as before.
    async fn fetch_json_bytes_stream(
        &self,
        url: &str,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        debug!("Fetch JSON Bytes Stream.");
        let response = self.client.get(url).send().await?.error_for_status()?;
        debug!("Received response from: {}", url);
        let etag = response.headers().get(ETAG).cloned();
        let client = self.client.clone();
        let url = url.to_string();
        Ok(async_stream::stream! {
            let mut body: BoxStream<'static, reqwest::Result<Bytes>> =
                response.bytes_stream().boxed();
            let mut received: u64 = 0;
            let mut resumes = 0;
            loop {
                match body.next().await {
                    Some(Ok(chunk)) => {
                        received += chunk.len() as u64;
                        yield Ok(chunk);
                    }
                    Some(Err(err)) => {
                        warn!(
                            "Download of {url} dropped after {received} bytes: {}",
                            error_chain(&err)
                        );
                        match resume(&client, &url, etag.as_ref(), received, &mut resumes).await {
                            Some(rest) => body = rest,
                            None => {
                                yield Err(err);
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
        })
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

/// Reopens `url` from byte `received`, or returns None when it cannot be done
/// safely: no ETag, the file changed, or the reconnect budget is spent.
async fn resume(
    client: &Client,
    url: &str,
    etag: Option<&HeaderValue>,
    received: u64,
    resumes: &mut u32,
) -> Option<BoxStream<'static, reqwest::Result<Bytes>>> {
    let Some(etag) = etag else {
        warn!("{url} sent no ETag, so the download cannot be resumed safely.");
        return None;
    };
    while *resumes < MAX_RESUMES {
        *resumes += 1;
        tokio::time::sleep(RESUME_DELAY * *resumes).await;
        let response = match client
            .get(url)
            .header(RANGE, format!("bytes={received}-"))
            .header(IF_RANGE, etag.clone())
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                warn!(
                    "Resume {}/{MAX_RESUMES} of {url} failed: {}",
                    *resumes,
                    error_chain(&err)
                );
                continue;
            }
        };
        let starts_at_offset = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with(&format!("bytes {received}-")));
        if response.status() == StatusCode::PARTIAL_CONTENT && starts_at_offset {
            info!(
                "Resumed {url} at byte {received} (attempt {}/{MAX_RESUMES}).",
                *resumes
            );
            return Some(response.bytes_stream().boxed());
        }
        if response.status() == StatusCode::OK {
            warn!("{url} changed since the download started, so it cannot be resumed.");
            return None;
        }
        warn!(
            "Resume {}/{MAX_RESUMES} of {url} got {} instead of the rest of the file.",
            *resumes,
            response.status()
        );
    }
    warn!("Gave up on {url} after {MAX_RESUMES} resumes.");
    None
}

/// An error and every cause beneath it. reqwest's own message stops at the top
/// level ("error decoding response body"), which hides whether the connection
/// was reset, timed out or closed early.
pub(crate) fn error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const BODY: &[u8] = b"[1, 2, 3, 4, 5, 6, 7, 8]";
    const CUT: usize = 10;

    /// Answers one connection per scripted response, then closes it, and
    /// returns the request heads it saw.
    async fn serve(responses: Vec<Vec<u8>>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/AllPrintings.json",
            listener.local_addr().unwrap()
        );
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                requests.push(String::from_utf8_lossy(&buf[..n]).to_lowercase());
                socket.write_all(&response).await.unwrap();
            }
            requests
        });
        (url, handle)
    }

    /// The full-file response, cut off after `CUT` bytes as a dropped connection.
    fn dropped_after_cut(etag: Option<&str>) -> Vec<u8> {
        let etag = etag.map_or(String::new(), |e| format!("ETag: {e}\r\n"));
        let mut r = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{etag}Connection: close\r\n\r\n",
            BODY.len()
        )
        .into_bytes();
        r.extend_from_slice(&BODY[..CUT]);
        r
    }

    fn rest_of_file() -> Vec<u8> {
        let mut r = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {CUT}-{}/{}\r\nConnection: close\r\n\r\n",
            BODY.len() - CUT,
            BODY.len() - 1,
            BODY.len()
        )
        .into_bytes();
        r.extend_from_slice(&BODY[CUT..]);
        r
    }

    /// Everything the stream yielded, and whether it ended in an error.
    async fn download(url: &str) -> (Vec<u8>, bool) {
        let stream = HttpClient::new()
            .fetch_json_bytes_stream(url)
            .await
            .unwrap();
        let mut stream = Box::pin(stream);
        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => got.extend_from_slice(&chunk),
                Err(_) => return (got, true),
            }
        }
        (got, false)
    }

    #[tokio::test]
    async fn resumes_a_dropped_download_from_where_it_stopped() {
        let (url, server) = serve(vec![dropped_after_cut(Some("\"v1\"")), rest_of_file()]).await;

        let (got, failed) = download(&url).await;

        assert!(!failed);
        assert_eq!(got, BODY);
        let requests = server.await.unwrap();
        assert!(requests[1].contains(&format!("range: bytes={CUT}-")));
        assert!(requests[1].contains("if-range: \"v1\""));
    }

    #[tokio::test]
    async fn refuses_to_splice_a_file_that_changed_mid_download() {
        let mut republished = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v2\"\r\nConnection: close\r\n\r\n",
            BODY.len()
        )
        .into_bytes();
        republished.extend_from_slice(BODY);
        let (url, server) = serve(vec![dropped_after_cut(Some("\"v1\"")), republished]).await;

        let (got, failed) = download(&url).await;

        assert!(failed);
        assert_eq!(got, &BODY[..CUT]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fails_a_drop_without_an_etag_instead_of_guessing() {
        let (url, server) = serve(vec![dropped_after_cut(None)]).await;

        let (got, failed) = download(&url).await;

        assert!(failed);
        assert_eq!(got, &BODY[..CUT]);
        assert_eq!(server.await.unwrap().len(), 1);
    }
}
