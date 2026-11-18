use anyhow::{bail, Context, Result};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::{Stream, StreamExt};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING};
use reqwest::Client;
use serde::de::DeserializeOwned;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio_util::io::ReaderStream;
use tracing::{info, warn};

/// How many times one download is tried, from the start, before it fails.
const DOWNLOAD_ATTEMPTS: u32 = 3;
/// Wait before each retry, multiplied by the attempt number.
const RETRY_DELAY: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(15)
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

    pub async fn all_cards_stream(&self) -> Result<impl Stream<Item = std::io::Result<Bytes>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::ALL_CARDS_URL);
        info!("Stream all cards from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn all_today_prices_stream(
        &self,
    ) -> Result<impl Stream<Item = std::io::Result<Bytes>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::TODAY_PRICES_URL);
        info!("Stream all prices from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn all_prices_stream(&self) -> Result<impl Stream<Item = std::io::Result<Bytes>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::ALL_PRICES_URL);
        info!("Stream all historical prices from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn cardkingdom_pricelist_stream(
        &self,
    ) -> Result<impl Stream<Item = std::io::Result<Bytes>>> {
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

    /// Downloads `url` to a temporary file, then streams its JSON from disk.
    ///
    /// Streaming straight from the network tied the download to the database:
    /// the parser stops reading while it saves each batch, so AllPrintings sat
    /// on an open connection for 8-9 minutes, and MTGJSON's CDN reset it part
    /// way on 2026-11-16 (17:00, 18:00 UTC) and 2026-11-17 (16:00), failing the
    /// card update each time. On disk the download runs at network speed, and a
    /// dropped one is fetched again from the start.
    ///
    /// Resuming with a Range request cannot work against MTGJSON: Cloudflare
    /// marks the full response's ETag weak, which `If-Range` never matches, and
    /// its ranges index the stored gzip file rather than the JSON it serves.
    ///
    /// Every source here sends gzip when asked - a third of the bytes for
    /// AllPrintings, a tenth for AllPricesToday - and gzip's trailer carries a
    /// CRC of the content, so a damaged file fails decoding instead of parsing.
    async fn fetch_json_bytes_stream(
        &self,
        url: &str,
    ) -> Result<impl Stream<Item = std::io::Result<Bytes>>> {
        let mut attempt = 1;
        let (file, gzip) = loop {
            match self.download(url).await {
                Ok(done) => break done,
                Err(err) if attempt < DOWNLOAD_ATTEMPTS => {
                    warn!("Download {attempt}/{DOWNLOAD_ATTEMPTS} of {url} failed: {err:#}");
                    tokio::time::sleep(RETRY_DELAY * attempt).await;
                    attempt += 1;
                }
                Err(err) => {
                    return Err(err.context(format!(
                        "giving up on {url} after {DOWNLOAD_ATTEMPTS} attempts"
                    )))
                }
            }
        };
        let file = BufReader::new(file);
        let reader: Pin<Box<dyn AsyncRead + Send>> = if gzip {
            Box::pin(GzipDecoder::new(file))
        } else {
            Box::pin(file)
        };
        Ok(ReaderStream::new(reader))
    }

    /// One attempt at `url`: its body, as sent, in a temporary file rewound to
    /// the start, and whether that body is gzip. The file has no name, so the
    /// OS removes it once it is closed, however scry exits.
    async fn download(&self, url: &str) -> Result<(tokio::fs::File, bool)> {
        let response = self
            .client
            .get(url)
            .header(ACCEPT_ENCODING, "gzip")
            .send()
            .await?
            .error_for_status()?;
        let gzip = match response.headers().get(CONTENT_ENCODING) {
            None => false,
            Some(v) if v == "gzip" => true,
            Some(other) => bail!("{url} sent an unsupported Content-Encoding: {other:?}"),
        };
        let expected = response.content_length();
        let mut file = tokio::fs::File::from_std(tempfile::tempfile()?);
        let mut received: u64 = 0;
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.with_context(|| format!("{url} dropped after {received} bytes"))?;
            file.write_all(&chunk).await?;
            received += chunk.len() as u64;
        }
        if let Some(expected) = expected.filter(|&n| n != received) {
            bail!("{url} ended after {received} of {expected} bytes");
        }
        file.flush().await?;
        file.rewind().await?;
        info!(
            "Downloaded {url}: {received} bytes{}.",
            if gzip { " (gzip)" } else { "" }
        );
        Ok((file, gzip))
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
    use async_compression::tokio::bufread::GzipEncoder;
    use tokio::io::AsyncReadExt;
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

    fn response(headers: &str, body: &[u8], content_length: usize) -> Vec<u8> {
        let mut r = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n{headers}Connection: close\r\n\r\n"
        )
        .into_bytes();
        r.extend_from_slice(body);
        r
    }

    fn whole_file() -> Vec<u8> {
        response("", BODY, BODY.len())
    }

    /// The full-file response, cut off after `CUT` bytes as a dropped connection.
    fn dropped_after_cut() -> Vec<u8> {
        response("", &BODY[..CUT], BODY.len())
    }

    async fn gzipped(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        GzipEncoder::new(data).read_to_end(&mut out).await.unwrap();
        out
    }

    /// Everything the stream yielded, or the error that ended it.
    async fn download(url: &str) -> Result<Vec<u8>> {
        let stream = HttpClient::new().fetch_json_bytes_stream(url).await?;
        let mut stream = Box::pin(stream);
        let mut got = Vec::new();
        while let Some(chunk) = stream.next().await {
            got.extend_from_slice(&chunk?);
        }
        Ok(got)
    }

    #[tokio::test]
    async fn downloads_again_from_the_start_after_a_drop() {
        let (url, server) = serve(vec![dropped_after_cut(), whole_file()]).await;

        let got = download(&url).await.unwrap();

        assert_eq!(got, BODY);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(!requests[1].contains("range:"));
    }

    #[tokio::test]
    async fn gives_up_after_the_last_attempt() {
        let drops = vec![dropped_after_cut(); DOWNLOAD_ATTEMPTS as usize];
        let (url, server) = serve(drops).await;

        let err = download(&url).await.unwrap_err();

        assert!(format!("{err:#}").contains("giving up"));
        assert_eq!(server.await.unwrap().len(), DOWNLOAD_ATTEMPTS as usize);
    }

    #[tokio::test]
    async fn asks_for_gzip_and_decodes_it() {
        let gz = gzipped(BODY).await;
        let (url, server) =
            serve(vec![response("Content-Encoding: gzip\r\n", &gz, gz.len())]).await;

        let got = download(&url).await.unwrap();

        assert_eq!(got, BODY);
        assert!(server.await.unwrap()[0].contains("accept-encoding: gzip"));
    }

    #[tokio::test]
    async fn fails_a_damaged_gzip_file_instead_of_parsing_it() {
        let mut gz = gzipped(BODY).await;
        let crc = gz.len() - 8;
        gz[crc] ^= 0xff;
        let (url, server) =
            serve(vec![response("Content-Encoding: gzip\r\n", &gz, gz.len())]).await;

        assert!(download(&url).await.is_err());
        server.await.unwrap();
    }
}
