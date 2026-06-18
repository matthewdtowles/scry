use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::info;

/// Cumulative per-table write timing for one ingest pass (scry#23 / #22).
///
/// Each price batch writes up to four tables; this attributes the wall time so a
/// single run shows where it actually goes. The two granular tables are expected
/// to dominate. Counters are atomic so timing survives the shared-`&self` write
/// path (the same reason `granular_failures` is an atomic).
#[derive(Default)]
pub(crate) struct WriteTimings {
    pub price: TableTiming,
    pub price_history: TableTiming,
    pub granular_price: TableTiming,
    pub granular_price_history: TableTiming,
}

impl WriteTimings {
    /// Log per-table totals (milliseconds and call count) at the end of a pass.
    pub fn log_summary(&self, context: &str) {
        info!(
            "{context} write totals (ms/calls): price={}/{} price_history={}/{} \
             granular_price={}/{} granular_price_history={}/{}",
            self.price.millis(),
            self.price.calls(),
            self.price_history.millis(),
            self.price_history.calls(),
            self.granular_price.millis(),
            self.granular_price.calls(),
            self.granular_price_history.millis(),
            self.granular_price_history.calls(),
        );
    }
}

/// Elapsed nanoseconds + call count for one table, accumulated across batches.
#[derive(Default)]
pub(crate) struct TableTiming {
    nanos: AtomicU64,
    calls: AtomicU64,
}

impl TableTiming {
    fn record(&self, elapsed: Duration) {
        self.nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn millis(&self) -> u64 {
        self.nanos.load(Ordering::Relaxed) / 1_000_000
    }

    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

/// Await a write future, recording its elapsed time into `timing`. Records on
/// both success and failure so best-effort granular writes still count toward
/// the total - a slow *failing* write is exactly what we want to surface.
pub(crate) async fn timed<T>(timing: &TableTiming, fut: impl Future<Output = T>) -> T {
    let start = Instant::now();
    let out = fut.await;
    timing.record(start.elapsed());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_zero() {
        let t = TableTiming::default();
        assert_eq!(t.millis(), 0);
        assert_eq!(t.calls(), 0);
    }

    #[test]
    fn accumulates_across_calls() {
        let t = TableTiming::default();
        t.record(Duration::from_millis(10));
        t.record(Duration::from_millis(5));
        assert_eq!(t.millis(), 15);
        assert_eq!(t.calls(), 2);
    }

    #[tokio::test]
    async fn timed_records_one_call_and_returns_value() {
        let t = TableTiming::default();
        let v = timed(&t, async { 42 }).await;
        assert_eq!(v, 42);
        assert_eq!(t.calls(), 1);
    }
}
