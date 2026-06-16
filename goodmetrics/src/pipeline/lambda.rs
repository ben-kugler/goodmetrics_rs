//! An immediate, unaggregated metrics pipeline for serverless / lambda environments.
//!
//! The default goodmetrics pipeline accumulates metrics into an
//! [`Aggregator`](crate::pipeline::Aggregator) and a background task drains it on
//! an interval. That model assumes the process keeps running between reports. In a
//! lambda the process is frozen the instant your handler returns, so a background
//! interval task can't reliably deliver anything.
//!
//! This module flips the model around: each `Metrics` is converted to a wire batch
//! the moment it is recorded (no time-window aggregation) and buffered. You then
//! [`flush`](LambdaFlusher::flush) the buffer at the end of your handler, which
//! sends everything downstream and awaits delivery before you return. The buffer is
//! also flushed best-effort when the [`LambdaFlusher`] is dropped.
//!
//! ```no_run
//! # async fn example() {
//! use goodmetrics::MetricsFactory;
//! use goodmetrics::allocator::AlwaysNewMetricsAllocator;
//! use goodmetrics::downstream::{get_client, GoodmetricsBatcher, GoodmetricsDownstream};
//! use goodmetrics::pipeline::{lambda_metrics, DistributionMode};
//!
//! // 1. Build a downstream as usual:
//! let downstream = GoodmetricsDownstream::new(
//!     get_client(
//!         "https://ingest.example.com",
//!         || None,
//!         goodmetrics::proto::goodmetrics::metrics_client::MetricsClient::with_origin,
//!     ).expect("channel"),
//!     Some(("authorization", "token".parse().expect("header"))),
//!     [("application", "example")],
//! );
//!
//! // 2. Wire up the immediate pipeline. Keep the factory and flusher around across
//! //    invocations (e.g. in your cold-start setup).
//! let (sink, mut flusher) =
//!     lambda_metrics(downstream, GoodmetricsBatcher, DistributionMode::Histogram);
//! let metrics_factory: MetricsFactory<AlwaysNewMetricsAllocator, _> = MetricsFactory::new(sink);
//!
//! // 3. In each invocation, record metrics then flush before returning:
//! {
//!     let mut metrics = metrics_factory.record_scope("handler");
//!     metrics.dimension("route", "/health");
//!     metrics.measurement("items", 3);
//! } // metrics is converted and buffered here, on drop
//! flusher.flush().await; // delivered before the handler returns
//! # }
//! ```

use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::allocator::MetricsRef;
use crate::downstream::MetricsSender;

use super::aggregator::aggregate_metrics_into;
use super::{AggregatedMetricsMap, AggregationBatcher, DimensionPosition, DistributionMode, Sink};

/// Shared buffer of converted-but-not-yet-sent wire batches.
type Buffer<TBatch> = Arc<Mutex<Vec<TBatch>>>;

/// A Sink that converts each Metrics into a wire batch immediately, with no
/// time-window aggregation, and buffers it until the paired LambdaFlusher sends
/// it.
pub struct LambdaSink<TBatcher: AggregationBatcher> {
    buffer: Buffer<TBatcher::TBatch>,
    batcher: Mutex<TBatcher>,
    distribution_mode: DistributionMode,
}

impl<TBatcher: AggregationBatcher> LambdaSink<TBatcher> {
    fn new(
        buffer: Buffer<TBatcher::TBatch>,
        batcher: TBatcher,
        distribution_mode: DistributionMode,
    ) -> Self {
        Self {
            buffer,
            batcher: Mutex::new(batcher),
            distribution_mode,
        }
    }
}

impl<TBatcher> Clone for LambdaSink<TBatcher>
where
    TBatcher: AggregationBatcher + Clone,
{
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
            batcher: Mutex::new(self.batcher.lock().expect("local mutex").clone()),
            distribution_mode: self.distribution_mode,
        }
    }
}

impl<TMetricsRef, TBatcher> Sink<TMetricsRef> for LambdaSink<TBatcher>
where
    TMetricsRef: MetricsRef,
    TBatcher: AggregationBatcher,
{
    fn accept(&self, to_sink: TMetricsRef) {
        // Use the recorded scope's elapsed time as the window this single batch
        // covers, so downstreams that care about start/end timestamps get a
        // sensible (non-zero) interval.
        let covered_time = to_sink.as_ref().start_time.elapsed();

        let mut map = AggregatedMetricsMap::default();
        let mut position = DimensionPosition::default();
        aggregate_metrics_into(&mut map, self.distribution_mode, &mut position, to_sink);

        let batch = self
            .batcher
            .lock()
            .expect("local mutex")
            .batch_aggregations(SystemTime::now(), covered_time, &mut map);
        self.buffer.lock().expect("local mutex").push(batch);
    }
}

/// Sends batches buffered by a [`LambdaSink`] downstream on demand.
///
/// Call [`flush`](Self::flush) at the end of each invocation to deliver everything
/// recorded so far and await delivery. Anything still buffered when the flusher is
/// dropped is flushed best-effort (see [`Drop`]).
pub struct LambdaFlusher<TSender: MetricsSender> {
    buffer: Buffer<TSender::Batch>,
    downstream: TSender,
}

impl<TSender: MetricsSender> LambdaFlusher<TSender> {
    /// Send every batch buffered so far, awaiting delivery. Cheap and safe to call
    /// when nothing is buffered, so call it unconditionally at the end of each
    /// invocation.
    pub async fn flush(&mut self) {
        let batches = std::mem::take(&mut *self.buffer.lock().expect("local mutex"));
        for batch in batches {
            self.downstream.send_batch(batch).await;
        }
    }
}

impl<TSender: MetricsSender> Drop for LambdaFlusher<TSender> {
    fn drop(&mut self) {
        let has_unflushed = self
            .buffer
            .lock()
            .map(|buffer| !buffer.is_empty())
            .unwrap_or(false);
        if !has_unflushed {
            return;
        }

        // Drop is synchronous but flushing is async. We can only drive it to
        // completion on a multi-threaded runtime via block_in_place; everywhere
        // else we warn rather than risk a panic or a silent drop. The supported
        // path is always to call flush().await yourself before returning.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| handle.block_on(self.flush()));
                }
                _ => log::warn!(
                    "LambdaFlusher dropped with unflushed metrics on a current-thread runtime; \
                     call flush().await before returning to guarantee delivery"
                ),
            },
            Err(_) => log::warn!(
                "LambdaFlusher dropped with unflushed metrics outside of a tokio runtime; \
                 these metrics were not sent"
            ),
        }
    }
}

/// Build an immediate lambda metrics pipeline: a LambdaSink to put behind a
/// MetricsFactory and a LambdaFlusher to flush it.
///
/// The two share a buffer. The `batcher` and `downstream` must agree on the wire
/// batch type, e.g. GoodmetricsBatcher with GoodmetricsDownstream,
/// or OpentelemetryBatcher with OpenTelemetryDownstream.
pub fn lambda_metrics<TSender, TBatcher>(
    downstream: TSender,
    batcher: TBatcher,
    distribution_mode: DistributionMode,
) -> (LambdaSink<TBatcher>, LambdaFlusher<TSender>)
where
    TSender: MetricsSender,
    TBatcher: AggregationBatcher<TBatch = TSender::Batch>,
{
    let buffer: Buffer<TSender::Batch> = Default::default();
    (
        LambdaSink::new(buffer.clone(), batcher, distribution_mode),
        LambdaFlusher { buffer, downstream },
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test {
    use std::sync::{Arc, Mutex};

    use crate::{
        allocator::AlwaysNewMetricsAllocator,
        downstream::{GoodmetricsBatcher, MetricsSender},
        pipeline::DistributionMode,
        proto::goodmetrics::Datum,
        MetricsFactory,
    };

    use super::{lambda_metrics, LambdaSink};

    /// A MetricsSender that just records what it was handed, so we can assert on it.
    #[derive(Default, Clone)]
    struct RecordingSender {
        sent: Arc<Mutex<Vec<Vec<Datum>>>>,
    }
    impl MetricsSender for RecordingSender {
        type Batch = Vec<Datum>;
        fn send_batch(
            &mut self,
            batch: Self::Batch,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            self.sent.lock().unwrap().push(batch);
            Box::pin(async {})
        }
    }

    #[test_log::test(tokio::test)]
    async fn records_immediately_and_flushes() {
        let downstream = RecordingSender::default();
        let sent = downstream.sent.clone();
        let (sink, mut flusher): (LambdaSink<GoodmetricsBatcher>, _) =
            lambda_metrics(downstream, GoodmetricsBatcher, DistributionMode::Histogram);
        let factory: MetricsFactory<AlwaysNewMetricsAllocator, _> = MetricsFactory::new(sink);

        // Nothing is sent until we flush, but recording buffers a batch immediately.
        {
            let mut metrics = factory.record_scope("test");
            metrics.dimension("dim", "value");
            metrics.measurement("count", 1);
        }
        assert!(sent.lock().unwrap().is_empty(), "nothing sent before flush");

        flusher.flush().await;
        let sent = sent.lock().unwrap();
        assert_eq!(1, sent.len(), "one invocation produced one batch");
        let batch = &sent[0];
        assert_eq!(1, batch.len(), "one datum for the one recording");
        assert_eq!("test", batch[0].metric);
        assert!(batch[0].measurements.contains_key("count"));
    }

    #[test_log::test(tokio::test)]
    async fn each_recording_is_its_own_unaggregated_batch() {
        let downstream = RecordingSender::default();
        let sent = downstream.sent.clone();
        let (sink, mut flusher): (LambdaSink<GoodmetricsBatcher>, _) =
            lambda_metrics(downstream, GoodmetricsBatcher, DistributionMode::Histogram);
        let factory: MetricsFactory<AlwaysNewMetricsAllocator, _> = MetricsFactory::new(sink);

        for i in 0..3 {
            let mut metrics = factory.record_scope("test");
            metrics.measurement("i", i);
        }
        flusher.flush().await;

        assert_eq!(
            3,
            sent.lock().unwrap().len(),
            "three recordings stay as three separate, unaggregated batches"
        );
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn flushes_on_drop_when_flush_is_not_called() {
        let downstream = RecordingSender::default();
        let sent = downstream.sent.clone();
        let (sink, flusher): (LambdaSink<GoodmetricsBatcher>, _) =
            lambda_metrics(downstream, GoodmetricsBatcher, DistributionMode::Histogram);
        let factory: MetricsFactory<AlwaysNewMetricsAllocator, _> = MetricsFactory::new(sink);

        {
            let mut metrics = factory.record_scope("test");
            metrics.dimension("dim", "value");
            metrics.measurement("count", 1);
        }
        assert!(
            sent.lock().unwrap().is_empty(),
            "nothing sent while buffered and flush() was never called"
        );

        // Never call flush(); dropping the flusher must deliver the buffered batch.
        drop(flusher);

        let sent = sent.lock().unwrap();
        assert_eq!(1, sent.len(), "drop flushed the buffered batch");
        assert_eq!("test", sent[0][0].metric);
        assert!(sent[0][0].measurements.contains_key("count"));
    }
}
