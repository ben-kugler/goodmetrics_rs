// For use in monitoring environments where preaggregation is not necessary; lambda env
// require no preaggregation, instead we'd like to send metrics on a per-record basis.
// Each recorded Metrics becomes its own batch and each measurement is sent as an
// individual datapoint.
//
//! ```no_run
//! # async fn example() {
//! use goodmetrics::MetricsFactory;
//! use goodmetrics::allocator::AlwaysNewMetricsAllocator;
//! use goodmetrics::downstream::{get_client, OpentelemetryBatcher, OpenTelemetryDownstream};
//! use goodmetrics::pipeline::{lambda_metrics, DistributionMode};
//!
//! // 1. Build a downstream as usual:
//! let downstream = OpenTelemetryDownstream::new_with_dimensions(
//!     get_client(
//!         "https://ingest.example.com",
//!         || None,
//!         goodmetrics::proto::opentelemetry::collector::metrics::v1::metrics_service_client::MetricsServiceClient::with_origin,
//!     ).expect("channel"),
//!     Some(("authorization", "token".parse().expect("header"))),
//!     [("application", "example")],
//! );
//!
//! // 2. Wire up the immediate pipeline. Keep the factory and flusher around across
//! //    invocations (e.g. in your cold-start setup).
//! let (sink, mut flusher) =
//!     lambda_metrics(downstream, OpentelemetryBatcher, DistributionMode::Histogram);
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
use crate::types::Name;

use super::{DimensionPosition, DistributionMode, MetricsBatcher, Sink};

/// Shared buffer of converted-but-not-yet-sent wire batches.
type Buffer<TBatch> = Arc<Mutex<Vec<TBatch>>>;

/// A Sink that converts each Metrics into a wire batch immediately, with no
/// time-window aggregation, buffers until the paired LambdaFlusher sends
/// it.
pub struct LambdaSink<TBatcher: MetricsBatcher> {
    buffer: Buffer<TBatcher::TBatch>,
    batcher: Mutex<TBatcher>,
    distribution_mode: DistributionMode,
}

impl<TBatcher: MetricsBatcher> LambdaSink<TBatcher> {
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
    TBatcher: MetricsBatcher + Clone,
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
    TBatcher: MetricsBatcher,
{
    fn accept(&self, mut to_sink: TMetricsRef) {
        // Use the recorded scope's elapsed time as the window for single batchs
        let covered_time = to_sink.as_ref().start_time.elapsed();

        let metrics = to_sink.as_mut();
        let name = std::mem::replace(&mut metrics.metrics_name, Name::Str("_uninitialized_"));
        let (dimensions, measurements) = metrics.drain();
        let dimensions: DimensionPosition = dimensions.drain().collect();
        let measurements: Vec<_> = measurements.drain().collect();

        let batch = self
            .batcher
            .lock()
            .expect("local mutex")
            .batch_unaggregated(
                SystemTime::now(),
                covered_time,
                self.distribution_mode,
                name,
                dimensions,
                measurements,
            );
        self.buffer.lock().expect("local mutex").push(batch);
    }
}

/// Sends batches buffered by a LambdaSink downstream on demand.
pub struct LambdaFlusher<TSender: MetricsSender> {
    buffer: Buffer<TSender::Batch>,
    downstream: TSender,
}

impl<TSender: MetricsSender> LambdaFlusher<TSender> {
    /// Send every batch buffered so far, awaiting delivery.=
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
        if has_unflushed {
            log::error!(
                "LambdaFlusher dropped with unflushed metrics, \
                call flush().await before returning to guarantee delivery"
            )
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
    TBatcher: MetricsBatcher<TBatch = TSender::Batch>,
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
        downstream::{GoodmetricsBatcher, MetricsSender, OpentelemetryBatcher},
        pipeline::DistributionMode,
        proto::{goodmetrics::Datum, opentelemetry::metrics::v1::Metric},
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

    /// As [`RecordingSender`], but for the opentelemetry wire type.
    #[derive(Default, Clone)]
    struct RecordingOtelSender {
        sent: Arc<Mutex<Vec<Vec<Metric>>>>,
    }
    impl MetricsSender for RecordingOtelSender {
        type Batch = Vec<Metric>;
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

    #[test_log::test(tokio::test)]
    async fn opentelemetry_observation_is_a_single_raw_gauge() {
        use crate::proto::opentelemetry::metrics::v1::metric::Data;

        let downstream = RecordingOtelSender::default();
        let sent = downstream.sent.clone();
        let (sink, mut flusher): (LambdaSink<OpentelemetryBatcher>, _) = lambda_metrics(
            downstream,
            OpentelemetryBatcher,
            DistributionMode::Histogram,
        );
        let factory: MetricsFactory<AlwaysNewMetricsAllocator, _> = MetricsFactory::new(sink);

        {
            let mut metrics = factory.record_scope("handler");
            metrics.measurement("items", 3);
        }
        flusher.flush().await;

        let sent = sent.lock().unwrap();
        assert_eq!(1, sent.len(), "one invocation produced one batch");
        let batch = &sent[0];

        // The observation must be one raw gauge, not expanded into a four-part
        // min/max/sum/count statistic set as the aggregating pipeline would do.
        // (record_scope also records a `totaltime` distribution, so the batch has
        // more than just this metric.)
        let items: Vec<_> = batch
            .iter()
            .filter(|m| m.name.starts_with("handler_items"))
            .collect();
        assert_eq!(
            vec!["handler_items"],
            items.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
            "the observation is one metric named handler_items, with no _min/_max/_sum/_count"
        );
        match items[0].data.as_ref().expect("data") {
            Data::Gauge(gauge) => {
                assert_eq!(
                    1,
                    gauge.data_points.len(),
                    "one datapoint for the one value"
                );
                use crate::proto::opentelemetry::metrics::v1::number_data_point::Value;
                assert!(
                    matches!(gauge.data_points[0].value, Some(Value::AsInt(3))),
                    "the raw recorded value is preserved as an integer gauge"
                );
            }
            other => panic!("observation should become a gauge, got {other:?}"),
        }
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
