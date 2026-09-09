//! Custom Telegraf measurements emitted by this evaluator.
//!
//! These ride the *same* global metrics tracker that `init_metrics_tracker`
//! (called in `main`) installs, so they flow to the same Telegraf socket as the
//! adapter's own `sov_celestia_adapter_*` measurements — no separate pipeline.

use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use sov_metrics::{Metric, track_metrics};
use sov_rollup_interface::node::SecondaryShutdownController;

/// Version this binary was built at: the git tag when HEAD was tagged, otherwise
/// the short commit SHA (or `"unknown"` for a build with neither). Resolved by
/// `build.rs` at compile time and carried as the `version` tag on
/// [`BuildInfoMeasurement`].
pub const BUILD_VERSION: &str = env!("EVALUATOR_BUILD_VERSION");

const SLO_METRICS_EMIT_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
struct HistogramBucket {
    upper_bound_seconds: f64,
    label: &'static str,
}

const HISTOGRAM_BUCKETS: [HistogramBucket; 11] = [
    HistogramBucket {
        upper_bound_seconds: 0.25,
        label: "0.25",
    },
    HistogramBucket {
        upper_bound_seconds: 0.5,
        label: "0.5",
    },
    HistogramBucket {
        upper_bound_seconds: 1.0,
        label: "1",
    },
    HistogramBucket {
        upper_bound_seconds: 2.0,
        label: "2",
    },
    HistogramBucket {
        upper_bound_seconds: 4.0,
        label: "4",
    },
    HistogramBucket {
        upper_bound_seconds: 8.0,
        label: "8",
    },
    HistogramBucket {
        upper_bound_seconds: 12.0,
        label: "12",
    },
    HistogramBucket {
        upper_bound_seconds: 16.0,
        label: "16",
    },
    HistogramBucket {
        upper_bound_seconds: 30.0,
        label: "30",
    },
    HistogramBucket {
        upper_bound_seconds: 60.0,
        label: "60",
    },
    HistogramBucket {
        upper_bound_seconds: f64::INFINITY,
        label: "+Inf",
    },
];

/// Operations covered by the evaluator's public SLO metric contract.
#[derive(Clone, Copy, Debug)]
pub enum SloOperation {
    PayForBlob,
    RecentBlockRead,
}

impl SloOperation {
    fn index(self) -> usize {
        match self {
            Self::PayForBlob => 0,
            Self::RecentBlockRead => 1,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::PayForBlob => "pay_for_blob",
            Self::RecentBlockRead => "recent_block_read",
        }
    }
}

const SLO_OPERATIONS: [SloOperation; 2] = [SloOperation::PayForBlob, SloOperation::RecentBlockRead];

#[derive(Clone, Debug, Default)]
struct OperationState {
    successes: u64,
    failures: u64,
    duration_buckets: [u64; HISTOGRAM_BUCKETS.len()],
    duration_sum_seconds: f64,
    duration_count: u64,
}

impl OperationState {
    fn record(&mut self, success: bool, duration: Duration) {
        if success {
            self.successes += 1;
        } else {
            self.failures += 1;
        }

        let duration_seconds = duration.as_secs_f64();
        self.duration_sum_seconds += duration_seconds;
        self.duration_count += 1;
        for (bucket, count) in HISTOGRAM_BUCKETS
            .iter()
            .zip(self.duration_buckets.iter_mut())
        {
            if duration_seconds <= bucket.upper_bound_seconds {
                *count += 1;
            }
        }
    }
}

#[derive(Debug, Default)]
struct SloMetrics {
    operations: Mutex<[OperationState; SLO_OPERATIONS.len()]>,
}

impl SloMetrics {
    fn record(&self, operation: SloOperation, success: bool, duration: Duration) {
        let mut operations = self.operations.lock().unwrap();
        operations[operation.index()].record(success, duration);
        emit_slo_snapshot(&operations);
    }

    fn emit(&self) {
        let operations = self.operations.lock().unwrap();
        emit_slo_snapshot(&operations);
    }
}

static SLO_METRICS: OnceLock<SloMetrics> = OnceLock::new();

/// Initialize all SLO series at zero and refresh them often enough to remain
/// present when Telegraf expires inactive input series.
pub fn initialize_slo_metrics(shutdown_controller: SecondaryShutdownController) {
    let metrics = SLO_METRICS.get_or_init(SloMetrics::default);
    metrics.emit();

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SLO_METRICS_EMIT_INTERVAL);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => metrics.emit(),
                _ = shutdown_controller.wait_for_shutdown() => break,
            }
        }
    });
}

/// Record one completed SLO operation. Work cancelled before completion never
/// calls this function and therefore does not affect either metric family.
pub fn record_slo_operation(operation: SloOperation, success: bool, duration: Duration) {
    SLO_METRICS
        .get_or_init(SloMetrics::default)
        .record(operation, success, duration);
}

#[derive(Debug)]
struct OperationCounterMeasurement {
    operation: &'static str,
    outcome: &'static str,
    total: u64,
}

impl Metric for OperationCounterMeasurement {
    fn measurement_name(&self) -> &'static str {
        "celestia_adapter_evaluator_operations"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        write!(
            buffer,
            "{},operation={},outcome={} total={}i",
            self.measurement_name(),
            self.operation,
            self.outcome,
            self.total,
        )
    }
}

#[derive(Debug)]
struct OperationDurationBucketMeasurement {
    operation: &'static str,
    le: &'static str,
    bucket: u64,
}

impl Metric for OperationDurationBucketMeasurement {
    fn measurement_name(&self) -> &'static str {
        "celestia_adapter_evaluator_operation_duration_seconds"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        write!(
            buffer,
            "{},operation={},le={} bucket={}i",
            self.measurement_name(),
            self.operation,
            self.le,
            self.bucket,
        )
    }
}

#[derive(Debug)]
struct OperationDurationSummaryMeasurement {
    operation: &'static str,
    sum: f64,
    count: u64,
}

impl Metric for OperationDurationSummaryMeasurement {
    fn measurement_name(&self) -> &'static str {
        "celestia_adapter_evaluator_operation_duration_seconds"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        write!(
            buffer,
            "{},operation={} sum={},count={}i",
            self.measurement_name(),
            self.operation,
            self.sum,
            self.count,
        )
    }
}

fn emit_slo_snapshot(operations: &[OperationState; SLO_OPERATIONS.len()]) {
    for operation in SLO_OPERATIONS {
        let state = &operations[operation.index()];
        let operation = operation.label();
        emit(OperationCounterMeasurement {
            operation,
            outcome: "success",
            total: state.successes,
        });
        emit(OperationCounterMeasurement {
            operation,
            outcome: "failure",
            total: state.failures,
        });
        for (bucket, count) in HISTOGRAM_BUCKETS.iter().zip(state.duration_buckets) {
            emit(OperationDurationBucketMeasurement {
                operation,
                le: bucket.label,
                bucket: count,
            });
        }
        emit(OperationDurationSummaryMeasurement {
            operation,
            sum: state.duration_sum_seconds,
            count: state.duration_count,
        });
    }
}

/// The running binary's build version, following the Prometheus `*_build_info`
/// convention: the useful information rides as a *tag* (`version`) and the field
/// is a constant `1`, so `evaluator_build_info{version="..."}` is a queryable
/// label with an always-1 value.
///
/// Line protocol:
/// `evaluator_build,version=<v> info=1i`
///
/// The measurement is `evaluator_build` with an `info` field (not measurement
/// `evaluator_build_info` with a `value` field): Telegraf's `metric_version = 2`
/// renders `measurement,tag field=val` as the Prometheus series
/// `measurement_field{tag=...}`, so this split yields exactly the conventional
/// `evaluator_build_info{version=...}` — whereas a `value` field would suffix it
/// to the non-standard `evaluator_build_info_value`.
///
/// Unlike the read measurements, nothing else emits this, so it must be re-sent
/// on an interval shorter than Telegraf's `expiration_interval` (90s locally) or
/// the series is expired from the scrape endpoint and disappears from Prometheus.
#[derive(Debug)]
pub struct BuildInfoMeasurement {
    pub version: &'static str,
}

impl Metric for BuildInfoMeasurement {
    fn measurement_name(&self) -> &'static str {
        "evaluator_build"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        let name = self.measurement_name();
        let version = self.version;
        write!(buffer, "{name},version={version} info=1i")
    }
}

/// One historical `block_results` read on the consensus RPC.
///
/// Line protocol:
/// `evaluator_consensus_block_results,is_success=<0|1> response_time_us=..,height=..,num_txs=..,num_events=..`
#[derive(Debug)]
pub struct ConsensusReadMeasurement {
    pub is_success: bool,
    pub height: u64,
    pub response_time_us: u128,
    pub num_txs: u64,
    pub num_events: u64,
}

impl Metric for ConsensusReadMeasurement {
    fn measurement_name(&self) -> &'static str {
        "evaluator_consensus_block_results"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        let name = self.measurement_name();
        let is_success = u8::from(self.is_success);
        let height = self.height;
        let response_time_us = self.response_time_us;
        let num_txs = self.num_txs;
        let num_events = self.num_events;
        write!(
            buffer,
            "{name},is_success={is_success} response_time_us={response_time_us},height={height},num_txs={num_txs},num_events={num_events}",
        )
    }
}

/// One archival `get_block_at` read on the DA (bridge) node via the adapter.
///
/// Line protocol:
/// `evaluator_da_archival_read,is_success=<0|1> response_time_us=..,height=..,blob_count=..`
///
/// Complements the adapter's own `sov_celestia_adapter_get_block` (emitted on
/// success only): this measurement also records *failed* reads, so a dashboard
/// can show archival read success rate.
#[derive(Debug)]
pub struct DaArchivalReadMeasurement {
    pub is_success: bool,
    pub height: u64,
    pub response_time_us: u128,
    pub blob_count: u64,
}

impl Metric for DaArchivalReadMeasurement {
    fn measurement_name(&self) -> &'static str {
        "evaluator_da_archival_read"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        let name = self.measurement_name();
        let is_success = u8::from(self.is_success);
        let height = self.height;
        let response_time_us = self.response_time_us;
        let blob_count = self.blob_count;
        write!(
            buffer,
            "{name},is_success={is_success} response_time_us={response_time_us},height={height},blob_count={blob_count}",
        )
    }
}

/// One recent-tip `get_block_at` read via the sequential reading loop.
///
/// Line protocol:
/// `evaluator_recent_read,is_success=<0|1> response_time_us=..,height=..,blob_count=..`
///
/// The recent reader shares the SDK's `sov_celestia_adapter_get_block_*` metrics
/// with the DA archival loop (both go through `get_block_at`), so those cannot
/// tell recent tip reads apart from random historical ones. This dedicated
/// measurement isolates the sequential reader: `height` is monotonic per
/// success, and — unlike the SDK metric, which is emitted on success only — it
/// also records *failed* reads via `is_success`, so freshness/latency alerts and
/// dashboards can key off the recent path alone.
#[derive(Debug)]
pub struct RecentReadMeasurement {
    pub is_success: bool,
    pub height: u64,
    pub response_time_us: u128,
    pub blob_count: u64,
}

impl Metric for RecentReadMeasurement {
    fn measurement_name(&self) -> &'static str {
        "evaluator_recent_read"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        let name = self.measurement_name();
        let is_success = u8::from(self.is_success);
        let height = self.height;
        let response_time_us = self.response_time_us;
        let blob_count = self.blob_count;
        write!(
            buffer,
            "{name},is_success={is_success} response_time_us={response_time_us},height={height},blob_count={blob_count}",
        )
    }
}

/// Submit a measurement to the global tracker. A no-op if the tracker was never
/// initialized (metrics are lossy by design).
pub fn emit<M: Metric + 'static>(measurement: M) {
    track_metrics(|tracker| tracker.submit(measurement));
}

#[cfg(test)]
mod tests {
    use super::*;
    use sov_metrics::{MonitoringConfig, init_metrics_tracker};
    use sov_rollup_interface::node::SecondaryShutdownController;
    use std::time::Duration;
    use tokio::net::UdpSocket;

    fn serialize(measurement: &impl Metric) -> String {
        let mut buffer = Vec::new();
        measurement.serialize_for_telegraf(&mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    #[test]
    fn slo_accumulator_counts_outcomes_and_cumulative_buckets() {
        let mut state = OperationState::default();
        state.record(true, Duration::from_millis(400));
        state.record(false, Duration::from_secs(13));

        assert_eq!(state.successes, 1);
        assert_eq!(state.failures, 1);
        assert_eq!(state.duration_count, 2);
        assert!((state.duration_sum_seconds - 13.4).abs() < f64::EPSILON);
        assert_eq!(state.duration_buckets[0], 0);
        assert_eq!(state.duration_buckets[1], 1);
        assert_eq!(state.duration_buckets[6], 1);
        assert_eq!(state.duration_buckets[7], 2);
        assert_eq!(state.duration_buckets[10], state.duration_count);
    }

    #[test]
    fn slo_measurements_have_the_prometheus_contract_shape() {
        assert_eq!(
            serialize(&OperationCounterMeasurement {
                operation: "pay_for_blob",
                outcome: "failure",
                total: 3,
            }),
            "celestia_adapter_evaluator_operations,operation=pay_for_blob,outcome=failure total=3i"
        );
        assert_eq!(
            serialize(&OperationDurationBucketMeasurement {
                operation: "recent_block_read",
                le: "12",
                bucket: 7,
            }),
            "celestia_adapter_evaluator_operation_duration_seconds,operation=recent_block_read,le=12 bucket=7i"
        );
        assert_eq!(
            serialize(&OperationDurationSummaryMeasurement {
                operation: "recent_block_read",
                sum: 8.5,
                count: 2,
            }),
            "celestia_adapter_evaluator_operation_duration_seconds,operation=recent_block_read sum=8.5,count=2i"
        );
    }

    /// End-to-end proof that a custom measurement reaches the Telegraf socket
    /// through the same global tracker the adapter installs, with exactly the
    /// line-protocol bytes the dashboard's Prometheus names are derived from.
    #[tokio::test]
    async fn measurements_reach_the_telegraf_socket() {
        // Stand in for Telegraf on an ephemeral port and point the tracker at it.
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();

        let shutdown = SecondaryShutdownController::new();
        init_metrics_tracker(&MonitoringConfig::default_on_port(port), &shutdown);

        emit(ConsensusReadMeasurement {
            is_success: true,
            height: 451233,
            response_time_us: 8123,
            num_txs: 14,
            num_events: 52,
        });
        emit(DaArchivalReadMeasurement {
            is_success: false,
            height: 88771,
            response_time_us: 204100,
            blob_count: 0,
        });
        emit(RecentReadMeasurement {
            is_success: true,
            height: 451234,
            response_time_us: 6123,
            blob_count: 3,
        });
        emit(BuildInfoMeasurement { version: "v1.2.3" });

        // The publisher batches into ~508-byte datagrams and only flushes on fill
        // or shutdown; two small metrics won't fill it, so trigger the shutdown
        // drain to force the buffered metrics onto the socket now.
        shutdown.shutdown();

        // Collect datagrams for a moment (order/batching is up to the publisher).
        let mut lines = Vec::new();
        let mut buf = [0u8; 4096];
        while let Ok(Ok(n)) =
            tokio::time::timeout(Duration::from_secs(5), socket.recv(&mut buf)).await
        {
            for line in std::str::from_utf8(&buf[..n]).unwrap().lines() {
                lines.push(line.to_string());
            }
            if lines.len() >= 4 {
                break;
            }
        }

        let consensus = lines
            .iter()
            .find(|l| l.starts_with("evaluator_consensus_block_results"))
            .expect("consensus measurement never arrived at the socket");
        let archival = lines
            .iter()
            .find(|l| l.starts_with("evaluator_da_archival_read"))
            .expect("DA archival measurement never arrived at the socket");
        let recent = lines
            .iter()
            .find(|l| l.starts_with("evaluator_recent_read"))
            .expect("recent read measurement never arrived at the socket");
        let build_info = lines
            .iter()
            .find(|l| l.starts_with("evaluator_build,"))
            .expect("build info measurement never arrived at the socket");

        // Telegraf appends a timestamp; assert the measurement/tag/field prefix.
        assert!(
            consensus.starts_with(
                "evaluator_consensus_block_results,is_success=1 \
                 response_time_us=8123,height=451233,num_txs=14,num_events=52"
            ),
            "unexpected consensus line: {consensus}"
        );
        assert!(
            archival.starts_with(
                "evaluator_da_archival_read,is_success=0 \
                 response_time_us=204100,height=88771,blob_count=0"
            ),
            "unexpected archival line: {archival}"
        );
        assert!(
            recent.starts_with(
                "evaluator_recent_read,is_success=1 \
                 response_time_us=6123,height=451234,blob_count=3"
            ),
            "unexpected recent line: {recent}"
        );
        assert!(
            build_info.starts_with("evaluator_build,version=v1.2.3 info=1i"),
            "unexpected build info line: {build_info}"
        );
    }
}
