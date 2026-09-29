//! Custom Telegraf measurements emitted by this evaluator.
//!
//! These ride the *same* global metrics tracker that `init_metrics_tracker`
//! (called in `main`) installs, so they flow to the same Telegraf socket as the
//! adapter's own `sov_celestia_adapter_*` measurements — no separate pipeline.

use std::io::Write;

use sov_metrics::{Metric, track_metrics};

/// Version this binary was built at: the git tag when HEAD was tagged, otherwise
/// the short commit SHA (or `"unknown"` for a build with neither). Resolved by
/// `build.rs` at compile time and carried as the `version` tag on
/// [`BuildInfoMeasurement`].
pub const BUILD_VERSION: &str = env!("EVALUATOR_BUILD_VERSION");

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
    use sov_shutdown::SecondaryShutdownController;
    use std::time::Duration;
    use tokio::net::UdpSocket;

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
