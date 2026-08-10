//! Custom Telegraf measurements emitted by this evaluator.
//!
//! These ride the *same* global metrics tracker that `init_metrics_tracker`
//! (called in `main`) installs, so they flow to the same Telegraf socket as the
//! adapter's own `sov_celestia_adapter_*` measurements — no separate pipeline.

use std::io::Write;

use sov_metrics::{Metric, track_metrics};

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
            if lines.len() >= 2 {
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
    }
}
