use anyhow::Context;
use rand::Rng;
use sov_celestia_adapter::{CelestiaService, DaService};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

const TOTAL_SUBMISSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Debug, Default)]
pub struct Stats {
    pub success_count: u64,
    pub error_count: u64,
    pub successful_bytes: usize,
}

pub async fn run_submission_loop(
    celestia_service: Arc<CelestiaService>,
    finish_time: Instant,
    result_tx: mpsc::UnboundedSender<anyhow::Result<usize>>,
    blob_size_min: usize,
    blob_size_max: usize,
) {
    tracing::info!("Starting submission loop");
    let mut submission_tasks = JoinSet::new();
    let mut interval = tokio::time::interval(Duration::from_secs(6));

    while Instant::now() < finish_time {
        interval.tick().await;
        tracing::info!("Kicking of new submission task");

        let service = celestia_service.clone();
        let tx = result_tx.clone();

        submission_tasks.spawn(async move {
            let result = submit_blob(&service, blob_size_min, blob_size_max).await;
            let _ = tx.send(result);
        });
    }

    drop(result_tx);

    while submission_tasks.join_next().await.is_some() {}
}

fn generate_random_blob(blob_size_min: usize, blob_size_max: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let size = rng.gen_range(blob_size_min..=blob_size_max);
    let mut blob: Vec<u8> = vec![0u8; size];
    rng.fill(&mut blob[..]);
    blob
}

async fn submit_blob(
    celestia_service: &CelestiaService,
    blob_size_min: usize,
    blob_size_max: usize,
) -> anyhow::Result<usize> {
    let blob = generate_random_blob(blob_size_min, blob_size_max);
    let receiver = tokio::time::timeout(
        TOTAL_SUBMISSION_TIMEOUT,
        celestia_service.send_transaction(&blob),
    )
    .await
    .context("Sending tx")?;
    let receipt = tokio::time::timeout(TOTAL_SUBMISSION_TIMEOUT, receiver)
        .await
        .context("awaiting on channel receiver result")???;
    tracing::debug!(?receipt, "Receipt from sov-celestia-adapter");
    Ok(blob.len())
}

pub async fn run_stats_collector(
    mut result_rx: mpsc::UnboundedReceiver<anyhow::Result<usize>>,
) -> Stats {
    let mut stats = Stats::default();

    while let Some(result) = result_rx.recv().await {
        match result {
            Ok(bytes_sent) => {
                stats.success_count += 1;
                stats.successful_bytes += bytes_sent;
                tracing::info!(total = stats.success_count, "Submission succeeded");
            }
            Err(e) => {
                stats.error_count += 1;
                tracing::info!(
                    error = %e,
                    total = stats.error_count,
                    "Submission failed",
                );
            }
        }
    }

    stats
}
