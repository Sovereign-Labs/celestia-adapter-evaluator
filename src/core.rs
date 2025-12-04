use anyhow::Context;
use rand::Rng;
use sov_celestia_adapter::{CelestiaService, DaService};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

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
    max_in_flight: usize,
    total_submission_timeout: std::time::Duration,
) {
    tracing::info!(max_in_flight, "Starting submission loop");
    let mut submission_tasks = JoinSet::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(6));
    let semaphore = Arc::new(Semaphore::new(max_in_flight));

    while Instant::now() < finish_time {
        interval.tick().await;

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        tracing::info!(
            available_permits = semaphore.available_permits(),
            "Kicking off new submission task"
        );

        let service = celestia_service.clone();
        let tx = result_tx.clone();

        submission_tasks.spawn(async move {
            let result = submit_blob(
                &service,
                blob_size_min,
                blob_size_max,
                total_submission_timeout,
            )
            .await;
            let _ = tx.send(result);
            drop(permit);
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
    total_submission_timeout: std::time::Duration,
) -> anyhow::Result<usize> {
    let blob = generate_random_blob(blob_size_min, blob_size_max);
    let receiver = tokio::time::timeout(
        total_submission_timeout,
        celestia_service.send_transaction(&blob),
    )
    .await
    .context("Sending tx")?;
    let receipt = tokio::time::timeout(total_submission_timeout, receiver)
        .await
        .context("awaiting on channel receiver result")???;
    tracing::debug!(?receipt, "Receipt from sov-celestia-adapter");
    Ok(blob.len())
}

pub async fn run_stats_collector(
    mut result_rx: mpsc::UnboundedReceiver<anyhow::Result<usize>>,
    stats_interval: std::time::Duration,
) -> Stats {
    let mut stats = Stats::default();
    let mut interval = tokio::time::interval(stats_interval);
    interval.tick().await; // Skip immediate first tick

    loop {
        tokio::select! {
            biased;
            result = result_rx.recv() => {
                match result {
                    Some(Ok(bytes_sent)) => {
                        stats.success_count += 1;
                        stats.successful_bytes += bytes_sent;
                        tracing::info!(
                            total_success = stats.success_count,
                            total_failed = stats.error_count,
                            "Submission succeeded");
                    }
                    Some(Err(error)) => {
                        stats.error_count += 1;
                        tracing::info!(
                            ?error,
                            total_success = stats.success_count,
                            total_failed = stats.error_count,
                            "Submission failed");
                    }
                    None => break,
                }
            }
            _ = interval.tick() => {
                tracing::info!(
                    success_count = stats.success_count,
                    error_count = stats.error_count,
                    successful_bytes = stats.successful_bytes,
                    "Periodic stats report",
                );
            }
        }
    }

    stats
}
