use anyhow::Context;
use rand::Rng;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use sov_celestia_adapter::verifier::CelestiaVerifier;
use sov_celestia_adapter::{
    BlobReaderTrait, BlockHeaderTrait, CelestiaService, DaService, DaVerifier,
};
use sov_shutdown::SecondaryShutdownController;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tendermint_rpc::HttpClient;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::consensus;
use crate::metrics::{
    self, ConsensusReadMeasurement, DaArchivalReadMeasurement, RecentReadMeasurement,
};

pub enum ResultEvent {
    Submit(anyhow::Result<usize>),
    Read(anyhow::Result<usize>),
    /// One historical `block_results` read on the consensus RPC.
    ConsensusRead(anyhow::Result<()>),
    /// One archival `get_block_at` read on the DA (bridge) node. `Ok` carries the
    /// number of batch blobs read at that height.
    ArchivalRead(anyhow::Result<usize>),
}

#[derive(Debug, Default)]
pub struct Stats {
    pub success_count: u64,
    pub error_count: u64,
    pub successful_bytes: usize,
    pub blocks_read_success: u64,
    pub block_read_error: u64,
    pub blobs_read: u64,
    pub consensus_reads_success: u64,
    pub consensus_reads_error: u64,
    pub da_archival_reads_success: u64,
    pub da_archival_reads_error: u64,
    pub da_archival_blobs_read: u64,
}

pub async fn run_submission_loop(
    celestia_service: Arc<CelestiaService>,
    finish_time: Instant,
    result_tx: mpsc::UnboundedSender<ResultEvent>,
    blob_source: Arc<BlobSource>,
    max_in_flight: usize,
    total_submission_timeout: std::time::Duration,
    shutdown_controller: SecondaryShutdownController,
) {
    tracing::info!(max_in_flight, "Starting submission loop");
    let mut submission_tasks = JoinSet::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(6));
    let semaphore = Arc::new(Semaphore::new(max_in_flight));

    // Record shutdown in a flag and break so we can drain in-flight submissions
    // (with a grace period) before returning.
    let mut shutting_down = false;
    while Instant::now() < finish_time {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_controller.wait_for_shutdown() => { shutting_down = true; break; }
        }

        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => permit.unwrap(),
            _ = shutdown_controller.wait_for_shutdown() => { shutting_down = true; break; }
        };
        tracing::info!(
            available_permits = semaphore.available_permits(),
            "Kicking off new submission task"
        );

        let service = celestia_service.clone();
        let tx = result_tx.clone();
        let blob_source = blob_source.clone();

        submission_tasks.spawn(async move {
            let result = submit_blob(&service, &blob_source, total_submission_timeout).await;
            let _ = tx.send(ResultEvent::Submit(result));
            drop(permit);
        });
    }

    drop(result_tx);

    if shutting_down {
        let grace = std::time::Duration::from_secs(5);
        tracing::info!(
            ?grace,
            "Shutdown requested; waiting briefly for in-flight submissions"
        );
        let drained = tokio::time::timeout(grace, async {
            while submission_tasks.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                in_flight = submission_tasks.len(),
                "Grace period elapsed; aborting remaining submission tasks"
            );
            submission_tasks.abort_all();
        }
    }

    // Final drain: no-op if already drained; quickly reaps aborted tasks otherwise.
    while submission_tasks.join_next().await.is_some() {}
}

fn generate_random_blob(blob_size_min: usize, blob_size_max: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let size = rng.gen_range(blob_size_min..=blob_size_max);
    let mut blob: Vec<u8> = vec![0u8; size];
    rng.fill(&mut blob[..]);
    blob
}

/// Source of blob payloads for the submission loop.
pub enum BlobSource {
    /// Generate random bytes with size in `[min, max]`.
    Random { min: usize, max: usize },
    /// Replay stored blobs from a `celestia-blob-downloader` SQLite DB, in `id`
    /// order, looping back to the start once the end is reached.
    Database(DbBlobSource),
}

impl BlobSource {
    fn next_blob(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            BlobSource::Random { min, max } => Ok(generate_random_blob(*min, *max)),
            BlobSource::Database(db) => db.next_blob(),
        }
    }
}

/// Cyclic, sequential reader over the `blobs` table of a downloaded SQLite DB.
///
/// Streams one blob at a time (real blobs are multi-MB and a DB can be many GB)
/// using a `last_id` cursor behind a [`Mutex`]. Submission cadence is low, so
/// serializing reads on the mutex is negligible.
pub struct DbBlobSource {
    inner: Mutex<DbCursor>,
}

struct DbCursor {
    conn: Connection,
    last_id: i64,
}

impl DbBlobSource {
    /// Open the DB read-only and validate it looks like a downloader DB.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("opening blobs DB at {}", path.display()))?;
        Self::from_conn(conn)
    }

    /// Build from an already-open connection. Shared by [`Self::open`] and tests.
    fn from_conn(conn: Connection) -> anyhow::Result<Self> {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
            .context("querying `blobs` table (is this a celestia-blob-downloader DB?)")?;
        anyhow::ensure!(count > 0, "blobs DB contains no rows");
        tracing::info!(blob_count = count, "Opened blobs DB for replay");
        Ok(Self {
            inner: Mutex::new(DbCursor { conn, last_id: 0 }),
        })
    }

    /// Return the next blob's `data`, advancing the cursor and wrapping to the
    /// first row once past the last one.
    fn next_blob(&self) -> anyhow::Result<Vec<u8>> {
        let mut cur = self.inner.lock().unwrap();
        let next = cur
            .conn
            .query_row(
                "SELECT id, data FROM blobs WHERE id > ?1 ORDER BY id LIMIT 1",
                [cur.last_id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?;
        let (id, data) = match next {
            Some(row) => row,
            None => cur
                .conn
                .query_row("SELECT id, data FROM blobs ORDER BY id LIMIT 1", [], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .context("wrapping to first blob")?,
        };
        cur.last_id = id;
        Ok(data)
    }
}

async fn submit_blob(
    celestia_service: &CelestiaService,
    blob_source: &BlobSource,
    total_submission_timeout: std::time::Duration,
) -> anyhow::Result<usize> {
    let blob = blob_source.next_blob()?;
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
    mut result_rx: mpsc::UnboundedReceiver<ResultEvent>,
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
                    Some(ResultEvent::Submit(Ok(bytes_sent))) => {
                        stats.success_count += 1;
                        stats.successful_bytes += bytes_sent;
                        tracing::info!(
                            total_success = stats.success_count,
                            total_failed = stats.error_count,
                            "Submission succeeded");
                    }
                    Some(ResultEvent::Submit(Err(error))) => {
                        stats.error_count += 1;
                        tracing::info!(
                            ?error,
                            total_success = stats.success_count,
                            total_failed = stats.error_count,
                            "Submission failed");
                    }
                    Some(ResultEvent::Read(Ok(blobs))) => {
                        stats.blocks_read_success += 1;
                        stats.blobs_read += blobs as u64;
                        tracing::info!(
                            blocks_read_success = stats.blocks_read_success,
                            blobs_read = stats.blobs_read,
                            "Block read succeeded");
                    }
                    Some(ResultEvent::Read(Err(error))) => {
                        stats.block_read_error += 1;
                        tracing::info!(
                            ?error,
                            blocks_read_success = stats.blocks_read_success,
                            block_read_error = stats.block_read_error,
                            "Block read failed");
                    }
                    Some(ResultEvent::ConsensusRead(Ok(()))) => {
                        stats.consensus_reads_success += 1;
                        tracing::debug!(
                            consensus_reads_success = stats.consensus_reads_success,
                            "Consensus block_results read succeeded");
                    }
                    Some(ResultEvent::ConsensusRead(Err(error))) => {
                        stats.consensus_reads_error += 1;
                        tracing::info!(
                            ?error,
                            consensus_reads_success = stats.consensus_reads_success,
                            consensus_reads_error = stats.consensus_reads_error,
                            "Consensus block_results read failed");
                    }
                    Some(ResultEvent::ArchivalRead(Ok(blobs))) => {
                        stats.da_archival_reads_success += 1;
                        stats.da_archival_blobs_read += blobs as u64;
                        tracing::debug!(
                            da_archival_reads_success = stats.da_archival_reads_success,
                            blobs,
                            "DA archival read succeeded");
                    }
                    Some(ResultEvent::ArchivalRead(Err(error))) => {
                        stats.da_archival_reads_error += 1;
                        tracing::info!(
                            ?error,
                            da_archival_reads_success = stats.da_archival_reads_success,
                            da_archival_reads_error = stats.da_archival_reads_error,
                            "DA archival read failed");
                    }
                    None => break,
                }
            }
            _ = interval.tick() => {
                tracing::info!(
                    success_count = stats.success_count,
                    error_count = stats.error_count,
                    successful_bytes = stats.successful_bytes,
                    blocks_read_success = stats.blocks_read_success,
                    block_read_error = stats.block_read_error,
                    blobs_read = stats.blobs_read,
                    consensus_reads_success = stats.consensus_reads_success,
                    consensus_reads_error = stats.consensus_reads_error,
                    da_archival_reads_success = stats.da_archival_reads_success,
                    da_archival_reads_error = stats.da_archival_reads_error,
                    da_archival_blobs_read = stats.da_archival_blobs_read,
                    "Periodic stats report",
                );
            }
        }
    }

    stats
}

#[derive(Debug, Clone, Copy)]
pub enum FinishCondition {
    /// Stop after successfully reading this height (inclusive).
    UntilHeight(u64),
    /// Wall-clock deadline.
    AfterInstant(Instant),
    /// Run until shutdown signal.
    Forever,
}

#[derive(Debug, Clone)]
pub struct ReadingLoopConfig {
    /// If `None`, start from current chain head + 1.
    pub start_height: Option<u64>,
    pub finish: FinishCondition,
}

pub async fn run_reading_loop(
    celestia_service: Arc<CelestiaService>,
    config: ReadingLoopConfig,
    shutdown_controller: SecondaryShutdownController,
    result_tx: mpsc::UnboundedSender<ResultEvent>,
    verifier: CelestiaVerifier,
) {
    let mut height = match config.start_height {
        Some(h) => h,
        None => {
            let header = celestia_service.get_head_block_header().await.unwrap();
            header.height().checked_add(1).unwrap()
        }
    };

    tracing::info!(
        start_height = height,
        finish = ?config.finish,
        "Starting reading loop"
    );

    loop {
        match config.finish {
            FinishCondition::AfterInstant(deadline) if Instant::now() >= deadline => break,
            FinishCondition::UntilHeight(uh) if height > uh => {
                tracing::info!(height, "Reached until_height, stopping reading loop");
                break;
            }
            _ => {}
        }

        let result = tokio::select! {
            res = recent_read_once(&celestia_service, &verifier, height) => res,
            _ = shutdown_controller.wait_for_shutdown() => break,
        };

        if let Ok(blobs) = &result {
            tracing::debug!(height, blobs, "Read block");
            height = height.checked_add(1).unwrap();
        }
        let _ = result_tx.send(ResultEvent::Read(result));
    }
}

#[tracing::instrument(skip(celestia_service, verifier), level = "debug")]
async fn read_block(
    celestia_service: &CelestiaService,
    height: u64,
    verifier: &CelestiaVerifier,
) -> anyhow::Result<usize> {
    let block = celestia_service.get_block_at(height).await?;
    let mut relevant_blobs = celestia_service.extract_relevant_blobs(&block);

    for blob in relevant_blobs
        .batch_blobs
        .iter_mut()
        .chain(relevant_blobs.proof_blobs.iter_mut())
    {
        blob.advance(blob.total_len());
    }

    let relevant_proofs = celestia_service
        .get_extraction_proof(&block, &relevant_blobs)
        .await;

    verifier.verify_relevant_tx_list(block.header(), &relevant_blobs, relevant_proofs)?;

    Ok(relevant_blobs.batch_blobs.len())
}

/// One recent-tip read via the sequential [`run_reading_loop`]: time the same
/// [`read_block`] path the archival loop uses, emit a dedicated
/// [`RecentReadMeasurement`], and return the raw result so the loop can decide
/// whether to advance its height cursor.
///
/// Mirrors [`da_read_once`], but returns the `Result` rather than a pre-wrapped
/// [`ResultEvent`]: the loop needs the outcome to advance the cursor, and it
/// already builds the `ResultEvent::Read` itself. Because the SDK's
/// `sov_celestia_adapter_get_block_*` metrics conflate this path with the DA
/// archival loop, this measurement is what lets the recent path be observed and
/// alerted on in isolation.
async fn recent_read_once(
    celestia_service: &CelestiaService,
    verifier: &CelestiaVerifier,
    height: u64,
) -> anyhow::Result<usize> {
    let start = Instant::now();
    let result = read_block(celestia_service, height, verifier).await;
    let response_time_us = start.elapsed().as_micros();

    let (is_success, blob_count) = match &result {
        Ok(blobs) => (true, *blobs as u64),
        Err(_) => (false, 0),
    };
    metrics::emit(RecentReadMeasurement {
        is_success,
        height,
        response_time_us,
        blob_count,
    });
    result
}

/// Shared configuration for the random historical / archival read loops.
#[derive(Debug, Clone, Copy)]
pub struct RandomReadConfig {
    /// Inclusive lower bound of the random height range.
    pub from_height: u64,
    /// Inclusive upper bound of the random height range.
    pub to_height: u64,
    /// Delay between kicking off successive reads.
    pub interval: Duration,
    /// Max concurrent in-flight reads.
    pub max_in_flight: usize,
    /// Optional wall-clock deadline; the loop also stops on shutdown.
    pub deadline: Option<Instant>,
}

/// Generic random-read loop. Each tick picks a uniform-random height in
/// `[from_height, to_height]` and runs `read_at(height)` for it, bounded by
/// `interval` (cadence), `max_in_flight` (concurrency), an optional wall-clock
/// deadline, and shutdown; every produced [`ResultEvent`] is forwarded to the
/// stats collector. The per-height operation — which node it hits, what it
/// measures, and which `ResultEvent` it yields — is supplied entirely by
/// `read_at`; the two implementations are [`consensus_read_once`] and
/// [`da_read_once`]. Mirrors [`run_submission_loop`]'s interval + semaphore +
/// drained `JoinSet` shape.
pub async fn run_random_read_loop<F, Fut>(
    name: &'static str,
    config: RandomReadConfig,
    shutdown_controller: SecondaryShutdownController,
    result_tx: mpsc::UnboundedSender<ResultEvent>,
    read_at: F,
) where
    F: Fn(u64) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ResultEvent> + Send + 'static,
{
    if config.from_height > config.to_height {
        tracing::warn!(
            read_loop = name,
            from = config.from_height,
            to = config.to_height,
            "Empty height range; read loop not started"
        );
        return;
    }

    tracing::info!(
        read_loop = name,
        from = config.from_height,
        to = config.to_height,
        interval_ms = config.interval.as_millis(),
        max_in_flight = config.max_in_flight,
        "Starting random read loop"
    );

    let semaphore = Arc::new(Semaphore::new(config.max_in_flight));
    let mut interval = tokio::time::interval(config.interval);
    let mut tasks = JoinSet::new();
    let mut shutting_down = false;

    loop {
        if let Some(deadline) = config.deadline
            && Instant::now() >= deadline
        {
            break;
        }

        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_controller.wait_for_shutdown() => { shutting_down = true; break; }
        }

        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => permit.unwrap(),
            _ = shutdown_controller.wait_for_shutdown() => { shutting_down = true; break; }
        };

        let height = rand::thread_rng().gen_range(config.from_height..=config.to_height);
        let fut = read_at(height);
        let tx = result_tx.clone();

        tasks.spawn(async move {
            let event = fut.await;
            let _ = tx.send(event);
            drop(permit);
        });
    }

    if shutting_down {
        tracing::info!(
            read_loop = name,
            "Shutdown requested; draining in-flight reads"
        );
    }
    while tasks.join_next().await.is_some() {}
}

/// One consensus `block_results` read: time it, emit the Telegraf measurement,
/// and map it to a [`ResultEvent::ConsensusRead`]. The consensus implementation
/// of [`run_random_read_loop`]'s `read_at`.
pub async fn consensus_read_once(http: &HttpClient, height: u64) -> ResultEvent {
    let start = Instant::now();
    let result = consensus::read_block_results(http, height).await;
    let response_time_us = start.elapsed().as_micros();

    let (is_success, num_txs, num_events, event) = match result {
        Ok(summary) => (true, summary.num_txs, summary.num_events, Ok(())),
        Err(error) => (false, 0, 0, Err(error)),
    };

    metrics::emit(ConsensusReadMeasurement {
        is_success,
        height,
        response_time_us,
        num_txs,
        num_events,
    });
    ResultEvent::ConsensusRead(event)
}

/// One archival `get_block_at` read via the adapter (the same [`read_block`]
/// path the sequential sync loop uses): time it, emit the Telegraf measurement,
/// and map it to a [`ResultEvent::ArchivalRead`]. The DA implementation of
/// [`run_random_read_loop`]'s `read_at`.
pub async fn da_read_once(
    celestia_service: &CelestiaService,
    verifier: &CelestiaVerifier,
    height: u64,
) -> ResultEvent {
    let start = Instant::now();
    let result = read_block(celestia_service, height, verifier).await;
    let response_time_us = start.elapsed().as_micros();

    let (is_success, blob_count) = match &result {
        Ok(blobs) => (true, *blobs as u64),
        Err(_) => (false, 0),
    };
    metrics::emit(DaArchivalReadMeasurement {
        is_success,
        height,
        response_time_us,
        blob_count,
    });
    ResultEvent::ArchivalRead(result)
}

#[cfg(test)]
mod tests {
    use super::DbBlobSource;
    use rusqlite::Connection;

    fn seeded_db(values: &[&[u8]]) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE blobs(id INTEGER PRIMARY KEY, data BLOB NOT NULL);")
            .unwrap();
        for data in values {
            conn.execute("INSERT INTO blobs(data) VALUES (?1)", [data])
                .unwrap();
        }
        conn
    }

    #[test]
    fn db_blob_source_reads_in_order_and_wraps() {
        let conn = seeded_db(&[&[1], &[2], &[3]]);
        let src = DbBlobSource::from_conn(conn).unwrap();

        assert_eq!(src.next_blob().unwrap(), vec![1u8]);
        assert_eq!(src.next_blob().unwrap(), vec![2u8]);
        assert_eq!(src.next_blob().unwrap(), vec![3u8]);
        // Past the last row → wrap back to the first.
        assert_eq!(src.next_blob().unwrap(), vec![1u8]);
        assert_eq!(src.next_blob().unwrap(), vec![2u8]);
    }

    #[test]
    fn db_blob_source_single_row_repeats() {
        let conn = seeded_db(&[&[42]]);
        let src = DbBlobSource::from_conn(conn).unwrap();

        assert_eq!(src.next_blob().unwrap(), vec![42u8]);
        assert_eq!(src.next_blob().unwrap(), vec![42u8]);
    }

    #[test]
    fn db_blob_source_rejects_empty_table() {
        let conn = seeded_db(&[]);
        assert!(DbBlobSource::from_conn(conn).is_err());
    }
}
