use anyhow::Context;
use rand::Rng;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use sov_celestia_adapter::verifier::CelestiaVerifier;
use sov_celestia_adapter::{
    BlobReaderTrait, BlockHeaderTrait, CelestiaService, DaService, DaVerifier, SlotData,
};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;

pub enum ResultEvent {
    Submit(anyhow::Result<usize>),
    Read(anyhow::Result<usize>),
}

#[derive(Debug, Default)]
pub struct Stats {
    pub success_count: u64,
    pub error_count: u64,
    pub successful_bytes: usize,
    pub blocks_read_success: u64,
    pub block_read_error: u64,
    pub blobs_read: u64,
}

pub async fn run_submission_loop(
    celestia_service: Arc<CelestiaService>,
    finish_time: Instant,
    result_tx: mpsc::UnboundedSender<ResultEvent>,
    blob_source: Arc<BlobSource>,
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
        let blob_source = blob_source.clone();

        submission_tasks.spawn(async move {
            let result = submit_blob(&service, &blob_source, total_submission_timeout).await;
            let _ = tx.send(ResultEvent::Submit(result));
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
                .query_row(
                    "SELECT id, data FROM blobs ORDER BY id LIMIT 1",
                    [],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
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
    mut shutdown_rx: watch::Receiver<()>,
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
            res = read_block(&celestia_service, height, &verifier) => res,
            _ = shutdown_rx.changed() => break,
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
