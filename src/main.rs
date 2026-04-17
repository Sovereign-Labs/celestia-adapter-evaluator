mod core;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::core::{
    FinishCondition, ReadingLoopConfig, Stats, run_reading_loop, run_stats_collector,
    run_submission_loop,
};
use clap::{Args as ClapArgs, Parser, Subcommand};
use sov_celestia_adapter::verifier::CelestiaVerifier;
use sov_celestia_adapter::{
    CelestiaConfig, DaService, DaVerifier, MonitoringConfig, init_metrics_tracker,
};
use tokio::sync::{mpsc, watch};
use tracing_subscriber::EnvFilter;

const STATS_INTERVAL: Duration = Duration::from_secs(120);

#[derive(Parser, Debug)]
#[command(name = "celestia-adapter-evaluator", version)]
#[command(about = "Celestia Adapter Evaluator", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Submit random blobs and read them back, collecting throughput stats.
    SubmitAndRead(SubmitAndReadArgs),
    /// Read-only: sync blobs from a namespace starting at a given height.
    SyncAndRead(SyncAndReadArgs),
}

#[derive(ClapArgs, Debug)]
struct SubmitAndReadArgs {
    /// 10-byte ASCII namespace for the rollup.
    #[arg(long, value_parser = validate_namespace)]
    namespace: String,

    /// Celestia node RPC endpoint URL.
    #[arg(long)]
    rpc_endpoint: String,

    /// Celestia node gRPC endpoint URL.
    #[arg(long)]
    grpc_endpoint: String,

    /// Authentication token for the gRPC endpoint.
    #[arg(long)]
    grpc_token: Option<String>,

    /// Hex-encoded private key for signing submissions.
    #[arg(long, value_parser = validate_hex_string)]
    signer_private_key: String,

    /// Duration to run the evaluation, in seconds.
    #[arg(long)]
    run_for_seconds: u64,

    /// Minimum submitted blob size in bytes.
    #[arg(long, default_value_t = 6 * 1024 * 1024)]
    blob_size_min: usize,

    /// Maximum submitted blob size in bytes.
    #[arg(long, default_value_t = 6 * 1024 * 1024)]
    blob_size_max: usize,
}

#[derive(ClapArgs, Debug)]
struct SyncAndReadArgs {
    /// 10-byte ASCII namespace for the rollup.
    #[arg(long, value_parser = validate_namespace)]
    namespace: String,

    /// Celestia node RPC endpoint URL.
    #[arg(long)]
    rpc_endpoint: String,

    /// First block height to read (required).
    #[arg(long)]
    from_height: u64,

    /// Stop after successfully reading this height (inclusive).
    /// Mutually exclusive with --run-for-seconds.
    #[arg(long, conflicts_with = "run_for_seconds")]
    until_height: Option<u64>,

    /// Wall-clock deadline, in seconds. Mutually exclusive with --until-height.
    /// If neither is given, runs until Ctrl+C / SIGTERM.
    #[arg(long)]
    run_for_seconds: Option<u64>,
}

fn validate_namespace(s: &str) -> Result<String, String> {
    if !s.is_ascii() {
        return Err("Namespace must be an ASCII string".to_string());
    }
    if s.len() != 10 {
        return Err(format!(
            "Namespace must be exactly 10 bytes long, got {} bytes",
            s.len()
        ));
    }
    Ok(s.to_string())
}

fn validate_hex_string(s: &str) -> Result<String, String> {
    hex::decode(s)
        .map(|_| s.to_string())
        .map_err(|e| format!("Invalid hex string: {}", e))
}

fn build_rollup_params(namespace: &str) -> sov_celestia_adapter::verifier::RollupParams {
    let batch_namespace =
        sov_celestia_adapter::types::Namespace::new_v0(namespace.as_bytes()).unwrap();
    let proof_namespace = sov_celestia_adapter::types::Namespace::const_v0([1; 10]);
    sov_celestia_adapter::verifier::RollupParams {
        rollup_batch_namespace: batch_namespace,
        rollup_proof_namespace: proof_namespace,
    }
}

fn spawn_signal_listener(shutdown_sender: Arc<watch::Sender<()>>) {
    tokio::spawn(async move {
        let ctrl_c = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!(?e, "Failed to listen for Ctrl+C");
                std::future::pending::<()>().await;
            }
        };

        #[cfg(unix)]
        let terminate = async {
            use tokio::signal::unix::{SignalKind, signal};
            match signal(SignalKind::terminate()) {
                Ok(mut s) => {
                    s.recv().await;
                }
                Err(e) => {
                    tracing::error!(?e, "Failed to install SIGTERM handler");
                    std::future::pending::<()>().await;
                }
            }
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => tracing::info!("Received Ctrl+C, initiating shutdown"),
            _ = terminate => tracing::info!("Received SIGTERM, initiating shutdown"),
        }
        let _ = shutdown_sender.send(());
    });
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("debug".parse().unwrap())
                .add_directive("h2=warn".parse().unwrap())
                .add_directive("tower=warn".parse().unwrap())
                .add_directive("rustls=warn".parse().unwrap())
                .add_directive("jsonrpsee=warn".parse().unwrap())
                .add_directive("hyper=warn".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    let (shutdown_sender, mut shutdown_receiver) = watch::channel(());
    shutdown_receiver.mark_unchanged();
    let shutdown_sender = Arc::new(shutdown_sender);

    spawn_signal_listener(shutdown_sender.clone());

    let monitoring_config = MonitoringConfig::standard();
    init_metrics_tracker(&monitoring_config, shutdown_receiver.clone());

    match cli.command {
        Commands::SubmitAndRead(args) => run_submit_and_read(args, shutdown_receiver).await,
        Commands::SyncAndRead(args) => run_sync_and_read(args, shutdown_receiver).await,
    }

    let _ = shutdown_sender.send(());
}

async fn run_submit_and_read(args: SubmitAndReadArgs, shutdown_rx: watch::Receiver<()>) {
    tracing::info!("Mode: submit-and-read");
    tracing::info!("Namespace: {}", args.namespace);
    tracing::info!("Run for seconds: {}", args.run_for_seconds);

    let mut celestia_config = CelestiaConfig::minimal(args.rpc_endpoint)
        .with_submission(args.grpc_endpoint, args.signer_private_key);
    celestia_config.grpc_auth_token = args.grpc_token;
    celestia_config.backoff_max_times = 3;
    celestia_config.backoff_min_delay_ms = 1_000;
    celestia_config.backoff_max_delay_ms = 4_000;

    let params = build_rollup_params(&args.namespace);

    let celestia_service =
        sov_celestia_adapter::CelestiaService::new(celestia_config, params, shutdown_rx.clone())
            .await;

    let signer_address = celestia_service
        .get_signer()
        .await
        .expect("Signer should be set with args.signer_private_key");
    tracing::info!(%signer_address, "Used address");

    let celestia_service = Arc::new(celestia_service);
    let finish_time = Instant::now() + Duration::from_secs(args.run_for_seconds);
    let start = Instant::now();

    let (result_tx, result_rx) = mpsc::unbounded_channel();

    let max_in_flight = 64;
    let expected_worst_case_submission_time_secs = 153;
    let total_submission_timeout =
        Duration::from_secs(max_in_flight as u64 * expected_worst_case_submission_time_secs);

    let submission_handle = tokio::spawn(run_submission_loop(
        celestia_service.clone(),
        finish_time,
        result_tx.clone(),
        args.blob_size_min,
        args.blob_size_max,
        max_in_flight,
        total_submission_timeout,
    ));
    let verifier = CelestiaVerifier::new(params);
    let reading_handle = tokio::spawn(run_reading_loop(
        celestia_service,
        ReadingLoopConfig {
            start_height: None,
            finish: FinishCondition::AfterInstant(finish_time),
        },
        shutdown_rx,
        result_tx,
        verifier,
    ));
    let stats_handle = tokio::spawn(run_stats_collector(result_rx, STATS_INTERVAL));

    submission_handle.await.unwrap();
    reading_handle.await.unwrap();
    let stats = stats_handle.await.unwrap();

    print_submit_report(&stats, start.elapsed());
}

async fn run_sync_and_read(args: SyncAndReadArgs, shutdown_rx: watch::Receiver<()>) {
    if let Some(until) = args.until_height
        && args.from_height > until
    {
        eprintln!(
            "--from-height ({}) must be <= --until-height ({})",
            args.from_height, until
        );
        std::process::exit(2);
    }

    let finish = match (args.until_height, args.run_for_seconds) {
        (Some(uh), None) => FinishCondition::UntilHeight(uh),
        (None, Some(s)) => FinishCondition::AfterInstant(Instant::now() + Duration::from_secs(s)),
        (None, None) => FinishCondition::Forever,
        (Some(_), Some(_)) => unreachable!("clap conflicts_with enforces mutual exclusion"),
    };

    tracing::info!("Mode: sync-and-read");
    tracing::info!("Namespace: {}", args.namespace);
    tracing::info!(from_height = args.from_height, ?finish, "Sync parameters");

    let celestia_config = CelestiaConfig::minimal(args.rpc_endpoint);
    let params = build_rollup_params(&args.namespace);

    let celestia_service =
        sov_celestia_adapter::CelestiaService::new(celestia_config, params, shutdown_rx.clone())
            .await;
    let celestia_service = Arc::new(celestia_service);

    let start = Instant::now();
    let (result_tx, result_rx) = mpsc::unbounded_channel();

    let verifier = CelestiaVerifier::new(params);
    let reading_handle = tokio::spawn(run_reading_loop(
        celestia_service,
        ReadingLoopConfig {
            start_height: Some(args.from_height),
            finish,
        },
        shutdown_rx,
        result_tx,
        verifier,
    ));
    let stats_handle = tokio::spawn(run_stats_collector(result_rx, STATS_INTERVAL));

    reading_handle.await.unwrap();
    let stats = stats_handle.await.unwrap();

    print_sync_report(&stats, start.elapsed());
}

fn print_submit_report(stats: &Stats, elapsed: Duration) {
    tracing::info!("=== Final Stats (submit-and-read) ===");
    tracing::info!("Running time: {:.2?}", elapsed);
    let total = stats.success_count + stats.error_count;
    if total > 0 {
        let success_percent = (stats.success_count as f64 / total as f64) * 100.0;
        let error_percent = (stats.error_count as f64 / total as f64) * 100.0;
        tracing::info!(
            "Successful submission: {} ({success_percent:.2}%)",
            stats.success_count
        );
        tracing::info!(
            "Failed submission: {} ({error_percent:.2}%)",
            stats.error_count
        );
    } else {
        tracing::info!("No submissions completed");
    }
    let throughput_kib_s = stats.successful_bytes as f64 / elapsed.as_secs_f64() / 1024.0;
    tracing::info!("Throughput: {throughput_kib_s:.2} KiB/s");
    tracing::info!(
        "Blocks read: {} success, {} errors",
        stats.blocks_read_success,
        stats.block_read_error
    );
    tracing::info!("Blobs read: {}", stats.blobs_read);
}

fn print_sync_report(stats: &Stats, elapsed: Duration) {
    tracing::info!("=== Final Stats (sync-and-read) ===");
    tracing::info!("Running time: {:.2?}", elapsed);
    tracing::info!(
        "Blocks read: {} success, {} errors",
        stats.blocks_read_success,
        stats.block_read_error
    );
    tracing::info!("Blobs read: {}", stats.blobs_read);
}
