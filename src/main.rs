mod core;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::core::{run_stats_collector, run_submission_loop};
use clap::Parser;
use sov_celestia_adapter::{CelestiaConfig, MonitoringConfig, init_metrics_tracker};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "celestia-adapter-evaluator")]
#[command(about = "Celestia Adapter Evaluator", long_about = None)]
struct Args {
    #[arg(long, value_parser = validate_namespace)]
    namespace: String,

    #[arg(long)]
    rpc_endpoint: String,

    #[arg(long)]
    grpc_endpoint: String,

    #[arg(long)]
    grpc_token: Option<String>,

    #[arg(long, value_parser = validate_hex_string)]
    signer_private_key: String,

    #[arg(long)]
    run_for_seconds: u64,

    #[arg(long, default_value_t = 6 * 1024 * 1024)]
    blob_size_min: usize,

    #[arg(long, default_value_t = 6 * 1024 * 1024)]
    blob_size_max: usize,
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

    let args = Args::parse();

    let (shutdown_sender, mut shutdown_receiver) = tokio::sync::watch::channel(());
    shutdown_receiver.mark_unchanged();

    let monitoring_config = MonitoringConfig::standard();
    init_metrics_tracker(&monitoring_config, shutdown_receiver);

    tracing::info!("Namespace: {}", args.namespace);
    tracing::info!("RPC Endpoint: {}", args.rpc_endpoint);
    tracing::info!("gRPC Endpoint: {}", args.grpc_endpoint);
    // println!("Signer Private Key: {}", args.signer_private_key);
    tracing::info!("Run for seconds: {}", args.run_for_seconds);

    let mut celestia_config = CelestiaConfig::minimal(args.rpc_endpoint)
        .with_submission(args.grpc_endpoint, args.signer_private_key);
    celestia_config.grpc_auth_token = args.grpc_token;
    celestia_config.backoff_max_times = 3;
    celestia_config.backoff_min_delay_ms = 1_000;
    celestia_config.backoff_max_delay_ms = 4_000;

    let batch_namespace =
        sov_celestia_adapter::types::Namespace::new_v0(args.namespace.as_bytes()).unwrap();
    let proof_namespace = sov_celestia_adapter::types::Namespace::const_v0([1; 10]);
    let params = sov_celestia_adapter::verifier::RollupParams {
        rollup_batch_namespace: batch_namespace,
        rollup_proof_namespace: proof_namespace,
    };

    let celestia_service =
        sov_celestia_adapter::CelestiaService::new(celestia_config, params).await;

    let celestia_service = Arc::new(celestia_service);
    let finish_time = Instant::now() + Duration::from_secs(args.run_for_seconds);

    let (result_tx, result_rx) = mpsc::unbounded_channel();

    let start = std::time::Instant::now();
    let submission_handle = tokio::spawn(run_submission_loop(
        celestia_service,
        finish_time,
        result_tx,
        args.blob_size_min,
        args.blob_size_max,
    ));
    let stats_handle = tokio::spawn(run_stats_collector(result_rx));

    submission_handle.await.unwrap();
    let stats = stats_handle.await.unwrap();

    tracing::info!("=== Final Stats ===");
    tracing::info!("Running time: {:.2?}", start.elapsed());
    let total = stats.success_count + stats.error_count;
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
    let throughput_kib_s = stats.successful_bytes as f64 / start.elapsed().as_secs_f64() / 1024.0;
    tracing::info!("Throughput: {throughput_kib_s:.2} KiB/s");

    shutdown_sender.send(()).unwrap();
}
