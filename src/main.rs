mod core;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::core::{
    BlobSource, DbBlobSource, FinishCondition, ReadingLoopConfig, Stats, run_reading_loop,
    run_stats_collector, run_submission_loop,
};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use sov_celestia_adapter::verifier::CelestiaVerifier;
use sov_celestia_adapter::{
    CelestiaConfig, CompressOnSubmit, DaService, DaVerifier, MonitoringConfig, init_metrics_tracker,
};
use sov_rollup_interface::node::SecondaryShutdownController;
use tokio::sync::mpsc;
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

/// CLI selector mirroring [`CompressOnSubmit`]. Kept separate so we can derive
/// `ValueEnum` without touching the SDK type.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum CompressionArg {
    /// Post batch blobs verbatim.
    Off,
    /// Wrap batch blobs in a chunked LZ4 envelope when it is smaller than raw.
    Lz4,
}

impl From<CompressionArg> for CompressOnSubmit {
    fn from(arg: CompressionArg) -> Self {
        match arg {
            CompressionArg::Off => CompressOnSubmit::Off,
            CompressionArg::Lz4 => CompressOnSubmit::Lz4,
        }
    }
}

#[derive(ClapArgs, Debug)]
#[command(group(
    clap::ArgGroup::new("signer")
        .required(true)
        .args(["signer_private_key", "signer_mnemonic"])
))]
struct SubmitAndReadArgs {
    /// 10-byte ASCII namespace for the rollup.
    #[arg(long, value_parser = validate_namespace)]
    namespace: String,

    /// Celestia node RPC endpoint URL.
    #[arg(long)]
    rpc_endpoint: String,

    /// Authentication token for the RPC endpoint.
    #[arg(long)]
    rpc_token: Option<String>,

    /// Celestia node gRPC endpoint URL.
    #[arg(long)]
    grpc_endpoint: String,

    /// Authentication token for the gRPC endpoint.
    #[arg(long)]
    grpc_token: Option<String>,

    /// Hex-encoded private key for signing submissions.
    #[arg(long, value_parser = validate_hex_string)]
    signer_private_key: Option<String>,

    /// BIP39 mnemonic (12 or 24 words). Derived via m/44'/118'/0'/0/0.
    #[arg(long)]
    signer_mnemonic: Option<String>,

    /// Duration to run the evaluation, in seconds.
    #[arg(long)]
    run_for_seconds: u64,

    /// Minimum submitted blob size in bytes.
    #[arg(long, default_value_t = 6 * 1024 * 1024)]
    blob_size_min: usize,

    /// Maximum submitted blob size in bytes.
    #[arg(long, default_value_t = 6 * 1024 * 1024)]
    blob_size_max: usize,

    /// Path to a celestia-blob-downloader SQLite DB. When set, replay the
    /// stored blobs sequentially (looping at the end) instead of submitting
    /// random data. The `--blob-size-*` flags are ignored in this mode.
    #[arg(long)]
    blobs_db: Option<std::path::PathBuf>,

    /// Batch-blob compression mode on submit. When omitted, the
    /// `CelestiaConfig` default (currently `off`) is used.
    #[arg(long, value_enum)]
    compression: Option<CompressionArg>,

    /// Target uncompressed chunk size (bytes) for the LZ4 compression envelope.
    /// Only has an effect together with `--compression lz4`. When omitted, the
    /// `CelestiaConfig` default (currently 482, one continuation share) is used.
    /// Out-of-range values (valid range is 1..=16384) are rejected by the adapter
    /// at startup.
    #[arg(long)]
    compression_chunk_size: Option<usize>,
}

#[derive(ClapArgs, Debug)]
struct SyncAndReadArgs {
    /// 10-byte ASCII namespace for the rollup.
    #[arg(long, value_parser = validate_namespace)]
    namespace: String,

    /// Celestia node RPC endpoint URL.
    #[arg(long)]
    rpc_endpoint: String,

    /// Authentication token for the RPC endpoint.
    #[arg(long)]
    rpc_token: Option<String>,

    /// First block height to read.
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

fn mnemonic_to_hex(phrase: &str) -> anyhow::Result<String> {
    let normalized = phrase.split_whitespace().collect::<Vec<_>>().join(" ");
    let mnemonic = bip39::Mnemonic::parse(&normalized)
        .map_err(|e| anyhow::anyhow!("invalid BIP39 mnemonic: {e}"))?;
    let seed = mnemonic.to_seed("");
    let path: bip32::DerivationPath = "m/44'/118'/0'/0/0"
        .parse()
        .expect("static HD path is valid");
    let xprv = bip32::XPrv::derive_from_path(seed, &path)
        .map_err(|e| anyhow::anyhow!("HD derivation failed: {e}"))?;
    Ok(hex::encode(xprv.private_key().to_bytes()))
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

fn spawn_signal_listener(shutdown_controller: SecondaryShutdownController) {
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
        shutdown_controller.shutdown();
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

    let shutdown_controller = SecondaryShutdownController::new();

    spawn_signal_listener(shutdown_controller.clone());

    let monitoring_config = MonitoringConfig::standard();
    let _ = init_metrics_tracker(&monitoring_config, &shutdown_controller);

    match cli.command {
        Commands::SubmitAndRead(args) => run_submit_and_read(args, &shutdown_controller).await,
        Commands::SyncAndRead(args) => run_sync_and_read(args, &shutdown_controller).await,
    }

    shutdown_controller.shutdown();
}

async fn run_submit_and_read(args: SubmitAndReadArgs, shutdown_controller: &SecondaryShutdownController) {
    tracing::info!("Mode: submit-and-read");
    tracing::info!("Namespace: {}", args.namespace);
    tracing::info!("Run for seconds: {}", args.run_for_seconds);

    let signer_key_hex = match (args.signer_private_key, args.signer_mnemonic) {
        (Some(hex), None) => hex,
        (None, Some(m)) => match mnemonic_to_hex(&m) {
            Ok(hex) => hex,
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        },
        _ => unreachable!("ArgGroup enforces exactly one signer flag"),
    };

    let mut celestia_config = CelestiaConfig::minimal(args.rpc_endpoint)
        .with_submission(args.grpc_endpoint, signer_key_hex);
    celestia_config.rpc_auth_token = args.rpc_token;
    celestia_config.grpc_auth_token = args.grpc_token;
    celestia_config.backoff_max_times = 3;
    celestia_config.backoff_min_delay_ms = 1_000;
    celestia_config.backoff_max_delay_ms = 4_000;
    if let Some(compression) = args.compression {
        celestia_config.compression = compression.into();
    }
    if let Some(chunk_size) = args.compression_chunk_size {
        celestia_config.compression_chunk_size = chunk_size;
    }

    let params = build_rollup_params(&args.namespace);

    let celestia_service =
        sov_celestia_adapter::CelestiaService::new(celestia_config, params, shutdown_controller)
            .await;

    let signer_address = celestia_service
        .get_signer()
        .await
        .expect("Signer should be set from --signer-private-key or --signer-mnemonic");
    tracing::info!(%signer_address, "Used address");

    let celestia_service = Arc::new(celestia_service);
    let finish_time = Instant::now() + Duration::from_secs(args.run_for_seconds);
    let start = Instant::now();

    let (result_tx, result_rx) = mpsc::unbounded_channel();

    let max_in_flight = 64;
    let expected_worst_case_submission_time_secs = 153;
    let total_submission_timeout =
        Duration::from_secs(max_in_flight as u64 * expected_worst_case_submission_time_secs);

    let blob_source = match args.blobs_db {
        Some(path) => match DbBlobSource::open(&path) {
            Ok(db) => {
                tracing::info!(path = %path.display(), "Replaying blobs from downloaded DB");
                Arc::new(BlobSource::Database(db))
            }
            Err(e) => {
                eprintln!("Failed to open --blobs-db: {e:#}");
                std::process::exit(2);
            }
        },
        None => Arc::new(BlobSource::Random {
            min: args.blob_size_min,
            max: args.blob_size_max,
        }),
    };

    let submission_handle = tokio::spawn(run_submission_loop(
        celestia_service.clone(),
        finish_time,
        result_tx.clone(),
        blob_source,
        max_in_flight,
        total_submission_timeout,
        shutdown_controller.clone(),
    ));
    let verifier = CelestiaVerifier::new(params);
    let reading_handle = tokio::spawn(run_reading_loop(
        celestia_service,
        ReadingLoopConfig {
            start_height: None,
            finish: FinishCondition::AfterInstant(finish_time),
        },
        shutdown_controller.clone(),
        result_tx,
        verifier,
    ));
    let stats_handle = tokio::spawn(run_stats_collector(result_rx, STATS_INTERVAL));

    submission_handle.await.unwrap();
    reading_handle.await.unwrap();
    let stats = stats_handle.await.unwrap();

    print_submit_report(&stats, start.elapsed());
}

async fn run_sync_and_read(args: SyncAndReadArgs, shutdown_controller: &SecondaryShutdownController) {
    if let Some(until) = args.until_height
        && args.from_height > until
    {
        eprintln!(
            "--from-height ({}) must be <= --until-height ({})",
            args.from_height, until
        );
        std::process::exit(2);
    }

    tracing::info!("Mode: sync-and-read");
    tracing::info!("Namespace: {}", args.namespace);

    let mut celestia_config = CelestiaConfig::minimal(args.rpc_endpoint);
    celestia_config.rpc_auth_token = args.rpc_token;
    let params = build_rollup_params(&args.namespace);

    let celestia_service =
        sov_celestia_adapter::CelestiaService::new(celestia_config, params, shutdown_controller)
            .await;
    let celestia_service = Arc::new(celestia_service);

    let start = Instant::now();
    let finish = match (args.until_height, args.run_for_seconds) {
        (Some(uh), None) => FinishCondition::UntilHeight(uh),
        (None, Some(s)) => FinishCondition::AfterInstant(start + Duration::from_secs(s)),
        (None, None) => FinishCondition::Forever,
        (Some(_), Some(_)) => unreachable!("clap conflicts_with enforces mutual exclusion"),
    };
    tracing::info!(from_height = args.from_height, ?finish, "Sync parameters");

    let (result_tx, result_rx) = mpsc::unbounded_channel();

    let verifier = CelestiaVerifier::new(params);
    let reading_handle = tokio::spawn(run_reading_loop(
        celestia_service,
        ReadingLoopConfig {
            start_height: Some(args.from_height),
            finish,
        },
        shutdown_controller.clone(),
        result_tx,
        verifier,
    ));
    let stats_handle = tokio::spawn(run_stats_collector(result_rx, STATS_INTERVAL));

    reading_handle.await.unwrap();
    let stats = stats_handle.await.unwrap();

    print_sync_report(&stats, start.elapsed());
}

fn print_read_stats(stats: &Stats) {
    tracing::info!(
        "Blocks read: {} success, {} errors",
        stats.blocks_read_success,
        stats.block_read_error
    );
    tracing::info!("Blobs read: {}", stats.blobs_read);
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
    print_read_stats(stats);
}

fn print_sync_report(stats: &Stats, elapsed: Duration) {
    tracing::info!("=== Final Stats (sync-and-read) ===");
    tracing::info!("Running time: {:.2?}", elapsed);
    print_read_stats(stats);
}

#[cfg(test)]
mod tests {
    use super::{Cli, Commands, CompressionArg, mnemonic_to_hex};
    use clap::Parser;

    const TEST_MNEMONIC_12: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

    #[test]
    fn mnemonic_to_hex_produces_32_byte_hex() {
        let hex = mnemonic_to_hex(TEST_MNEMONIC_12).unwrap();
        assert_eq!(hex.len(), 64, "expected 64-char hex (32 bytes)");
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // Stability snapshot for the canonical BIP39 `abandon ... about` phrase at
    // Celestia's default path m/44'/118'/0'/0/0. If this ever changes, HD derivation
    // drifted — cross-check against `cel-key export --unarmored-hex --unsafe` before
    // updating.
    #[test]
    fn mnemonic_to_hex_stability_snapshot() {
        let hex = mnemonic_to_hex(TEST_MNEMONIC_12).unwrap();
        assert_eq!(
            hex,
            "c4a48e2fce1481cd3294b4490f6678090ea98d3d0e5cd984558ab0968741b104"
        );
    }

    // BIP39 spec test vector (Trezor python-mnemonic): the seed for the `abandon...about`
    // 12-word phrase with the canonical "TREZOR" passphrase. Verifies our bip39 crate
    // produces a spec-compliant seed, which in turn guarantees the downstream HD
    // derivation matches cel-key / cosmjs / Keplr for any path and any passphrase.
    #[test]
    fn mnemonic_seed_matches_bip39_spec_vector() {
        let mnemonic = bip39::Mnemonic::parse(TEST_MNEMONIC_12).unwrap();
        let seed = mnemonic.to_seed("TREZOR");
        assert_eq!(
            hex::encode(seed),
            "c55257c360c07c72029aebc1b53c05ed0362ada38ead3e3e9efa3708e53495531f09a6987599d18264c1e1c92f2cf141630c7a3c4ab7c81b2f001698e7463b04"
        );
    }

    #[test]
    fn mnemonic_to_hex_is_deterministic() {
        let a = mnemonic_to_hex(TEST_MNEMONIC_12).unwrap();
        let b = mnemonic_to_hex(TEST_MNEMONIC_12).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn mnemonic_to_hex_normalizes_whitespace() {
        let padded = format!("  {}\n  ", TEST_MNEMONIC_12.replace(' ', "   "));
        let a = mnemonic_to_hex(TEST_MNEMONIC_12).unwrap();
        let b = mnemonic_to_hex(&padded).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn mnemonic_to_hex_changes_with_phrase() {
        let other = "legal winner thank year wave sausage worth useful legal winner thank yellow";
        let a = mnemonic_to_hex(TEST_MNEMONIC_12).unwrap();
        let b = mnemonic_to_hex(other).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn mnemonic_to_hex_rejects_wrong_word_count() {
        let eleven_words = TEST_MNEMONIC_12.rsplit_once(' ').unwrap().0;
        assert!(mnemonic_to_hex(eleven_words).is_err());
    }

    #[test]
    fn mnemonic_to_hex_rejects_bad_checksum() {
        let bad = TEST_MNEMONIC_12.replace("about", "abandon");
        assert!(mnemonic_to_hex(&bad).is_err());
    }

    #[test]
    fn mnemonic_to_hex_rejects_unknown_word() {
        let bad = TEST_MNEMONIC_12.replace("about", "notaword");
        assert!(mnemonic_to_hex(&bad).is_err());
    }

    #[test]
    fn submit_and_read_accepts_rpc_token() {
        let cli = Cli::parse_from([
            "celestia-adapter-evaluator",
            "submit-and-read",
            "--namespace",
            "myrollup00",
            "--rpc-endpoint",
            "http://localhost:26657",
            "--rpc-token",
            "rpc-secret",
            "--grpc-endpoint",
            "http://localhost:9090",
            "--signer-private-key",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--run-for-seconds",
            "60",
        ]);

        let Commands::SubmitAndRead(args) = cli.command else {
            panic!("expected submit-and-read command");
        };
        assert_eq!(args.rpc_token.as_deref(), Some("rpc-secret"));
    }

    fn submit_and_read_args(extra: &[&str]) -> super::SubmitAndReadArgs {
        let base = [
            "celestia-adapter-evaluator",
            "submit-and-read",
            "--namespace",
            "myrollup00",
            "--rpc-endpoint",
            "http://localhost:26657",
            "--grpc-endpoint",
            "http://localhost:9090",
            "--signer-private-key",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--run-for-seconds",
            "60",
        ];
        let cli = Cli::parse_from(base.iter().chain(extra).copied());
        let Commands::SubmitAndRead(args) = cli.command else {
            panic!("expected submit-and-read command");
        };
        args
    }

    #[test]
    fn compression_defaults_to_none() {
        // Omitting the flag leaves the config default untouched.
        assert!(submit_and_read_args(&[]).compression.is_none());
    }

    #[test]
    fn compression_parses_lz4() {
        let args = submit_and_read_args(&["--compression", "lz4"]);
        assert!(matches!(args.compression, Some(CompressionArg::Lz4)));
    }

    #[test]
    fn compression_parses_off() {
        let args = submit_and_read_args(&["--compression", "off"]);
        assert!(matches!(args.compression, Some(CompressionArg::Off)));
    }

    #[test]
    fn compression_rejects_unknown_value() {
        let result = Cli::try_parse_from([
            "celestia-adapter-evaluator",
            "submit-and-read",
            "--namespace",
            "myrollup00",
            "--rpc-endpoint",
            "http://localhost:26657",
            "--grpc-endpoint",
            "http://localhost:9090",
            "--signer-private-key",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--run-for-seconds",
            "60",
            "--compression",
            "gzip",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn compression_chunk_size_defaults_to_none() {
        // Omitting the flag leaves the config default untouched.
        assert!(
            submit_and_read_args(&[])
                .compression_chunk_size
                .is_none()
        );
    }

    #[test]
    fn compression_chunk_size_parses_value() {
        let args = submit_and_read_args(&["--compression-chunk-size", "1024"]);
        assert_eq!(args.compression_chunk_size, Some(1024));
    }

    #[test]
    fn compression_chunk_size_rejects_non_numeric() {
        let result = Cli::try_parse_from([
            "celestia-adapter-evaluator",
            "submit-and-read",
            "--namespace",
            "myrollup00",
            "--rpc-endpoint",
            "http://localhost:26657",
            "--grpc-endpoint",
            "http://localhost:9090",
            "--signer-private-key",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--run-for-seconds",
            "60",
            "--compression-chunk-size",
            "abc",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn sync_and_read_accepts_rpc_token() {
        let cli = Cli::parse_from([
            "celestia-adapter-evaluator",
            "sync-and-read",
            "--namespace",
            "myrollup00",
            "--rpc-endpoint",
            "http://localhost:26657",
            "--rpc-token",
            "rpc-secret",
            "--from-height",
            "123",
        ]);

        let Commands::SyncAndRead(args) = cli.command else {
            panic!("expected sync-and-read command");
        };
        assert_eq!(args.rpc_token.as_deref(), Some("rpc-secret"));
    }
}
