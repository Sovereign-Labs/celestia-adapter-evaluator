# Celestia Adapter Evaluator

A tool for evaluating the performance of the Sovereign SDK's Celestia adapter by submitting random blobs to a Celestia node and measuring throughput and success rates.

## Prerequisites

- Rust (edition 2024)
- Access to a Celestia node (RPC and gRPC endpoints)
- A funded Celestia account (private key in hex format)

## Building

```bash
cargo build --release
```

## Usage

Simple case with local bridge and consensus nodes:

```bash
cargo run --release -- \
  --namespace "myrollup00" \
  --rpc-endpoint "http://localhost:26657" \
  --grpc-endpoint "http://localhost:9090" \
  --signer-private-key "<hex-encoded-private-key>" \
  --run-for-seconds 60
```
**QuickNode**

Here is an example of how to run this evaluator with [QuickNode](https://www.quicknode.com/guides/infrastructure/node-setup/run-a-celestia-light-node), where:

* `foo-bar-baz.celestia-mocha.quiknode.pro` is your endpoint (host only) 
* `TOKEN` is authentication token to access endpoint

```bash
cargo run --release -- \
  --namespace "myrollup00" \
  --rpc-endpoint="https://foo-bar-baz.celestia-mocha.quiknode.pro/TOKEN" \
  --grpc-endpoint="https://foo-bar-baz.celestia-mocha.quiknode.pro:9090" \
  --grpc-token="TOKEN" \
  --signer-private-key "<hex-encoded-private-key>" \
  --run-for-seconds 60
```

### Required Arguments

| Argument               | Description                                      |
|------------------------|--------------------------------------------------|
| `--namespace`          | 10-byte ASCII namespace for the rollup           |
| `--rpc-endpoint`       | Celestia node RPC endpoint URL                   |
| `--grpc-endpoint`      | Celestia node gRPC endpoint URL                  |
| `--signer-private-key` | Hex-encoded private key for signing transactions |
| `--run-for-seconds`    | Duration to run the evaluation                   |

### Optional Arguments

| Argument          | Default | Description                            |
|-------------------|---------|----------------------------------------|
| `--grpc-token`    | None    | Authentication token for gRPC endpoint |
| `--blob-size-min` | 6 MiB   | Minimum blob size in bytes             |
| `--blob-size-max` | 6 MiB   | Maximum blob size in bytes             |

## Output

The evaluator logs progress during execution and prints final statistics including:

- Total running time
- Number of successful/failed submissions
- Success/failure percentages
- Throughput in KiB/s

## Real-time metrics

It is possible to user https://github.com/Sovereign-Labs/sov-observability to collect and visualize metrics from this tool.

Clone https://github.com/Sovereign-Labs/sov-observability and run it with `make start` 
Data from the tool will be visible in "Sovereign Celestia Adapter" dashboard

## License

Sovereign Permissionless Commercial License
