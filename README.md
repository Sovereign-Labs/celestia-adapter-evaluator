# Celestia Adapter Evaluator

A tool for exercising the Sovereign SDK's Celestia adapter. It has two modes:

- **`submit-and-read`** — the original evaluator: submits random blobs to a Celestia node while also reading them back, and reports throughput / success rates. Requires a funded Celestia account.
- **`sync-and-read`** — read-only: pulls blobs for a given namespace starting at a given height and reports read stats. Needs only an RPC endpoint — no signer, no gRPC.

## Prerequisites

- Rust (edition 2024)
- Access to a Celestia node (RPC endpoint; gRPC only needed for `submit-and-read`)
- For `submit-and-read`: a funded Celestia account (private key in hex format)

## Building

```bash
cargo build --release
```

## Docker

Alternatively, build and run the evaluator in a container. The binary is the image entrypoint, so the same subcommands / arguments apply:

```bash
docker build -t celestia-adapter-evaluator .

docker run --rm celestia-adapter-evaluator submit-and-read \
  --namespace "myrollup00" \
  --rpc-endpoint "https://<host>" \
  --grpc-endpoint "https://<host>:9090" \
  --grpc-token "<token>" \
  --signer-private-key "<hex-encoded-private-key>" \
  --run-for-seconds 60
```

Prebuilt multi-arch images (amd64, arm64) are published to `ghcr.io/sovereign-labs/celestia-adapter-evaluator`, tagged with the short revision of the pinned `sov-celestia-adapter`.

## Usage

### `submit-and-read`

Submits random blobs on an interval and reads blocks as they appear.

Local bridge + consensus nodes:

```bash
cargo run --release -- submit-and-read \
  --namespace "myrollup00" \
  --rpc-endpoint "http://localhost:26657" \
  --grpc-endpoint "http://localhost:9090" \
  --signer-private-key "<hex-encoded-private-key>" \
  --run-for-seconds 60
```

[QuickNode](https://www.quicknode.com/guides/infrastructure/node-setup/run-a-celestia-light-node) example, where:

* `foo-bar-baz.celestia-mocha.quiknode.pro` is your endpoint (host only)
* `TOKEN` is the authentication token

```bash
cargo run --release -- submit-and-read \
  --namespace "myrollup00" \
  --rpc-endpoint="https://foo-bar-baz.celestia-mocha.quiknode.pro/TOKEN" \
  --grpc-endpoint="https://foo-bar-baz.celestia-mocha.quiknode.pro:9090" \
  --grpc-token="TOKEN" \
  --signer-private-key "<hex-encoded-private-key>" \
  --run-for-seconds 60
```

#### `submit-and-read` arguments

| Argument               | Required | Description                                         |
|------------------------|----------|-----------------------------------------------------|
| `--namespace`          | yes      | 10-byte ASCII namespace for the rollup              |
| `--rpc-endpoint`       | yes      | Celestia node RPC endpoint URL                      |
| `--grpc-endpoint`      | yes      | Celestia node gRPC endpoint URL                     |
| `--signer-private-key` | yes      | Hex-encoded private key for signing submissions     |
| `--run-for-seconds`    | yes      | Duration to run the evaluation                      |
| `--grpc-token`         | no       | Authentication token for the gRPC endpoint          |
| `--blob-size-min`      | no       | Minimum blob size in bytes (default 6 MiB)          |
| `--blob-size-max`      | no       | Maximum blob size in bytes (default 6 MiB)          |

### `sync-and-read`

Read-only: fetches and verifies blocks in the given namespace starting at `--from-height`. Useful for monitoring a deployed rollup's DA layer without needing signer credentials.

Historical range — read 1000 blocks and exit:

```bash
cargo run --release -- sync-and-read \
  --namespace "myrollup00" \
  --rpc-endpoint "http://localhost:26657" \
  --from-height 1234567 \
  --until-height 1235567
```

Live tail — read from a height and keep going until stopped:

```bash
cargo run --release -- sync-and-read \
  --namespace "myrollup00" \
  --rpc-endpoint="https://foo-bar-baz.celestia-mocha.quiknode.pro/TOKEN" \
  --from-height 1234567
# Ctrl+C / SIGTERM to stop; final stats are printed on exit.
```

Fixed-duration run:

```bash
cargo run --release -- sync-and-read \
  --namespace "myrollup00" \
  --rpc-endpoint "http://localhost:26657" \
  --from-height 1234567 \
  --run-for-seconds 3600
```

Stop conditions (pick at most one of the flags — they are mutually exclusive):

- `--until-height <H>` — stop after reading height `H`.
- `--run-for-seconds <N>` — stop after `N` seconds of wall clock.
- neither — run until Ctrl+C / SIGTERM.

When caught up to the chain head the loop waits for new blocks via the sov-celestia-adapter's built-in backoff; Ctrl+C interrupts an in-flight fetch cleanly.

#### `sync-and-read` arguments

| Argument              | Required | Description                                                                    |
|-----------------------|----------|--------------------------------------------------------------------------------|
| `--namespace`         | yes      | 10-byte ASCII namespace for the rollup                                         |
| `--rpc-endpoint`      | yes      | Celestia node RPC endpoint URL                                                 |
| `--from-height`       | yes      | First block height to read                                                     |
| `--until-height`      | no       | Stop after reading this height (inclusive). Mutually exclusive with `--run-for-seconds` |
| `--run-for-seconds`   | no       | Wall-clock deadline in seconds. Mutually exclusive with `--until-height`       |

## Output

The evaluator logs progress during execution and prints final statistics on exit.

`submit-and-read` reports:
- Total running time
- Successful / failed submission counts + percentages
- Throughput in KiB/s
- Blocks read (success / error) and blobs read

`sync-and-read` reports:
- Total running time
- Blocks read (success / error) and blobs read

## Real-time metrics

It is possible to use https://github.com/Sovereign-Labs/sov-observability to collect and visualize metrics from this tool.

Clone https://github.com/Sovereign-Labs/sov-observability and run it with `make start`.
Data from the tool will be visible in the "Sovereign Celestia Adapter" dashboard.

## License

Sovereign Permissionless Commercial License
