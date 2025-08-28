# pulsar-rs-perf

The performance tool on [pulsar-rs](https://github.com/streamnative/pulsar-rs).

## How to build

Please ensure your system has `protoc` installed. For example, on macOS, you can run:

```bash
brew install protobuf
```

Then you can build the project with:

```bash
cargo build
```

Run the performance tool:

## Produce messages

```bash
RUST_LOG=info ./target/debug/pulsar-rs-perf my-topic produce
```

The command above will produce messages to `my-topic` with default settings, which is equivalent to:

```bash
RUST_LOG=info ./target/release/pulsar-rs-perf my-topic \
    --service-url "pulsar://localhost:6650" \
    produce --rate 500 -n 10000
```

Options for the main command:
- `--service-url`: The Pulsar service URL (default: `pulsar://localhost:6650`)
- `--auth-token`: The authentication token (default: `None`, which means no authentication)

Options for `produce` sub-command:
- `--rate`: The message producing rate (default: `500` messages per second)
- `-n` or `--num-messages`: The total number of messages to send (default: `10000`)
