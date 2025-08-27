# pulsar-rs-perf

The performance tool on [pulsar-rs](https://github.com/streamnative/pulsar-rs).

## How to build

Please ensure your system has `protoc` installed. For example, on macOS, you can run:

```bash
brew install protobuf
```

Then you can build the project with:

```bash
cargo build --release
```

Run the performance tool:

```bash
RUST_LOG=info cargo run
```
