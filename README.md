# StreamWeld

[![CI](https://github.com/scrogson/streamweld/workflows/CI/badge.svg)](https://github.com/scrogson/streamweld/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/streamweld.svg)](https://crates.io/crates/streamweld)
[![Documentation](https://docs.rs/streamweld/badge.svg)](https://docs.rs/streamweld)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/scrogson/streamweld/branch/main/graph/badge.svg)](https://codecov.io/gh/scrogson/streamweld)

A GenStage-inspired streaming library for Rust with demand-driven backpressure control and batch-first processing.

## Features

- **GenStage-inspired Architecture**: Explicit demand signaling for natural backpressure control
- **Batch-First Processing**: Efficient batch processing with configurable demand sizes
- **Smart Combinators**: Intelligent demand management in filter, map, and take operations
- **Async/Await Native**: Built from the ground up for Rust's async ecosystem
- **Zero-Copy Where Possible**: Minimal allocations and efficient memory usage
- **Configurable Pipelines**: Tunable batch sizes, concurrency, and timeouts
- **Comprehensive Error Handling**: Graceful error propagation and recovery
- **Rich Ecosystem**: Sources, sinks, processors, and combinators for common use cases

## Quick Start

Add StreamWeld to your `Cargo.toml`:

```toml
[dependencies]
streamweld = "0.1"
```

### Basic Usage

```rust
use streamweld::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a source that produces numbers 1-10
    let source = RangeSource::new(1..11);

    // Process them (double each number)
    let processor = MapProcessor::new(|x| x * 2);

    // Print the results
    let sink = PrintSink::with_prefix("Result".to_string());

    // Build and run the pipeline
    Pipeline::new(source, processor)
        .sink(sink)
        .await?;

    Ok(())
}
```

### GenStage-Style Demand Processing

```rust
use streamweld::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let mut source = RangeSource::new(1..21);

    // Request specific batch sizes
    println!("Requesting 5 items:");
    let batch1 = source.handle_demand(5).await?;
    println!("Got: {:?}", batch1);

    println!("Requesting 3 more items:");
    let batch2 = source.handle_demand(3).await?;
    println!("Got: {:?}", batch2);

    Ok(())
}
```

### Advanced Pipeline Configuration

```rust
use streamweld::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let source = RangeSource::new(1..1001)
        .filter(|x| x % 2 == 0)  // Even numbers only
        .map(|x| x * x)          // Square them
        .take(50);               // Take first 50

    let processor = BatchProcessor::new(10); // Group into batches of 10
    let sink = CollectSink::new();
    let sink_ref = sink.clone();

    Pipeline::new(source, processor)
        .demand_batch_size(100)                    // Process 100 items at a time
        .buffer_size(20)                           // Buffer up to 20 batches
        .max_concurrency(4)                        // Use 4 concurrent workers
        .operation_timeout(Duration::from_secs(30)) // 30 second timeout
        .sink(sink)
        .await?;

    let results = sink_ref.into_items().await;
    println!("Processed {} batches", results.len());

    Ok(())
}
```

## Architecture

StreamWeld is built around three core traits:

- **Source**: Produces items with explicit demand signaling via `handle_demand(usize)`
- **Processor**: Transforms items with batch-first processing via `process_batch(Vec<Input>)`
- **Sink**: Consumes items with batch-first writing via `write_batch(Vec<Item>)`

### Demand-Driven Processing

Unlike traditional streaming libraries that use implicit backpressure, StreamWeld uses explicit demand signaling inspired by Elixir's GenStage:

```rust
// Sources respond to explicit demand requests
let items = source.handle_demand(batch_size).await?;

// Processors handle batches efficiently
let results = processor.process_batch(input_batch).await?;

// Sinks write batches with minimal overhead
sink.write_batch(output_batch).await?;
```

This approach provides:

- **Natural Backpressure**: Consumers control the flow rate
- **Efficient Batching**: Reduced async overhead through batch processing
- **Smart Resource Management**: Memory usage scales with demand, not data size
- **Predictable Performance**: Configurable batch sizes for different workloads

## Built-in Components

### Sources

- `RangeSource` - Generate sequences of numbers
- `VecSource` - Iterate over collections
- `FibonacciSource` - Generate Fibonacci sequences
- `RepeatSource` - Repeat values
- `MergeSource` - Combine multiple sources

### Processors

- `MapProcessor` - Transform items
- `FilterProcessor` - Filter items by predicate
- `BatchProcessor` - Group items into batches
- `RateLimitProcessor` - Control processing rate
- `ErrorHandlingProcessor` - Handle and recover from errors

### Sinks

- `PrintSink` - Print items to stdout
- `CollectSink` - Collect items into a vector
- `CountSink` - Count processed items
- `ThroughputSink` - Measure processing throughput

### Combinators

Sources support chainable combinators:

```rust
let source = RangeSource::new(1..1000)
    .filter(|x| x % 2 == 0)    // Keep even numbers
    .map(|x| x * 3)            // Multiply by 3
    .take(100);                // Take first 100
```

## Performance

StreamWeld is designed for high-throughput scenarios:

- **Batch Processing**: Processes items in configurable batches (default: 100)
- **Smart Demand Management**: Combinators request extra items when filtering
- **Minimal Allocations**: Zero-copy processing where possible
- **Async Efficiency**: Reduced async overhead through batching

Run benchmarks:

```bash
cargo bench
```

## Examples

The repository includes comprehensive examples:

```bash
# Basic usage patterns
cargo run --example basic

# Complex pipeline configurations
cargo run --example complex

# All examples with detailed explanations
cargo run --example examples
```

## Testing

Run the full test suite:

```bash
# Unit and integration tests
cargo test

# With all features enabled
cargo test --all-features

# Include doc tests
cargo test --doc
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/scrogson/streamweld.git
   cd streamweld
   ```

2. Install Rust (if not already installed):

   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

3. Run tests:

   ```bash
   cargo test --all-features
   ```

4. Run examples:
   ```bash
   cargo run --example basic
   ```

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

- Inspired by Elixir's [GenStage](https://hexdocs.pm/gen_stage/GenStage.html) library
- Built on Rust's excellent async ecosystem
- Thanks to the Rust community for feedback and contributions
