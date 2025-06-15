# StreamWeld

A Rust implementation inspired by Elixir's GenStage, providing demand-driven data processing pipelines with automatic backpressure control.

[![Crates.io](https://img.shields.io/crates/v/streamweld.svg)](https://crates.io/crates/streamweld)
[![Documentation](https://docs.rs/streamweld/badge.svg)](https://docs.rs/streamweld)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/scrogson/streamweld)

## Features

- **Demand-driven architecture**: Sinks control the flow by pulling data from sources
- **Automatic backpressure**: Built-in flow control prevents overwhelming slow sinks
- **Composable pipelines**: Chain sources, processors, and sinks with a fluent API
- **Async/await support**: Full integration with Rust's async ecosystem
- **Type safety**: Leverage Rust's type system for compile-time guarantees
- **Configurable buffering**: Control memory usage and latency with buffer size settings
- **Error handling**: Structured error propagation with optional fail-fast behavior
- **Concurrent processing**: Support for parallel execution with configurable concurrency
- **Rich combinators**: Map, filter, take, chain, and other functional operations

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
streamweld = "0.1"
tokio = { version = "1.0", features = ["full"] }
```

## Basic Usage

```rust
use streamweld::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a source that generates numbers 1-10
    let source = RangeSource::new(1..11);

    // Create a processor that doubles each number
    let processor = MapProcessor::new(|x| x * 2);

    // Create a sink that prints each item
    let sink = PrintSink::with_prefix("Result".to_string());

    // Build and run the pipeline
    Pipeline::new(source, processor)
        .buffer_size(5)
        .sink(sink)
        .await?;

    Ok(())
}
```

## Core Concepts

### Source

A `Source` generates items on demand. Sources are pull-based, meaning they only generate items when explicitly requested by downstream sinks.

```rust
use streamweld::traits::Source;

struct CounterSource {
    current: u64,
    max: u64,
}

#[async_trait::async_trait]
impl Source for CounterSource {
    type Item = u64;

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        if self.current <= self.max {
            let item = self.current;
            self.current += 1;
            Ok(Some(item))
        } else {
            Ok(None) // Signal completion
        }
    }
}
```

### Sink

A `Sink` processes items from upstream sources.

```rust
use streamweld::traits::Sink;

struct LogSink;

#[async_trait::async_trait]
impl Sink for LogSink {
    type Item = String;

    async fn write(&mut self, item: Self::Item) -> Result<()> {
        println!("Consumed: {}", item);
        Ok(())
    }
}
```

### Processor

A `Processor` transforms items flowing through the pipeline.

```rust
use streamweld::traits::Processor;

struct DoubleProcessor;

#[async_trait::async_trait]
impl Processor for DoubleProcessor {
    type Input = i32;
    type Output = i32;

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        Ok(vec![item * 2])
    }
}
```

## Advanced Examples

### Complex Pipeline with Multiple Stages

```rust
use streamweld::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let source = RangeSource::new(1..101);

    let pipeline = Pipeline::new(source, FilterProcessor::new(|x: &i64| x % 2 == 0)) // Even numbers only
        .pipe(MapProcessor::new(|x| x * 3))                // Multiply by 3
        .pipe(BatchProcessor::new(5))                      // Group into batches of 5
        .pipe(DelayProcessor::new(Duration::from_millis(100))) // Add delay
        .buffer_size(20)
        .timeout(Duration::from_secs(30))
        .concurrency(4);

    pipeline.sink(PrintSink::with_prefix("Batch".to_string())).await?;
    Ok(())
}
```

### Error Handling

```rust
use streamweld::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let source = RangeSource::new(1..101);

    let error_handler = ErrorHandlingProcessor::new(
        MapProcessor::new(|x: i32| {
            if x % 10 == 0 {
                panic!("Simulated error!");
            }
            format!("Value: {}", x)
        })
    );

    let sink = PrintSink::with_prefix("Result".to_string());

    Pipeline::new(source, error_handler)
        .fail_fast(false) // Continue processing on errors
        .sink(sink)
        .await?;

    Ok(())
}
```

### Concurrent Processing

```rust
use streamweld::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let source = RangeSource::new(1..1001);
    let sink = CollectSink::new();
    let sink_ref = sink.clone();

    let pipeline = Pipeline::new(source, NoOpProcessor::new())
        .buffer_size(100)
        .concurrency(8)
        .timeout(Duration::from_secs(1))
        .fail_fast(true);

    pipeline.sink(sink).await?;

    let items = sink_ref.items();
    println!("Processed {} items", items.lock().await.len());

    Ok(())
}
```

## Built-in Implementations

### Sources

- `RangeSource` - Generates numbers from a range
- `VecSource` - Yields items from a vector
- `RepeatSource` - Repeats a value N times or infinitely
- `IntervalSource` - Generates items at timed intervals
- `ChunkSource` - Groups items into chunks
- `MergeSource` - Merges multiple sources round-robin
- `FibonacciSource` - Generates Fibonacci sequence

### Sinks

- `PrintSink` - Prints items to stdout
- `CollectSink` - Collects items into a vector
- `CountSink` - Counts processed items
- `FileSink` - Writes items to a file
- `BatchSink` - Batches items before processing
- `ThroughputSink` - Measures processing throughput
- `RateLimitedSink` - Applies rate limiting

### Processors

- `MapProcessor` - Transforms items with a function
- `FilterProcessor` - Filters items with a predicate
- `FlatMapProcessor` - Flat maps items to multiple outputs
- `BatchProcessor` - Groups items into batches
- `DebatchProcessor` - Ungroups batched items
- `DelayProcessor` - Adds delays between items
- `BufferProcessor` - Buffers items by time/count
- `RateLimitProcessor` - Applies rate limiting
- `TakeProcessor` - Takes only first N items
- `SkipProcessor` - Skips first N items

## Performance

Streamweld is designed for high performance with minimal allocations:

- Zero-cost abstractions using Rust's trait system
- Configurable buffering to balance memory usage and latency
- Support for concurrent processing with work-stealing
- Efficient backpressure through bounded channels
- Optional metrics collection for monitoring

Benchmarks show throughput of over 1M items/second on modern hardware for simple transformations.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

- Inspired by [Elixir's GenStage](https://hexdocs.pm/gen_stage/GenStage.html)
- Built on the excellent [Tokio](https://tokio.rs/) async runtime
- Thanks to the Rust community for feedback and contributions
