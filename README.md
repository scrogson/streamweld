# Streamweld

A Rust implementation inspired by Elixir's GenStage, providing demand-driven data processing pipelines with automatic backpressure control.

[![Crates.io](https://img.shields.io/crates/v/streamweld.svg)](https://crates.io/crates/streamweld)
[![Documentation](https://docs.rs/streamweld/badge.svg)](https://docs.rs/streamweld)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/scrogson/streamweld)

## Features

- **Demand-driven architecture**: Consumers control the flow by pulling data from producers
- **Automatic backpressure**: Built-in flow control prevents overwhelming slow consumers
- **Composable pipelines**: Chain producers, processors, and consumers with a fluent API
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
    // Create a producer that generates numbers 1-10
    let producer = RangeProducer::new(1..11);

    // Create a processor that doubles each number
    let processor = MapProcessor::new(|x| x * 2);

    // Create a consumer that prints each item
    let consumer = PrintConsumer::with_prefix("Result");

    // Build and run the pipeline
    Pipeline::new(producer)
        .pipe(processor)
        .with_buffer_size(5)
        .sink(consumer)
        .await?;

    Ok(())
}
```

## Core Concepts

### Producer

A `Producer` generates items on demand. Producers are pull-based, meaning they only generate items when explicitly requested by downstream consumers.

```rust
use streamweld::traits::Producer;

struct CounterProducer {
    current: u64,
    max: u64,
}

impl Producer for CounterProducer {
    type Item = u64;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
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

### Consumer

A `Consumer` processes items from upstream producers.

```rust
use streamweld::traits::Consumer;

struct LogConsumer;

impl Consumer for LogConsumer {
    type Item = String;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        println!("Consumed: {}", item);
        Ok(())
    }
}
```

### Processor

A `Processor` is both a consumer and producer - it transforms items flowing through the pipeline.

```rust
use streamweld::traits::Processor;

struct DoubleProcessor;

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
    let producer = RangeProducer::new(1..101);

    let pipeline = Pipeline::new(producer)
        .pipe(FilterProcessor::new(|x: &i64| x % 2 == 0)) // Even numbers only
        .pipe(MapProcessor::new(|x| x * 3))                // Multiply by 3
        .pipe(BatchProcessor::new(5))                      // Group into batches of 5
        .pipe(DelayProcessor::new(Duration::from_millis(100))) // Add delay
        .with_buffer_size(20)
        .with_timeout(Duration::from_secs(30))
        .with_concurrency(4);

    pipeline.sink(PrintConsumer::with_prefix("Batch")).await?;
    Ok(())
}
```

### Error Handling

```rust
use streamweld::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = /* your producer */;

    let error_handler = ErrorHandlingProcessor::new(
        MapProcessor::new(|x: i32| {
            if x % 10 == 0 {
                panic!("Simulated error!");
            }
            format!("Value: {}", x)
        })
    );

    let consumer = consumer_from_fn(|result: Result<String>| async move {
        match result {
            Ok(value) => println!("✓ {}", value),
            Err(e) => println!("✗ Error: {}", e),
        }
        Ok(())
    });

    Pipeline::new(producer)
        .pipe(error_handler)
        .with_fail_fast(false) // Continue processing on errors
        .sink(consumer)
        .await?;

    Ok(())
}
```

### Concurrent Processing

```rust
use streamweld::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = RangeProducer::new(1..1001);
    let consumer = CollectConsumer::new();
    let collector_ref = consumer.clone();

    let pipeline = ConcurrentPipeline::new(producer, consumer)
        .with_config(PipelineConfig {
            buffer_size: 100,
            max_concurrency: 8,
            operation_timeout: Some(Duration::from_secs(1)),
            fail_fast: true,
        });

    pipeline.run().await?;

    let items = collector_ref.items();
    println!("Processed {} items", items.lock().unwrap().len());

    Ok(())
}
```

## Built-in Implementations

### Producers

- `RangeProducer` - Generates numbers from a range
- `VecProducer` - Yields items from a vector
- `RepeatProducer` - Repeats a value N times or infinitely
- `IntervalProducer` - Generates items at timed intervals
- `ChunkProducer` - Groups items into chunks
- `MergeProducer` - Merges multiple producers round-robin
- `FibonacciProducer` - Generates Fibonacci sequence

### Consumers

- `PrintConsumer` - Prints items to stdout
- `CollectConsumer` - Collects items into a vector
- `CountConsumer` - Counts processed items
- `FileConsumer` - Writes items to a file
- `BatchConsumer` - Batches items before processing
- `ThroughputConsumer` - Measures processing throughput
- `RateLimitedConsumer` - Applies rate limiting

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
