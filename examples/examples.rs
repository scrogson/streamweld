//! Usage examples for the GenStage-inspired source/sink system.

use std::time::Duration;
use streamweld::prelude::*;
use streamweld::sources::MergeSource;
use streamweld::utils::into_fn;
use streamweld::utils::{from_fn, processor_from_fn};
use streamweld::Result;
use tokio::time::sleep;

/// Example 1: Basic pipeline with range source and print sink
pub async fn basic_pipeline_example() -> Result<()> {
    println!("=== Basic Pipeline Example ===");

    let source = RangeSource::new(1..11);
    let sink = PrintSink::<i64>::with_prefix("Item".to_string());

    let pipeline = Pipeline::new(source, NoOpProcessor::<i64>::new()).buffer_size(5);

    pipeline.sink(sink).await?;

    println!("Basic pipeline completed!\n");
    Ok(())
}

/// Example 2: Pipeline with processing
pub async fn processing_pipeline_example() -> Result<()> {
    println!("=== Processing Pipeline Example ===");

    let source = RangeSource::new(1..11);
    let processor = MapProcessor::new(|x| x * 2);
    let sink = PrintSink::<i64>::with_prefix("Doubled".to_string());

    let pipeline = Pipeline::new(source, processor).buffer_size(5);

    pipeline.sink(sink).await?;

    println!("Processing pipeline completed!\n");
    Ok(())
}

/// Multi-stage processor: filter even, double, batch
struct MultiStageProcessor {
    batch: Vec<i64>,
    batch_size: usize,
}

impl MultiStageProcessor {
    fn new(batch_size: usize) -> Self {
        Self {
            batch: Vec::with_capacity(batch_size),
            batch_size,
        }
    }
}

#[async_trait::async_trait]
impl streamweld::core::Processor for MultiStageProcessor {
    type Input = i64;
    type Output = Vec<i64>;

    async fn process_batch(&mut self, items: Vec<Self::Input>) -> Result<Vec<Self::Output>> {
        let mut results = Vec::new();

        for item in items {
            // Filter even numbers
            if item % 2 != 0 {
                continue;
            }
            // Double the value
            let doubled = item * 2;
            self.batch.push(doubled);

            if self.batch.len() >= self.batch_size {
                let batch = std::mem::take(&mut self.batch);
                results.push(batch);
            }
        }

        Ok(results)
    }

    async fn process(&mut self, item: Self::Input) -> Result<Vec<Self::Output>> {
        // Filter even numbers
        if item % 2 != 0 {
            return Ok(vec![]);
        }
        // Double the value
        let doubled = item * 2;
        self.batch.push(doubled);
        if self.batch.len() >= self.batch_size {
            let batch = std::mem::take(&mut self.batch);
            Ok(vec![batch])
        } else {
            Ok(vec![])
        }
    }

    async fn finish(&mut self) -> Result<Vec<Self::Output>> {
        if !self.batch.is_empty() {
            let batch = std::mem::take(&mut self.batch);
            Ok(vec![batch])
        } else {
            Ok(vec![])
        }
    }
}

/// Debug sink for Vec<i64>
struct DebugPrintSink;

#[async_trait::async_trait]
impl streamweld::core::Sink for DebugPrintSink {
    type Item = Vec<i64>;

    async fn write_batch(&mut self, items: Vec<Self::Item>) -> Result<()> {
        for batch in items {
            println!("Batch: {:?}", batch);
        }
        Ok(())
    }

    async fn write(&mut self, item: Self::Item) -> Result<()> {
        println!("Batch: {:?}", item);
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Example 3: Complex multi-stage pipeline
pub async fn complex_pipeline_example() -> Result<()> {
    println!("=== Complex Pipeline Example ===");

    let _source = RangeSource::new(1..21);
    let processor = MultiStageProcessor::new(3);
    let sink = DebugPrintSink;

    let pipeline = Pipeline::new(_source, processor).buffer_size(10);

    pipeline.sink(sink).await?;

    println!("Complex pipeline completed!\n");
    Ok(())
}

/// Example 4: Fibonacci generator with rate limiting
pub async fn fibonacci_rate_limited_example() -> Result<()> {
    println!("=== Fibonacci Rate Limited Example ===");

    let source = FibonacciSource::with_limit(10);
    let rate_limiter = RateLimitProcessor::new(2); // 2 per second
    let sink = PrintSink::<u64>::with_prefix("Fib".to_string());

    let pipeline = Pipeline::new(source, rate_limiter).operation_timeout(Duration::from_secs(1));

    let start = std::time::Instant::now();
    pipeline.sink(sink).await?;
    let elapsed = start.elapsed();

    println!(
        "Fibonacci pipeline completed in {:.2}s\n",
        elapsed.as_secs_f64()
    );
    Ok(())
}

/// Example 5: Concurrent processing with collection
pub async fn concurrent_collection_example() -> Result<()> {
    println!("=== Concurrent Collection Example ===");

    let source = RangeSource::new(1..101);
    let collector: CollectSink<i64> = CollectSink::new();
    let collector_ref = collector.clone();

    // Use builder pattern for config
    let pipeline = ConcurrentPipeline::new(source, collector)
        .buffer_size(20)
        .max_concurrency(4);

    pipeline.run().await?;

    let items = collector_ref.items();
    let collected = items.lock().await;
    println!("Collected {} items", collected.len());
    println!(
        "First 10 items: {:?}",
        &collected[..10.min(collected.len())]
    );

    println!("Concurrent collection completed!\n");
    Ok(())
}

/// Example 6: Error handling and recovery
pub async fn error_handling_example() -> Result<()> {
    println!("=== Error Handling Example ===");

    // Create a source that sometimes fails
    let source = from_fn(|| async {
        use std::sync::atomic::{AtomicI32, Ordering};
        static COUNTER: AtomicI32 = AtomicI32::new(0);

        let count = COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        if count % 3 == 0 {
            // Simulate occasional errors
            Err(Error::custom("Simulated error"))
        } else if count > 10 {
            Ok(None) // End production
        } else {
            Ok(Some(count))
        }
    });

    // Wrap in error handling
    let error_handler =
        ErrorHandlingProcessor::new(MapProcessor::new(|x: i32| format!("Value: {}", x)));

    let sink = into_fn(|result: Result<String>| async move {
        match result {
            Ok(value) => println!("✓ {}", value),
            Err(e) => println!("✗ Error: {}", e),
        }
        Ok(())
    });

    let pipeline = Pipeline::new(source, error_handler).fail_fast(false); // Continue on errors

    pipeline.sink(sink).await?;

    println!("Error handling example completed!\n");
    Ok(())
}

/// Example 7: Fan-out pattern with multiple sinks
pub async fn fan_out_example() -> Result<()> {
    println!("=== Fan-out Example ===");

    // Create multiple sinks
    let sink3 = CountSink::new();
    let counter_ref = sink3.clone();

    // Simple fan-out using multiple pipelines (for demonstration)
    let source1 = RangeSource::new(1..6);
    let source2 = RangeSource::new(6..11);
    let source3 = RangeSource::new(11..16);

    let pipeline1 = Pipeline::new(source1, NoOpProcessor::<i64>::new());
    let pipeline2 = Pipeline::new(source2, NoOpProcessor::<i64>::new());
    let pipeline3 = Pipeline::new(source3, NoOpProcessor::<i64>::new());

    // Run pipelines concurrently
    let results = tokio::try_join!(
        pipeline1.sink(PrintSink::<i64>::with_prefix("Pipeline-1".to_string())),
        pipeline2.sink(PrintSink::<i64>::with_prefix("Pipeline-2".to_string())),
        pipeline3.sink(counter_ref),
    );

    results?;

    println!("Fan-out example completed!\n");
    Ok(())
}

/// Example 8: Custom processor with async operations
pub async fn async_processor_example() -> Result<()> {
    println!("=== Async Processor Example ===");

    // Custom processor that simulates async work
    let async_processor = processor_from_fn(|x: i64| async move {
        // Simulate some async work
        sleep(Duration::from_millis(10)).await;

        // Transform the item
        let result = format!("Processed-{}", x);
        Ok(vec![result])
    });

    let source = RangeSource::new(1..11);
    let sink = PrintSink::<String>::with_prefix("Async".to_string());

    let pipeline = Pipeline::new(source, async_processor).operation_timeout(Duration::from_secs(5));

    let start = std::time::Instant::now();
    pipeline.sink(sink).await?;
    let elapsed = start.elapsed();

    println!(
        "Async processor completed in {:.2}s\n",
        elapsed.as_secs_f64()
    );
    Ok(())
}

/// Example 9: Throughput measurement
pub async fn throughput_measurement_example() -> Result<()> {
    println!("=== Throughput Measurement Example ===");

    let source = RangeSource::new(1..1001);
    let sink = ThroughputSink::new(
        CountSink::new(),
        Duration::from_millis(500), // Report every 500ms
    );

    let pipeline = Pipeline::new(source, NoOpProcessor::<i64>::new()).buffer_size(100);

    pipeline.sink(sink).await?;

    println!("Throughput measurement completed!\n");
    Ok(())
}

/// Example 10: Buffering and batching
pub async fn buffering_batching_example() -> Result<()> {
    println!("=== Buffering and Batching Example ===");

    // Create a source with irregular timing
    let source = IntervalSource::new(RangeSource::new(1..21), Duration::from_millis(50));

    // Buffer items for up to 200ms or 5 items
    let buffer_processor = BufferProcessor::new(5, Duration::from_millis(200));

    // Print the batches
    let sink = DebugPrintSink;

    let pipeline = Pipeline::new(source, buffer_processor).buffer_size(10);

    let start = std::time::Instant::now();
    pipeline.sink(sink).await?;
    let elapsed = start.elapsed();

    println!("Buffering completed in {:.2}s\n", elapsed.as_secs_f64());
    Ok(())
}

/// Example 11: Chaining sources
pub async fn chaining_example() -> Result<()> {
    println!("=== Chaining Sources ===");

    let source1 = RangeSource::new(1..4);
    let source2 = RangeSource::new(10..13);
    let source3 = RangeSource::new(20..23);

    let merged = MergeSource::new()
        .add_source(source1)
        .add_source(source2)
        .add_source(source3);

    Pipeline::new(merged, NoOpProcessor::<i64>::new())
        .sink(PrintSink::<i64>::with_prefix("Merged".to_string()))
        .await?;

    println!();
    Ok(())
}

/// Run all examples
pub async fn run_all_examples() -> Result<()> {
    println!("Running GenStage-inspired pipeline examples...\n");

    basic_pipeline_example().await?;
    processing_pipeline_example().await?;
    complex_pipeline_example().await?;
    fibonacci_rate_limited_example().await?;
    concurrent_collection_example().await?;
    error_handling_example().await?;
    fan_out_example().await?;
    async_processor_example().await?;
    throughput_measurement_example().await?;
    buffering_batching_example().await?;
    chaining_example().await?;

    println!("All examples completed successfully!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_pipeline() {
        basic_pipeline_example().await.unwrap();
    }

    #[tokio::test]
    async fn test_processing_pipeline() {
        processing_pipeline_example().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_collection() {
        concurrent_collection_example().await.unwrap();
    }
}

/// Entry point for running examples
#[tokio::main]
async fn main() -> Result<()> {
    run_all_examples().await
}
