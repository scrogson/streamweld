//! Usage examples for the GenStage-inspired producer/consumer system.

use std::time::Duration;
use streamweld::prelude::*;
use streamweld::util::{consumer_from_fn, from_fn, processor_from_fn};
use streamweld::Result;
use tokio::time::sleep;

/// Example 1: Basic pipeline with range producer and print consumer
pub async fn basic_pipeline_example() -> Result<()> {
    println!("=== Basic Pipeline Example ===");

    let producer = RangeProducer::new(1..11);
    let consumer = PrintConsumer::<i64>::with_prefix("Item".to_string());

    let pipeline = Pipeline::new(producer, NoOpProcessor::<i64>::new()).buffer_size(5);

    pipeline.sink(consumer).await?;

    println!("Basic pipeline completed!\n");
    Ok(())
}

/// Example 2: Pipeline with processing
pub async fn processing_pipeline_example() -> Result<()> {
    println!("=== Processing Pipeline Example ===");

    let producer = RangeProducer::new(1..11);
    let processor = MapProcessor::new(|x| x * 2);
    let consumer = PrintConsumer::<i64>::with_prefix("Doubled".to_string());

    let pipeline = Pipeline::new(producer, processor).buffer_size(5);

    pipeline.sink(consumer).await?;

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
impl streamweld::traits::Processor for MultiStageProcessor {
    type Input = i64;
    type Output = Vec<i64>;

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

/// Debug consumer for Vec<i64>
struct DebugPrintConsumer;

#[async_trait::async_trait]
impl streamweld::traits::Consumer for DebugPrintConsumer {
    type Item = Vec<i64>;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
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

    let producer = RangeProducer::new(1..21);
    let processor = MultiStageProcessor::new(3);
    let consumer = DebugPrintConsumer;

    let pipeline = Pipeline::new(producer, processor).buffer_size(10);

    pipeline.sink(consumer).await?;

    println!("Complex pipeline completed!\n");
    Ok(())
}

/// Example 4: Fibonacci generator with rate limiting
pub async fn fibonacci_rate_limited_example() -> Result<()> {
    println!("=== Fibonacci Rate Limited Example ===");

    let producer = FibonacciProducer::with_limit(10);
    let rate_limiter = RateLimitProcessor::new(2); // 2 per second
    let consumer = PrintConsumer::<u64>::with_prefix("Fib".to_string());

    let pipeline = Pipeline::new(producer, rate_limiter).operation_timeout(Duration::from_secs(1));

    let start = std::time::Instant::now();
    pipeline.sink(consumer).await?;
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

    let producer = RangeProducer::new(1..101);
    let collector = CollectConsumer::new();
    let collector_ref = collector.clone();

    // Use builder pattern for config
    let pipeline = ConcurrentPipeline::new(producer, collector)
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

    // Create a producer that sometimes fails
    let producer = from_fn(|| async {
        static mut COUNTER: i32 = 0;
        unsafe {
            COUNTER += 1;
            if COUNTER % 3 == 0 {
                // Simulate occasional errors
                Err(Error::custom("Simulated error"))
            } else if COUNTER > 10 {
                Ok(None) // End production
            } else {
                Ok(Some(COUNTER))
            }
        }
    });

    // Wrap in error handling
    let error_handler =
        ErrorHandlingProcessor::new(MapProcessor::new(|x: i32| format!("Value: {}", x)));

    let consumer = consumer_from_fn(|result: Result<String>| async move {
        match result {
            Ok(value) => println!("✓ {}", value),
            Err(e) => println!("✗ Error: {}", e),
        }
        Ok(())
    });

    let pipeline = Pipeline::new(producer, error_handler).fail_fast(false); // Continue on errors

    pipeline.sink(consumer).await?;

    println!("Error handling example completed!\n");
    Ok(())
}

/// Example 7: Fan-out pattern with multiple consumers
pub async fn fan_out_example() -> Result<()> {
    println!("=== Fan-out Example ===");

    let producer = RangeProducer::new(1..21);

    // Create multiple consumers
    let consumer1 = PrintConsumer::<i64>::with_prefix("Consumer-1".to_string());
    let consumer2 = PrintConsumer::<i64>::with_prefix("Consumer-2".to_string());
    let consumer3 = CountConsumer::new();
    let counter_ref = consumer3.clone();

    // Simple fan-out using multiple pipelines (for demonstration)
    let producer1 = RangeProducer::new(1..6);
    let producer2 = RangeProducer::new(6..11);
    let producer3 = RangeProducer::new(11..16);

    let pipeline1 = Pipeline::new(producer1, NoOpProcessor::<i64>::new());
    let pipeline2 = Pipeline::new(producer2, NoOpProcessor::<i64>::new());
    let pipeline3 = Pipeline::new(producer3, NoOpProcessor::<i64>::new());

    // Run pipelines concurrently
    let results = tokio::try_join!(
        pipeline1.sink(PrintConsumer::<i64>::with_prefix("Pipeline-1".to_string())),
        pipeline2.sink(PrintConsumer::<i64>::with_prefix("Pipeline-2".to_string())),
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

    let producer = RangeProducer::new(1..11);
    let consumer = PrintConsumer::<String>::with_prefix("Async".to_string());

    let pipeline =
        Pipeline::new(producer, async_processor).operation_timeout(Duration::from_secs(5));

    let start = std::time::Instant::now();
    pipeline.sink(consumer).await?;
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

    let producer = RangeProducer::new(1..1001);
    let consumer = ThroughputConsumer::new(
        CountConsumer::new(),
        Duration::from_millis(500), // Report every 500ms
    );

    let pipeline = Pipeline::new(producer, NoOpProcessor::<i64>::new()).buffer_size(100);

    pipeline.sink(consumer).await?;

    println!("Throughput measurement completed!\n");
    Ok(())
}

/// Example 10: Buffering and batching
pub async fn buffering_batching_example() -> Result<()> {
    println!("=== Buffering and Batching Example ===");

    // Create a producer with irregular timing
    let producer = IntervalProducer::new(RangeProducer::new(1..21), Duration::from_millis(50));

    // Buffer items for up to 200ms or 5 items
    let buffer_processor = BufferProcessor::new(5, Duration::from_millis(200));

    // Print the batches
    let consumer = DebugPrintConsumer;

    let pipeline = Pipeline::new(producer, buffer_processor).buffer_size(10);

    let start = std::time::Instant::now();
    pipeline.sink(consumer).await?;
    let elapsed = start.elapsed();

    println!("Buffering completed in {:.2}s\n", elapsed.as_secs_f64());
    Ok(())
}

/// Example 11: Chaining producers
pub async fn chaining_example() -> Result<()> {
    println!("=== Chaining Producers ===");

    let producer1 = RangeProducer::new(1..4);
    let producer2 = RangeProducer::new(10..13);
    let producer3 = RangeProducer::new(20..23);

    let chained = producer1.chain(producer2).chain(producer3);

    Pipeline::new(chained, NoOpProcessor::<i64>::new())
        .sink(PrintConsumer::<i64>::with_prefix("Chained".to_string()))
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
